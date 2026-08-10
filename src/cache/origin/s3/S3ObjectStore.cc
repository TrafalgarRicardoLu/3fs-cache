#include "cache/origin/s3/S3ObjectStore.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <fmt/format.h>
#include <folly/experimental/coro/Invoke.h>
#include <limits>
#include <thread>
#include <utility>

namespace hf3fs::cache::origin::s3 {
namespace {

std::string normalizeEtag(std::string etag) {
  if (etag.size() >= 2 && etag.front() == '"' && etag.back() == '"') {
    etag = etag.substr(1, etag.size() - 2);
  }
  return etag;
}

bool retryable(const S3Failure &failure) {
  return failure.kind == S3FailureKind::TIMEOUT || failure.kind == S3FailureKind::THROTTLED ||
         failure.kind == S3FailureKind::SERVER;
}

folly::Unexpected<Status> mapFailure(const S3Failure &failure) {
  switch (failure.kind) {
    case S3FailureKind::TIMEOUT:
      return makeError(CacheCode::kTimeout, failure.message);
    case S3FailureKind::THROTTLED:
      return makeError(CacheCode::kThrottled, failure.message);
    case S3FailureKind::SERVER:
      return makeError(CacheCode::kUnavailable, failure.message);
    case S3FailureKind::NOT_FOUND:
    case S3FailureKind::NO_SUCH_UPLOAD:
      return makeError(CacheCode::kNotFound, failure.message);
    case S3FailureKind::VERSION_MISMATCH:
      return makeError(CacheCode::kVersionMismatch, failure.message);
    case S3FailureKind::AUTHENTICATION:
      return makeError(CacheCode::kAccessDenied, failure.message);
    case S3FailureKind::INVALID_RESPONSE:
      return makeError(CacheCode::kInvalidResponse, failure.message);
    default:
      return makeError(CacheCode::kUnavailable, failure.message);
  }
}

Result<VersionSelector> selectVersion(const HeadResponse &response) {
  if (response.versionId.has_value() && !response.versionId->empty()) {
    return VersionSelector{VersionSelectorType::VERSION_ID, *response.versionId};
  }
  if (response.etag.has_value()) {
    auto etag = normalizeEtag(*response.etag);
    if (!etag.empty() && !etag.starts_with("W/")) {
      return VersionSelector{VersionSelectorType::STRONG_ETAG, std::move(etag)};
    }
  }
  return makeError(CacheCode::kVersionMismatch, "S3 object has neither VersionId nor a strong ETag");
}

bool responseMatches(const ImmutableObjectIdentity &object, const GetRangeResponse &response) {
  if (object.version.type == VersionSelectorType::VERSION_ID) {
    return response.versionId.has_value() && *response.versionId == object.version.value;
  }
  return response.etag.has_value() && normalizeEtag(*response.etag) == object.version.value;
}

std::string expectedContentRange(ByteRange range) {
  return fmt::format("bytes {}-{}", range.offset, range.offset + range.length - 1);
}

bool validContentRange(ByteRange range, std::string_view contentRange) {
  auto prefix = expectedContentRange(range) + "/";
  if (!contentRange.starts_with(prefix)) return false;
  auto totalText = contentRange.substr(prefix.size());
  uint64_t total = 0;
  auto [end, error] = std::from_chars(totalText.begin(), totalText.end(), total);
  return error == std::errc{} && end == totalText.end() && total > range.offset + range.length - 1;
}

}  // namespace

class S3ObjectStore::Permit {
 public:
  Permit() = default;
  Permit(S3ObjectStore &store, uint64_t bytes)
      : store_(&store),
        bytes_(bytes) {}
  Permit(Permit &&other) noexcept
      : store_(std::exchange(other.store_, nullptr)),
        bytes_(other.bytes_) {}
  Permit &operator=(Permit &&) = delete;
  Permit(const Permit &) = delete;
  Permit &operator=(const Permit &) = delete;
  ~Permit() {
    if (store_ != nullptr) store_->release(bytes_);
  }

 private:
  S3ObjectStore *store_{nullptr};
  uint64_t bytes_{0};
};

S3ObjectStore::S3ObjectStore(S3ObjectStoreConfig config, std::unique_ptr<S3RequestExecutor> executor)
    : config_(std::move(config)),
      executor_(std::move(executor)),
      ioExecutor_(std::max(config_.ioThreads, uint32_t{1})) {}

Result<S3ObjectStore::Permit> S3ObjectStore::acquire(uint64_t bytes) {
  if (config_.maxConcurrentRequests == 0) {
    return makeError(StatusCode::kInvalidConfig, "S3 max concurrency is zero");
  }
  if (bytes > config_.maxInflightBytes)
    return makeError(CacheCode::kRequestTooLarge, "S3 range exceeds inflight limit");
  std::unique_lock lock(mutex_);
  auto available = condition_.wait_for(lock, config_.totalTimeout, [&] {
    return activeRequests_ < config_.maxConcurrentRequests && bytes <= config_.maxInflightBytes - inflightBytes_;
  });
  if (!available) return makeError(CacheCode::kTimeout, "timed out waiting for an S3 request permit");
  ++activeRequests_;
  inflightBytes_ += bytes;
  return Permit(*this, bytes);
}

void S3ObjectStore::release(uint64_t bytes) {
  {
    std::lock_guard lock(mutex_);
    --activeRequests_;
    inflightBytes_ -= bytes;
  }
  condition_.notify_all();
}

Result<ObjectMetadata> S3ObjectStore::headSync(const ObjectRef &object) {
  RETURN_ON_ERROR(object.valid());
  auto permit = acquire(0);
  RETURN_ON_ERROR(permit);
  auto start = std::chrono::steady_clock::now();
  for (uint32_t attempt = 0;; ++attempt) {
    auto outcome = executor_->head(HeadRequest{object});
    if (auto response = std::get_if<HeadResponse>(&outcome)) {
      auto version = selectVersion(*response);
      RETURN_ON_ERROR(version);
      return ObjectMetadata{ImmutableObjectIdentity{object.originId, object.bucket, object.key, std::move(*version)},
                            response->size};
    }
    auto &failure = std::get<S3Failure>(outcome);
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto retryDelay = config_.retryDelay * (attempt + 1);
    if (!retryable(failure) || attempt >= config_.maxRetries || elapsed + retryDelay >= config_.totalTimeout) {
      return mapFailure(failure);
    }
    std::this_thread::sleep_for(retryDelay);
  }
}

Result<std::vector<uint8_t>> S3ObjectStore::getRangeSync(const ImmutableObjectIdentity &object, ByteRange range) {
  RETURN_ON_ERROR(object.valid());
  auto end = range.end();
  RETURN_ON_ERROR(end);
  if (range.empty()) return std::vector<uint8_t>{};
  auto permit = acquire(range.length);
  RETURN_ON_ERROR(permit);
  auto start = std::chrono::steady_clock::now();
  for (uint32_t attempt = 0;; ++attempt) {
    auto outcome = executor_->getRange(GetRangeRequest{object, range});
    if (auto response = std::get_if<GetRangeResponse>(&outcome)) {
      if (response->httpStatus != 206 || !validContentRange(range, response->contentRange) ||
          response->body.size() != range.length) {
        return makeError(CacheCode::kInvalidResponse, "invalid S3 ranged GET response");
      }
      if (!responseMatches(object, *response)) {
        return makeError(CacheCode::kVersionMismatch, "S3 response object version changed");
      }
      return std::move(response->body);
    }
    auto &failure = std::get<S3Failure>(outcome);
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto retryDelay = config_.retryDelay * (attempt + 1);
    if (!retryable(failure) || attempt >= config_.maxRetries || elapsed + retryDelay >= config_.totalTimeout) {
      return mapFailure(failure);
    }
    std::this_thread::sleep_for(retryDelay);
  }
}

Result<ListObjectsPage> S3ObjectStore::listObjectsSync(const ListObjectsRequest &request) {
  RETURN_ON_ERROR(request.valid());
  auto permit = acquire(0);
  RETURN_ON_ERROR(permit);
  auto start = std::chrono::steady_clock::now();
  for (uint32_t attempt = 0;; ++attempt) {
    auto outcome = executor_->listObjects({request.bucket, request.prefix, request.continuation, request.maxKeys});
    if (auto response = std::get_if<ListResponse>(&outcome)) {
      if (response->objects.size() > request.maxKeys ||
          (response->truncated &&
           (response->nextContinuation.empty() || response->nextContinuation == request.continuation)) ||
          (!response->truncated && !response->nextContinuation.empty())) {
        return makeError(CacheCode::kInvalidResponse, "invalid S3 object list pagination");
      }
      ListObjectsPage page;
      page.done = !response->truncated;
      page.nextContinuation = std::move(response->nextContinuation);
      page.objects.reserve(response->objects.size());
      std::string previous;
      for (auto &listed : response->objects) {
        if (listed.key.empty() || !listed.key.starts_with(request.prefix) ||
            (!previous.empty() && listed.key <= previous)) {
          return makeError(CacheCode::kInvalidResponse, "S3 object list is not strictly sorted within the prefix");
        }
        auto version = selectVersion({listed.size, std::move(listed.versionId), std::move(listed.etag)});
        RETURN_ON_ERROR(version);
        previous = listed.key;
        page.objects.push_back(
            {{request.originId, request.bucket, std::move(listed.key), std::move(*version)}, listed.size});
      }
      return page;
    }
    auto &failure = std::get<S3Failure>(outcome);
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto retryDelay = config_.retryDelay * (attempt + 1);
    if (!retryable(failure) || attempt >= config_.maxRetries || elapsed + retryDelay >= config_.totalTimeout) {
      return mapFailure(failure);
    }
    std::this_thread::sleep_for(retryDelay);
  }
}

Result<MultipartUpload> S3ObjectStore::createMultipartUploadSync(const CreateMultipartUploadRequest &request) {
  RETURN_ON_ERROR(request.valid());
  auto permit = acquire(0);
  RETURN_ON_ERROR(permit);
  auto outcome = executor_->createMultipartUpload({request.destination.bucket, request.destination.key});
  if (auto response = std::get_if<S3CreateMultipartResponse>(&outcome)) {
    MultipartUpload upload{request.destination, std::move(response->uploadId)};
    auto valid = upload.valid();
    if (valid.hasError()) return makeError(CacheCode::kInvalidResponse, valid.error().message());
    return upload;
  }
  return mapFailure(std::get<S3Failure>(outcome));
}

Result<UploadPartResult> S3ObjectStore::uploadPartSync(UploadPartRequest request) {
  RETURN_ON_ERROR(request.valid());
  auto permit = acquire(request.body.size());
  RETURN_ON_ERROR(permit);
  auto partSize = request.body.size();
  auto outcome = executor_->uploadPart({request.upload.destination.bucket,
                                        request.upload.destination.key,
                                        request.upload.uploadId,
                                        request.partNumber,
                                        std::move(request.body),
                                        request.checksum});
  if (auto response = std::get_if<S3UploadPartResponse>(&outcome)) {
    auto etag = normalizeEtag(std::move(response->etag));
    UploadPartResult result{{request.partNumber, partSize, std::move(etag), std::move(request.checksum)}};
    auto valid = result.valid();
    if (valid.hasError()) return makeError(CacheCode::kInvalidResponse, valid.error().message());
    return result;
  }
  return mapFailure(std::get<S3Failure>(outcome));
}

Result<ObjectMetadata> S3ObjectStore::completeMultipartUploadSync(CompleteMultipartUploadRequest request) {
  RETURN_ON_ERROR(request.valid());
  auto permit = acquire(0);
  RETURN_ON_ERROR(permit);
  auto outcome = executor_->completeMultipartUpload(
      {request.upload.destination.bucket, request.upload.destination.key, request.upload.uploadId, request.parts});
  if (auto response = std::get_if<S3CompleteMultipartResponse>(&outcome)) {
    if (response->bucket != request.upload.destination.bucket || response->key != request.upload.destination.key) {
      return makeError(CacheCode::kInvalidResponse, "S3 multipart completion changed the destination");
    }
    auto version = selectVersion({request.expectedSize, std::move(response->versionId), std::move(response->etag)});
    if (version.hasError()) return makeError(CacheCode::kInvalidResponse, version.error().message());
    return ObjectMetadata{{request.upload.destination.originId,
                           request.upload.destination.bucket,
                           request.upload.destination.key,
                           std::move(*version)},
                          request.expectedSize};
  }
  return mapFailure(std::get<S3Failure>(outcome));
}

Result<Void> S3ObjectStore::abortMultipartUploadSync(const AbortMultipartUploadRequest &request) {
  RETURN_ON_ERROR(request.valid());
  auto permit = acquire(0);
  RETURN_ON_ERROR(permit);
  auto outcome = executor_->abortMultipartUpload(
      {request.upload.destination.bucket, request.upload.destination.key, request.upload.uploadId});
  if (std::holds_alternative<S3AbortMultipartResponse>(outcome)) return Void{};
  return mapFailure(std::get<S3Failure>(outcome));
}

Result<ObjectMetadata> S3ObjectStore::headCompletedUploadSync(const HeadCompletedUploadRequest &request) {
  RETURN_ON_ERROR(request.valid());
  auto result = headSync(request.destination);
  RETURN_ON_ERROR(result);
  if (result->size != request.expectedSize) {
    return makeError(CacheCode::kInvalidResponse, "completed S3 object size does not match the staged upload");
  }
  return result;
}

Result<Void> S3ObjectStore::deleteObjectSync(const DeleteObjectRequest &request) {
  RETURN_ON_ERROR(request.valid());
  if (request.object.version.type == VersionSelectorType::STRONG_ETAG) {
    auto current = headSync({request.object.originId, request.object.bucket, request.object.key});
    if (current.hasError() && current.error().code() == CacheCode::kNotFound) return Void{};
    RETURN_ON_ERROR(current);
    if (current->identity.version.type != VersionSelectorType::STRONG_ETAG ||
        current->identity.version.value != request.object.version.value) {
      return makeError(CacheCode::kVersionMismatch, "S3 orphan object identity changed before deletion");
    }
  }
  auto permit = acquire(0);
  RETURN_ON_ERROR(permit);
  auto start = std::chrono::steady_clock::now();
  for (uint32_t attempt = 0;; ++attempt) {
    std::optional<std::string> versionId;
    if (request.object.version.type == VersionSelectorType::VERSION_ID) versionId = request.object.version.value;
    auto outcome = executor_->deleteObject({request.object.bucket, request.object.key, std::move(versionId)});
    if (std::holds_alternative<S3DeleteObjectResponse>(outcome)) return Void{};
    auto &failure = std::get<S3Failure>(outcome);
    if (failure.kind == S3FailureKind::NOT_FOUND) return Void{};
    auto elapsed = std::chrono::steady_clock::now() - start;
    auto retryDelay = config_.retryDelay * (attempt + 1);
    if (!retryable(failure) || attempt >= config_.maxRetries || elapsed + retryDelay >= config_.totalTimeout) {
      return mapFailure(failure);
    }
    std::this_thread::sleep_for(retryDelay);
  }
}

CoTryTask<ObjectMetadata> S3ObjectStore::head(const ObjectRef &object) {
  auto task = folly::coro::co_invoke([this, object]() -> CoTryTask<ObjectMetadata> { co_return headSync(object); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<std::vector<uint8_t>> S3ObjectStore::getRange(const ImmutableObjectIdentity &object, ByteRange range) {
  auto task = folly::coro::co_invoke(
      [this, object, range]() -> CoTryTask<std::vector<uint8_t>> { co_return getRangeSync(object, range); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<ListObjectsPage> S3ObjectStore::listObjects(const ListObjectsRequest &request) {
  auto task =
      folly::coro::co_invoke([this, request]() -> CoTryTask<ListObjectsPage> { co_return listObjectsSync(request); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<MultipartUpload> S3ObjectStore::createMultipartUpload(const CreateMultipartUploadRequest &request) {
  auto task = folly::coro::co_invoke(
      [this, request]() -> CoTryTask<MultipartUpload> { co_return createMultipartUploadSync(request); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<UploadPartResult> S3ObjectStore::uploadPart(UploadPartRequest request) {
  auto task = folly::coro::co_invoke([this, request = std::move(request)]() mutable -> CoTryTask<UploadPartResult> {
    co_return uploadPartSync(std::move(request));
  });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<ObjectMetadata> S3ObjectStore::completeMultipartUpload(CompleteMultipartUploadRequest request) {
  auto task = folly::coro::co_invoke([this, request = std::move(request)]() mutable -> CoTryTask<ObjectMetadata> {
    co_return completeMultipartUploadSync(std::move(request));
  });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<Void> S3ObjectStore::abortMultipartUpload(const AbortMultipartUploadRequest &request) {
  auto task =
      folly::coro::co_invoke([this, request]() -> CoTryTask<Void> { co_return abortMultipartUploadSync(request); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<ObjectMetadata> S3ObjectStore::headCompletedUpload(const HeadCompletedUploadRequest &request) {
  auto task = folly::coro::co_invoke(
      [this, request]() -> CoTryTask<ObjectMetadata> { co_return headCompletedUploadSync(request); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<Void> S3ObjectStore::deleteObject(const DeleteObjectRequest &request) {
  auto task = folly::coro::co_invoke([this, request]() -> CoTryTask<Void> { co_return deleteObjectSync(request); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

}  // namespace hf3fs::cache::origin::s3
