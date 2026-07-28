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

CoTryTask<ObjectMetadata> S3ObjectStore::head(const ObjectRef &object) {
  auto task = folly::coro::co_invoke([this, object]() -> CoTryTask<ObjectMetadata> { co_return headSync(object); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

CoTryTask<std::vector<uint8_t>> S3ObjectStore::getRange(const ImmutableObjectIdentity &object, ByteRange range) {
  auto task = folly::coro::co_invoke(
      [this, object, range]() -> CoTryTask<std::vector<uint8_t>> { co_return getRangeSync(object, range); });
  co_return co_await std::move(task).scheduleOn(&ioExecutor_);
}

}  // namespace hf3fs::cache::origin::s3
