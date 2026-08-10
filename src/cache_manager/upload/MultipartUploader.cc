#include "cache_manager/upload/MultipartUploader.h"

#include <algorithm>
#include <fmt/format.h>
#include <folly/hash/Checksum.h>
#include <limits>
#include <sys/stat.h>
#include <thread>

#include "client/meta/MetaClient.h"
#include "client/mgmtd/ICommonMgmtdClient.h"
#include "client/storage/StorageClient.h"

namespace hf3fs::cache_manager {
namespace {

bool retryable(const Status &status) {
  return status.code() == CacheCode::kThrottled || status.code() == CacheCode::kUnavailable ||
         status.code() == CacheCode::kTimeout || status.code() == RPCCode::kTimeout;
}

uint64_t partCount(uint64_t length, uint64_t partSize) {
  if (length == 0) return 1;
  return 1 + (length - 1) / partSize;
}

uint64_t expectedPartSize(uint64_t length, uint64_t partSize, uint32_t partNumber) {
  auto offset = uint64_t{partNumber - 1} * partSize;
  return offset < length ? std::min(partSize, length - offset) : 0;
}

}  // namespace

Result<Void> MultipartUploaderConfig::valid() const {
  if (partSize == 0 || maxRetries > 100 || initialBackoff.count() <= 0 || maxBackoff < initialBackoff) {
    return makeError(StatusCode::kInvalidConfig, "invalid multipart uploader retry or part configuration");
  }
  return Void{};
}

MultipartUploader::MultipartUploader(std::shared_ptr<MultipartUploaderBackend> backend, MultipartUploaderConfig config)
    : backend_(std::move(backend)),
      config_(config) {}

Result<Void> MultipartUploader::checkCancelled() const {
  if (!backend_) return makeError(StatusCode::kInvalidConfig, "multipart uploader backend is missing");
  if (backend_->cancelled()) return makeError(MetaCode::kRequestCanceled, "multipart upload cancelled");
  return Void{};
}

Result<Void> MultipartUploader::validateProgress(const cache::UploadJobRecord &job, uint64_t partSize) {
  RETURN_ON_ERROR(job.valid());
  if (partSize == 0) return makeError(StatusCode::kInvalidConfig, "multipart part size is zero");
  if (job.state != cache::UploadJobState::SEALED && job.state != cache::UploadJobState::UPLOADING) {
    return makeError(CacheCode::kStateConflict, "upload job is not sealed or uploading");
  }
  auto count = partCount(job.stagingLength, partSize);
  if (count > cache::kMaxUploadParts) {
    return makeError(CacheCode::kRequestTooLarge, "staged upload exceeds multipart part count");
  }
  if (job.parts.size() > count) {
    return makeError(CacheCode::kStateConflict, "upload checkpoint has too many parts");
  }
  for (const auto &part : job.parts) {
    if (part.size != expectedPartSize(job.stagingLength, partSize, part.partNumber) || part.checksum.empty()) {
      return makeError(CacheCode::kStateConflict, "upload checkpoint part size or checksum changed");
    }
  }
  if (job.state == cache::UploadJobState::SEALED && (!job.multipartId.empty() || !job.parts.empty())) {
    return makeError(CacheCode::kStateConflict, "sealed upload already has multipart progress");
  }
  return Void{};
}

std::string MultipartUploader::checksum(std::span<const uint8_t> data) {
  return fmt::format("crc32c:{:08x}", folly::crc32c(data.data(), data.size()));
}

CoTryTask<Void> MultipartUploader::waitBeforeRetry(uint32_t retry) {
  CO_RETURN_ON_ERROR(checkCancelled());
  auto delay = config_.initialBackoff;
  for (uint32_t i = 0; i < retry && delay < config_.maxBackoff; ++i) {
    delay = delay > config_.maxBackoff / 2 ? config_.maxBackoff : delay * 2;
  }
  CO_RETURN_ON_ERROR(co_await backend_->backoff(delay));
  CO_RETURN_ON_ERROR(checkCancelled());
  co_return Void{};
}

CoTryTask<cache::origin::UploadPartResult> MultipartUploader::uploadPartWithRetry(
    cache::origin::UploadPartRequest request) {
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto result = co_await backend_->uploadPart(request);
    if (result.hasValue()) co_return std::move(*result);
    if (!retryable(result.error()) || attempt >= config_.maxRetries) co_return makeError(result.error());
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
}

CoTryTask<cache::UploadJobRecord> MultipartUploader::beginWithRetry(const cache::UploadJobRecord &job,
                                                                    std::string multipartId) {
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto result = co_await backend_->beginMultipartUpload(job.jobId, job.stateVersion, multipartId);
    if (result.hasValue()) co_return std::move(*result);
    if (!retryable(result.error()) || attempt >= config_.maxRetries) co_return makeError(result.error());
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
}

CoTryTask<cache::UploadJobRecord> MultipartUploader::checkpointWithRetry(const cache::UploadJobRecord &job,
                                                                         cache::CompletedUploadPart part) {
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto result = co_await backend_->checkpointUploadPart(job.jobId, job.stateVersion, job.multipartId, part);
    if (result.hasValue()) co_return std::move(*result);
    if (!retryable(result.error()) || attempt >= config_.maxRetries) co_return makeError(result.error());
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
}

CoTryTask<cache::UploadJobRecord> MultipartUploader::upload(cache::UploadJobRecord job) {
  CO_RETURN_ON_ERROR(config_.valid());
  CO_RETURN_ON_ERROR(checkCancelled());
  CO_RETURN_ON_ERROR(validateProgress(job, config_.partSize));
  if (job.state == cache::UploadJobState::SEALED) {
    auto created = co_await backend_->createMultipartUpload(job.destination);
    CO_RETURN_ON_ERROR(created);
    if (created->valid().hasError() || created->destination != job.destination) {
      co_return makeError(CacheCode::kInvalidResponse, "multipart create changed upload destination");
    }
    auto begun = co_await beginWithRetry(job, created->uploadId);
    if (begun.hasError()) {
      // A timeout may hide a committed begin. Only abort after Metadata proves
      // this upload id was not durably selected for the job.
      auto durable = co_await backend_->getUploadJob(job.jobId);
      if (durable.hasValue() && durable->state == cache::UploadJobState::UPLOADING &&
          durable->multipartId == created->uploadId) {
        job = std::move(*durable);
      } else {
        if (durable.hasValue()) {
          auto aborted = co_await backend_->abortMultipartUpload({{created->destination, created->uploadId}});
          if (aborted.hasError() && aborted.error().code() != CacheCode::kNotFound) {
            co_return makeError(aborted.error());
          }
        }
        co_return makeError(begun.error());
      }
    } else {
      job = std::move(*begun);
    }
    CO_RETURN_ON_ERROR(validateProgress(job, config_.partSize));
  }

  const auto totalParts = partCount(job.stagingLength, config_.partSize);
  while (job.parts.size() < totalParts) {
    CO_RETURN_ON_ERROR(checkCancelled());
    const auto partNumber = job.nextPartNumber;
    const auto offset = uint64_t{partNumber - 1} * config_.partSize;
    const auto length = expectedPartSize(job.stagingLength, config_.partSize, partNumber);
    auto body = co_await backend_->readStaging(job.stagingInode, {offset, length});
    CO_RETURN_ON_ERROR(body);
    if (body->size() != length) {
      co_return makeError(CacheCode::kInvalidResponse, "staging read returned the wrong part length");
    }
    auto localChecksum = checksum(*body);
    cache::origin::UploadPartRequest request{{job.destination, job.multipartId},
                                             partNumber,
                                             std::move(*body),
                                             localChecksum};
    auto uploaded = co_await uploadPartWithRetry(std::move(request));
    CO_RETURN_ON_ERROR(uploaded);
    if (uploaded->valid().hasError() || uploaded->part.partNumber != partNumber || uploaded->part.size != length ||
        uploaded->part.checksum != localChecksum) {
      co_return makeError(CacheCode::kInvalidResponse, "multipart upload returned a mismatched part checkpoint");
    }
    auto checkpointed = co_await checkpointWithRetry(job, uploaded->part);
    CO_RETURN_ON_ERROR(checkpointed);
    job = std::move(*checkpointed);
    CO_RETURN_ON_ERROR(validateProgress(job, config_.partSize));
  }
  co_return job;
}

RealMultipartUploaderBackend::RealMultipartUploaderBackend(
    std::shared_ptr<meta::client::MetaClient> metaClient,
    std::shared_ptr<storage::client::StorageClient> storageClient,
    std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
    std::shared_ptr<cache::origin::ObjectStore> objectStore,
    std::string serviceName,
    std::string serviceToken,
    flat::UserInfo user)
    : metaClient_(std::move(metaClient)),
      storageClient_(std::move(storageClient)),
      mgmtdClient_(std::move(mgmtdClient)),
      objectStore_(std::move(objectStore)),
      serviceName_(std::move(serviceName)),
      serviceToken_(std::move(serviceToken)),
      user_(std::move(user)) {}

CoTryTask<std::vector<uint8_t>> RealMultipartUploaderBackend::readStaging(uint64_t inodeId, cache::ByteRange range) {
  if (cancelled()) co_return makeError(MetaCode::kRequestCanceled, "multipart upload cancelled");
  auto inode = co_await metaClient_->stat(user_, meta::InodeId{inodeId}, std::nullopt, false);
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isFile() || inode->id.u64() != inodeId || !(inode->acl.iflags & FS_IMMUTABLE_FL) ||
      range.offset > inode->asFile().length || range.length > inode->asFile().length - range.offset) {
    co_return makeError(CacheCode::kStateConflict, "staging inode is not the frozen upload snapshot");
  }
  if (range.length > std::numeric_limits<size_t>::max()) {
    co_return makeError(CacheCode::kRequestTooLarge, "staging read exceeds local address space");
  }
  std::vector<uint8_t> data(range.length);
  if (range.length == 0) co_return data;
  auto routing = mgmtdClient_->getRoutingInfo();
  if (!routing || !routing->raw()) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  auto buffer = storageClient_->registerIOBuffer(data.data(), data.size());
  CO_RETURN_ON_ERROR(buffer);
  const auto &file = inode->asFile();
  const auto chunkSize = file.layout.chunkSize.u64();
  if (chunkSize == 0) co_return makeError(CacheCode::kInvalidResponse, "staging inode has an empty chunk size");
  std::vector<storage::client::ReadIO> reads;
  uint64_t consumed = 0;
  while (consumed < range.length) {
    auto fileOffset = range.offset + consumed;
    auto chunkOffset = fileOffset % chunkSize;
    auto length = std::min<uint64_t>(range.length - consumed, chunkSize - chunkOffset);
    auto chainId = file.getChainId(*inode, fileOffset, *routing->raw());
    CO_RETURN_ON_ERROR(chainId);
    auto chunkId = file.getChunkId(inode->id, fileOffset);
    CO_RETURN_ON_ERROR(chunkId);
    reads.push_back(storageClient_->createReadIO(*chainId,
                                                 storage::ChunkId(*chunkId),
                                                 static_cast<uint32_t>(chunkOffset),
                                                 static_cast<uint32_t>(length),
                                                 data.data() + consumed,
                                                 &*buffer));
    consumed += length;
  }
  CO_RETURN_ON_ERROR(co_await storageClient_->batchRead(reads, user_));
  for (const auto &read : reads) {
    if (!read.status().isOK()) co_return makeError(read.status());
    if (read.resultLen() != read.dataLen()) {
      co_return makeError(CacheCode::kInvalidResponse, "staging read was shorter than the frozen snapshot");
    }
  }
  co_return data;
}

CoTryTask<cache::origin::MultipartUpload> RealMultipartUploaderBackend::createMultipartUpload(
    const cache::ObjectRef &destination) {
  if (!objectStore_) co_return makeError(StatusCode::kInvalidConfig, "multipart object store is missing");
  co_return co_await objectStore_->createMultipartUpload({destination});
}

CoTryTask<Void> RealMultipartUploaderBackend::abortMultipartUpload(cache::origin::AbortMultipartUploadRequest request) {
  if (!objectStore_) co_return makeError(StatusCode::kInvalidConfig, "multipart object store is missing");
  co_return co_await objectStore_->abortMultipartUpload(request);
}

CoTryTask<cache::UploadJobRecord> RealMultipartUploaderBackend::getUploadJob(cache::UploadJobId jobId) {
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "multipart Metadata client is missing");
  meta::GetUploadJobReq request;
  request.user = user_;
  request.jobId = jobId;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->getUploadJob(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->job);
}

CoTryTask<cache::origin::UploadPartResult> RealMultipartUploaderBackend::uploadPart(
    cache::origin::UploadPartRequest request) {
  if (!objectStore_) co_return makeError(StatusCode::kInvalidConfig, "multipart object store is missing");
  co_return co_await objectStore_->uploadPart(std::move(request));
}

CoTryTask<cache::UploadJobRecord> RealMultipartUploaderBackend::beginMultipartUpload(cache::UploadJobId jobId,
                                                                                     uint64_t expectedStateVersion,
                                                                                     std::string multipartId) {
  meta::BeginMultipartUploadReq request;
  request.service = {serviceName_, serviceToken_};
  request.jobId = jobId;
  request.expectedStateVersion = expectedStateVersion;
  request.multipartId = std::move(multipartId);
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto result = co_await metaClient_->beginMultipartUpload(std::move(request));
  CO_RETURN_ON_ERROR(result);
  co_return std::move(result->job);
}

CoTryTask<cache::UploadJobRecord> RealMultipartUploaderBackend::checkpointUploadPart(cache::UploadJobId jobId,
                                                                                     uint64_t expectedStateVersion,
                                                                                     std::string multipartId,
                                                                                     cache::CompletedUploadPart part) {
  meta::CheckpointUploadPartReq request;
  request.service = {serviceName_, serviceToken_};
  request.jobId = jobId;
  request.expectedStateVersion = expectedStateVersion;
  request.multipartId = std::move(multipartId);
  request.part = std::move(part);
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto result = co_await metaClient_->checkpointUploadPart(std::move(request));
  CO_RETURN_ON_ERROR(result);
  co_return std::move(result->job);
}

CoTryTask<Void> RealMultipartUploaderBackend::backoff(std::chrono::milliseconds delay) {
  constexpr auto poll = std::chrono::milliseconds{10};
  while (delay.count() > 0) {
    if (cancelled()) co_return makeError(MetaCode::kRequestCanceled, "multipart upload cancelled");
    auto slice = std::min(delay, poll);
    std::this_thread::sleep_for(slice);
    delay -= slice;
  }
  co_return Void{};
}

bool RealMultipartUploaderBackend::cancelled() const { return cancelled_.load(std::memory_order_relaxed); }

void RealMultipartUploaderBackend::cancel() { cancelled_.store(true, std::memory_order_relaxed); }

}  // namespace hf3fs::cache_manager
