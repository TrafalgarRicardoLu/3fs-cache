#include "cache_manager/upload/MultipartUploadFinalizer.h"

#include <algorithm>
#include <thread>

#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {
namespace {

bool retryable(const Status &status) {
  return status.code() == CacheCode::kThrottled || status.code() == CacheCode::kUnavailable ||
         status.code() == CacheCode::kTimeout || status.code() == RPCCode::kTimeout;
}

bool ambiguousComplete(const Status &status) {
  return retryable(status) || status.code() == CacheCode::kNotFound || status.code() == CacheCode::kInvalidResponse;
}

uint64_t uploadedBytes(const cache::UploadJobRecord &job) {
  uint64_t result = 0;
  for (const auto &part : job.parts) result += part.size;
  return result;
}

std::string boundedError(std::string error) {
  if (error.empty()) error = "multipart upload failed";
  if (error.size() > cache::kMaxUploadErrorBytes) error.resize(cache::kMaxUploadErrorBytes);
  return error;
}

}  // namespace

Result<Void> MultipartUploadFinalizerConfig::valid() const {
  if (maxRetries > 100 || initialBackoff.count() <= 0 || maxBackoff < initialBackoff) {
    return makeError(StatusCode::kInvalidConfig, "invalid multipart finalizer retry configuration");
  }
  return Void{};
}

MultipartUploadFinalizer::MultipartUploadFinalizer(std::shared_ptr<MultipartUploadFinalizerBackend> backend,
                                                   MultipartUploadFinalizerConfig config)
    : backend_(std::move(backend)),
      config_(config) {}

Result<Void> MultipartUploadFinalizer::checkCancelled() const {
  if (!backend_) return makeError(StatusCode::kInvalidConfig, "multipart finalizer backend is missing");
  if (backend_->cancelled()) return makeError(MetaCode::kRequestCanceled, "multipart finalization cancelled");
  return Void{};
}

CoTryTask<Void> MultipartUploadFinalizer::waitBeforeRetry(uint32_t retry) {
  CO_RETURN_ON_ERROR(checkCancelled());
  auto delay = config_.initialBackoff;
  for (uint32_t i = 0; i < retry && delay < config_.maxBackoff; ++i) {
    delay = delay > config_.maxBackoff / 2 ? config_.maxBackoff : delay * 2;
  }
  CO_RETURN_ON_ERROR(co_await backend_->backoff(delay));
  CO_RETURN_ON_ERROR(checkCancelled());
  co_return Void{};
}

CoTryTask<cache::UploadJobRecord> MultipartUploadFinalizer::mutateWithRetry(
    cache::UploadJobRecord job,
    meta::MultipartUploadMutation mutation,
    std::optional<cache::ImmutableObjectIdentity> completed,
    std::string error) {
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto result = co_await backend_->mutate(job, mutation, completed, error);
    if (result.hasValue()) co_return std::move(*result);
    if (!retryable(result.error()) || attempt >= config_.maxRetries) co_return makeError(result.error());
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
}

CoTryTask<cache::origin::ObjectMetadata> MultipartUploadFinalizer::recoverCompleted(const cache::UploadJobRecord &job) {
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto result = co_await backend_->headCompleted({job.destination, job.stagingLength});
    if (result.hasValue()) co_return std::move(*result);
    if (!retryable(result.error()) || attempt >= config_.maxRetries) co_return makeError(result.error());
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
}

CoTryTask<Void> MultipartUploadFinalizer::markTerminalFailure(cache::UploadJobRecord job, const Status &failure) {
  auto error = std::string{"multipart terminal failure: "} + std::string{StatusCode::toString(failure.code())};
  auto aborted = co_await mutateWithRetry(std::move(job),
                                          meta::MultipartUploadMutation::BEGIN_ABORT,
                                          std::nullopt,
                                          boundedError(std::move(error)));
  CO_RETURN_ON_ERROR(aborted);
  co_return Void{};
}

CoTryTask<cache::UploadJobRecord> MultipartUploadFinalizer::complete(cache::UploadJobRecord job) {
  CO_RETURN_ON_ERROR(config_.valid());
  CO_RETURN_ON_ERROR(job.valid());
  CO_RETURN_ON_ERROR(checkCancelled());
  if (job.state == cache::UploadJobState::UPLOADING) {
    if (uploadedBytes(job) != job.stagingLength) {
      co_return makeError(CacheCode::kStateConflict, "multipart upload is not fully checkpointed");
    }
    auto prepared = co_await mutateWithRetry(job, meta::MultipartUploadMutation::PREPARE_COMPLETE);
    CO_RETURN_ON_ERROR(prepared);
    job = std::move(*prepared);
  }
  if (job.state != cache::UploadJobState::COMPLETING) {
    co_return makeError(CacheCode::kStateConflict, "multipart upload is not completing");
  }

  cache::origin::ObjectMetadata object;
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto completed = co_await backend_->complete({{job.destination, job.multipartId}, job.parts, job.stagingLength});
    if (completed.hasValue()) {
      object = std::move(*completed);
      break;
    }
    auto failure = completed.error();
    if (!ambiguousComplete(failure)) co_return makeError(failure);
    auto recovered = co_await recoverCompleted(job);
    if (recovered.hasValue()) {
      object = std::move(*recovered);
      break;
    }
    if (recovered.error().code() != CacheCode::kNotFound || !retryable(failure) || attempt >= config_.maxRetries) {
      if (failure.code() == CacheCode::kInvalidResponse || recovered.error().code() == CacheCode::kVersionMismatch) {
        CO_RETURN_ON_ERROR(co_await markTerminalFailure(job, failure));
      }
      co_return makeError(failure);
    }
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
  if (object.size != job.stagingLength || object.identity.originId != job.destination.originId ||
      object.identity.bucket != job.destination.bucket || object.identity.key != job.destination.key ||
      object.identity.valid().hasError()) {
    auto failure = Status(CacheCode::kInvalidResponse, "completed multipart object identity changed");
    CO_RETURN_ON_ERROR(co_await markTerminalFailure(job, failure));
    co_return makeError(failure);
  }
  co_return co_await mutateWithRetry(job, meta::MultipartUploadMutation::SAVE_COMPLETED, object.identity);
}

CoTryTask<cache::UploadJobRecord> MultipartUploadFinalizer::cancel(cache::UploadJobRecord job, std::string reason) {
  CO_RETURN_ON_ERROR(config_.valid());
  CO_RETURN_ON_ERROR(job.valid());
  CO_RETURN_ON_ERROR(checkCancelled());
  if (job.state != cache::UploadJobState::ABORTING) {
    auto aborting = co_await mutateWithRetry(job,
                                             meta::MultipartUploadMutation::BEGIN_ABORT,
                                             std::nullopt,
                                             boundedError(std::move(reason)));
    CO_RETURN_ON_ERROR(aborting);
    job = std::move(*aborting);
  }
  for (uint32_t attempt = 0;; ++attempt) {
    CO_RETURN_ON_ERROR(checkCancelled());
    auto result = co_await backend_->abort({{job.destination, job.multipartId}});
    if (result.hasValue() || result.error().code() == CacheCode::kNotFound) break;
    if (!retryable(result.error()) || attempt >= config_.maxRetries) co_return makeError(result.error());
    CO_RETURN_ON_ERROR(co_await waitBeforeRetry(attempt));
  }
  co_return co_await mutateWithRetry(job, meta::MultipartUploadMutation::FINISH_ABORT);
}

RealMultipartUploadFinalizerBackend::RealMultipartUploadFinalizerBackend(
    std::shared_ptr<meta::client::MetaClient> metaClient,
    std::shared_ptr<cache::origin::ObjectStore> objectStore,
    std::string serviceName,
    std::string serviceToken)
    : metaClient_(std::move(metaClient)),
      objectStore_(std::move(objectStore)),
      serviceName_(std::move(serviceName)),
      serviceToken_(std::move(serviceToken)) {}

CoTryTask<cache::origin::ObjectMetadata> RealMultipartUploadFinalizerBackend::complete(
    cache::origin::CompleteMultipartUploadRequest request) {
  if (!objectStore_) co_return makeError(StatusCode::kInvalidConfig, "multipart object store is missing");
  co_return co_await objectStore_->completeMultipartUpload(std::move(request));
}

CoTryTask<cache::origin::ObjectMetadata> RealMultipartUploadFinalizerBackend::headCompleted(
    cache::origin::HeadCompletedUploadRequest request) {
  if (!objectStore_) co_return makeError(StatusCode::kInvalidConfig, "multipart object store is missing");
  co_return co_await objectStore_->headCompletedUpload(request);
}

CoTryTask<Void> RealMultipartUploadFinalizerBackend::abort(cache::origin::AbortMultipartUploadRequest request) {
  if (!objectStore_) co_return makeError(StatusCode::kInvalidConfig, "multipart object store is missing");
  co_return co_await objectStore_->abortMultipartUpload(request);
}

CoTryTask<cache::UploadJobRecord> RealMultipartUploadFinalizerBackend::mutate(
    cache::UploadJobRecord job,
    meta::MultipartUploadMutation mutation,
    std::optional<cache::ImmutableObjectIdentity> completed,
    std::string error) {
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "multipart metadata client is missing");
  meta::MutateMultipartUploadReq request;
  request.service = {serviceName_, serviceToken_};
  request.jobId = job.jobId;
  request.expectedStateVersion = job.stateVersion;
  request.multipartId = job.multipartId;
  request.mutation = mutation;
  request.completedObject = std::move(completed);
  request.error = std::move(error);
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto result = co_await metaClient_->mutateMultipartUpload(std::move(request));
  CO_RETURN_ON_ERROR(result);
  co_return std::move(result->job);
}

CoTryTask<Void> RealMultipartUploadFinalizerBackend::backoff(std::chrono::milliseconds delay) {
  constexpr auto poll = std::chrono::milliseconds{10};
  while (delay.count() > 0) {
    if (cancelled()) co_return makeError(MetaCode::kRequestCanceled, "multipart finalization cancelled");
    auto slice = std::min(delay, poll);
    std::this_thread::sleep_for(slice);
    delay -= slice;
  }
  co_return Void{};
}

bool RealMultipartUploadFinalizerBackend::cancelled() const { return cancelled_.load(std::memory_order_relaxed); }

void RealMultipartUploadFinalizerBackend::cancel() { cancelled_.store(true, std::memory_order_relaxed); }

}  // namespace hf3fs::cache_manager
