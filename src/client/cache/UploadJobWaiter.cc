#include "client/cache/UploadJobWaiter.h"

#include <algorithm>
#include <folly/experimental/coro/Sleep.h>

#include "client/meta/MetaClient.h"

namespace hf3fs::client::cache {

Result<Void> UploadJobWaiterConfig::valid() const {
  if (pollInterval <= 0_ns) return makeError(StatusCode::kInvalidConfig, "upload job poll interval must be positive");
  return Void{};
}

CoTryTask<hf3fs::cache::UploadJobRecord> MetaUploadJobWaiterBackend::get(flat::UserInfo user,
                                                                         hf3fs::cache::UploadJobId jobId) {
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "upload metadata client is missing");
  meta::GetUploadJobReq request;
  request.user = std::move(user);
  request.jobId = jobId;
  request.cacheProtocolVersion = hf3fs::cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->getUploadJob(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->job);
}

CoTryTask<Void> MetaUploadJobWaiterBackend::wait(Duration delay) {
  co_await folly::coro::sleep(delay.asUs());
  co_return Void{};
}

std::chrono::steady_clock::time_point MetaUploadJobWaiterBackend::now() const {
  return std::chrono::steady_clock::now();
}

UploadJobWaiter::UploadJobWaiter(std::shared_ptr<UploadJobWaiterBackend> backend, UploadJobWaiterConfig config)
    : backend_(std::move(backend)),
      config_(config) {}

bool UploadJobWaiter::retryable(const Status &status) {
  return status.code() == hf3fs::CacheCode::kTimeout || status.code() == hf3fs::CacheCode::kUnavailable ||
         status.code() == hf3fs::RPCCode::kTimeout || status.code() == hf3fs::RPCCode::kSendFailed ||
         status.code() == hf3fs::RPCCode::kSocketClosed || status.code() == hf3fs::RPCCode::kConnectFailed;
}

Result<hf3fs::cache::UploadJobRecord> UploadJobWaiter::terminalResult(hf3fs::cache::UploadJobRecord job) {
  switch (job.state) {
    case hf3fs::cache::UploadJobState::PUBLISHED:
      return job;
    case hf3fs::cache::UploadJobState::FAILED:
      return makeError(hf3fs::CacheCode::kUnavailable, "write-through publish failed");
    case hf3fs::cache::UploadJobState::CANCELLED:
      return makeError(hf3fs::MetaCode::kRequestCanceled, "write-through publish was cancelled");
    default:
      return makeError(hf3fs::CacheCode::kStateConflict, "write-through publish is not terminal");
  }
}

CoTryTask<hf3fs::cache::UploadJobRecord> UploadJobWaiter::query(flat::UserInfo user, hf3fs::cache::UploadJobId jobId) {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_ || jobId == hf3fs::cache::UploadJobId{}) {
    co_return makeError(StatusCode::kInvalidArg, "upload job waiter is not configured");
  }
  auto job = co_await backend_->get(std::move(user), jobId);
  CO_RETURN_ON_ERROR(job);
  CO_RETURN_ON_ERROR(job->valid());
  if (job->jobId != jobId) co_return makeError(hf3fs::CacheCode::kInvalidResponse, "upload job identity changed");
  co_return std::move(*job);
}

CoTryTask<hf3fs::cache::UploadJobRecord> UploadJobWaiter::awaitTerminal(flat::UserInfo user,
                                                                        hf3fs::cache::UploadJobId jobId,
                                                                        std::chrono::steady_clock::time_point deadline,
                                                                        Cancelled cancelled) {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_ || jobId == hf3fs::cache::UploadJobId{}) {
    co_return makeError(StatusCode::kInvalidArg, "upload job waiter is not configured");
  }
  while (true) {
    if (cancelled && cancelled()) {
      co_return makeError(hf3fs::MetaCode::kRequestCanceled, "upload job wait cancelled");
    }
    auto job = co_await backend_->get(user, jobId);
    if (job.hasValue()) {
      CO_RETURN_ON_ERROR(job->valid());
      if (job->jobId != jobId) co_return makeError(hf3fs::CacheCode::kInvalidResponse, "upload job identity changed");
      if (job->state == hf3fs::cache::UploadJobState::PUBLISHED || job->state == hf3fs::cache::UploadJobState::FAILED ||
          job->state == hf3fs::cache::UploadJobState::CANCELLED) {
        co_return terminalResult(std::move(*job));
      }
    } else if (!retryable(job.error())) {
      co_return makeError(job.error());
    }

    auto now = backend_->now();
    if (now >= deadline) co_return makeError(hf3fs::CacheCode::kTimeout, "upload job wait timed out");
    auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
    auto delay = Duration{std::min<std::chrono::nanoseconds>(config_.pollInterval, remaining)};
    CO_RETURN_ON_ERROR(co_await backend_->wait(delay));
  }
}

}  // namespace hf3fs::client::cache
