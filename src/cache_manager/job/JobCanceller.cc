#include "cache_manager/job/JobCanceller.h"

#include <algorithm>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {

CoTryTask<cache::PrefetchJobRecord> MetaJobCancellerBackend::cancelJob(cache::PrefetchJobId jobId) {
  meta::CancelPrefetchJobReq request;
  request.service = service_;
  request.jobId = jobId;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto response = co_await metaClient_->cancelPrefetchJob(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->job);
}

CoTryTask<meta::ListPrefetchPlanRsp> MetaJobCancellerBackend::list(cache::PrefetchJobId jobId,
                                                                   std::optional<cache::CacheBlockKey> after,
                                                                   uint32_t limit) {
  meta::ListPrefetchPlanReq request;
  request.service = service_;
  request.jobId = jobId;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  co_return co_await metaClient_->listPrefetchPlan(std::move(request));
}

CoTryTask<cache::PrefetchPlanEntry> MetaJobCancellerBackend::update(const cache::PrefetchPlanEntry &expected,
                                                                    const cache::PrefetchPlanEntry &desired) {
  meta::UpdatePrefetchPlanEntriesReq request;
  request.service = service_;
  request.items.push_back({expected, desired});
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto response = co_await metaClient_->updatePrefetchPlanEntries(std::move(request));
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != 1) {
    co_return makeError(CacheCode::kInvalidResponse, "invalid cancellation plan update result count");
  }
  CO_RETURN_ON_ERROR(response->results.front());
  co_return *response->results.front();
}

CoTryTask<void> MetaJobCancellerBackend::cancelQueued(const cache::CacheBlockKey &key,
                                                      const storage::PermitIdentity &permit) {
  CO_RETURN_ON_ERROR(co_await cacheBackend_->cancelQueuedAdmission(key, permit));
  co_return co_await cacheBackend_->releasePermit(permit);
}

std::optional<storage::PermitIdentity> JobCanceller::pending(cache::PrefetchJobId jobId,
                                                             const cache::CacheBlockKey &key) const {
  auto lock = std::scoped_lock(mutex_);
  auto found = std::find_if(pendingPermits_.begin(), pendingPermits_.end(), [&](const auto &item) {
    return item.jobId == jobId && item.key == key;
  });
  return found == pendingPermits_.end() ? std::nullopt : std::optional{found->permit};
}

void JobCanceller::remember(cache::PrefetchJobId jobId,
                            const cache::CacheBlockKey &key,
                            const storage::PermitIdentity &permit) {
  auto lock = std::scoped_lock(mutex_);
  auto found = std::find_if(pendingPermits_.begin(), pendingPermits_.end(), [&](const auto &item) {
    return item.jobId == jobId && item.key == key;
  });
  if (found != pendingPermits_.end()) return;
  pendingPermits_.push_back({jobId, key, permit});
}

void JobCanceller::forget(cache::PrefetchJobId jobId, const cache::CacheBlockKey &key) {
  auto lock = std::scoped_lock(mutex_);
  std::erase_if(pendingPermits_, [&](const auto &item) { return item.jobId == jobId && item.key == key; });
}

CoTryTask<JobCancellationResult> JobCanceller::cancel(cache::PrefetchJobId jobId,
                                                      const CancellationToken &cancellation) {
  if (pageLimit_ == 0 || pageLimit_ > meta::kMaxCacheBatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid Job cancellation page limit");
  }
  auto job = co_await backend_->cancelJob(jobId);
  CO_RETURN_ON_ERROR(job);
  if (job->state != cache::PrefetchJobState::CANCELLED || job->cancelEpoch == 0) {
    co_return makeError(CacheCode::kInvalidResponse, "Job cancellation was not persisted");
  }
  JobCancellationResult result;
  result.job = *job;
  std::optional<cache::CacheBlockKey> after;
  do {
    if (cancellation.isCancellationRequested()) throw OperationCancelled();
    auto page = co_await backend_->list(jobId, after, pageLimit_);
    CO_RETURN_ON_ERROR(page);
    if (page->entries.size() > pageLimit_ || (page->more && page->entries.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid cancellation plan page");
    }
    for (const auto &entry : page->entries) {
      if (entry.state == cache::PrefetchPlanEntryState::READY || entry.state == cache::PrefetchPlanEntryState::FAILED ||
          entry.state == cache::PrefetchPlanEntryState::CANCELLED) {
        continue;
      }
      auto queuedPermit = pending(jobId, entry.key);
      if (!queuedPermit) {
        auto local = hints_.cancelClaim(entry.key, jobId);
        if (local.hintRemoved && local.permit) {
          queuedPermit = local.permit;
          remember(jobId, entry.key, *local.permit);
        }
      }
      if (queuedPermit) {
        auto cancelled = co_await backend_->cancelQueued(entry.key, *queuedPermit);
        if (cancelled.hasError() && cancelled.error().code() != CacheCode::kStateConflict) {
          co_return makeError(cancelled.error());
        }
        forget(jobId, entry.key);
        if (cancelled.hasValue()) ++result.cancelledQueuedLoads;
      }
      auto desired = entry;
      desired.state = cache::PrefetchPlanEntryState::CANCELLED;
      desired.ready.reset();
      CO_RETURN_ON_ERROR(co_await backend_->update(entry, desired));
      ++result.cancelledEntries;
    }
    if (!page->more) break;
    after = page->entries.back().key;
  } while (true);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_JOB_CANCEL,
                              1,
                              {.sourceId = jobId.toUnderType().toHexString(), .reason = "cancelled"});
  co_return result;
}

}  // namespace hf3fs::cache_manager
