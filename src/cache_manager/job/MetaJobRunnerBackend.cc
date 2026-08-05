#include "cache_manager/job/MetaJobRunnerBackend.h"

namespace hf3fs::cache_manager {

CoTryTask<meta::ListPrefetchPlanRsp> MetaJobRunnerBackend::list(cache::PrefetchJobId jobId,
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

CoTryTask<cache::PrefetchPlanEntry> MetaJobRunnerBackend::update(const cache::PrefetchPlanEntry &expected,
                                                                 const cache::PrefetchPlanEntry &desired) {
  meta::UpdatePrefetchPlanEntriesReq request;
  request.service = service_;
  request.items.push_back({expected, desired});
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto response = co_await metaClient_->updatePrefetchPlanEntries(std::move(request));
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != 1) {
    co_return makeError(CacheCode::kInvalidResponse, "invalid prefetch plan update result count");
  }
  CO_RETURN_ON_ERROR(response->results.front());
  co_return *response->results.front();
}

CoTryTask<JobAdmissionResult> MetaJobRunnerBackend::admit(const cache::PrefetchPlanEntry &entry,
                                                          Completion completion) {
  auto response = co_await admission_.runPrefetch(entry, completion);
  CO_RETURN_ON_ERROR(response);
  if (response->status == EnsureCachedStatus::ACCEPTED) {
    co_return JobAdmissionResult{JobAdmissionDisposition::QUEUED};
  }
  if (response->status == EnsureCachedStatus::ATTACHED) {
    completion(Status::OK);
    co_return JobAdmissionResult{JobAdmissionDisposition::LOADING};
  }
  switch (response->bypassReason) {
    case BypassReason::CAPACITY:
      co_return JobAdmissionResult{JobAdmissionDisposition::CAPACITY_WAIT};
    case BypassReason::ADMISSION_DISABLED:
    case BypassReason::FEATURE_DISABLED:
    case BypassReason::POLICY:
    case BypassReason::UNAVAILABLE:
      co_return JobAdmissionResult{JobAdmissionDisposition::RETRYABLE};
    case BypassReason::EMPTY_RANGE:
      co_return JobAdmissionResult{JobAdmissionDisposition::TERMINAL};
    case BypassReason::NONE:
      break;
  }
  co_return makeError(CacheCode::kInvalidResponse, "invalid explicit admission response");
}

}  // namespace hf3fs::cache_manager
