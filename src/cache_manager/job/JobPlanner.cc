#include "cache_manager/job/JobPlanner.h"

#include <limits>

namespace hf3fs::cache_manager {

CoTryTask<void> MetaJobPlannerBackend::append(cache::PrefetchJobId jobId,
                                              std::vector<cache::PrefetchPlanEntry> entries,
                                              uint32_t sourceIndex,
                                              std::string cursor,
                                              bool complete,
                                              uint64_t activePinExpiresAtMs) {
  meta::AppendPrefetchPlanReq request;
  request.service = service_;
  request.jobId = jobId;
  request.entries = std::move(entries);
  request.plannerSourceIndex = sourceIndex;
  request.plannerCursor = std::move(cursor);
  request.planningComplete = complete;
  request.activePinExpiresAtMs = activePinExpiresAtMs;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  CO_RETURN_ON_ERROR(co_await metaClient_->appendPrefetchPlan(std::move(request)));
  co_return Void{};
}

CoTryTask<cache::PrefetchJobRecord> MetaJobPlannerBackend::get(cache::PrefetchJobId jobId) {
  meta::GetPrefetchJobReq request;
  request.service = service_;
  request.jobId = jobId;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto response = co_await metaClient_->getPrefetchJob(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->job);
}

CoTryTask<cache::PrefetchJobRecord> JobPlanner::runNextPage(const cache::PrefetchJobRecord &job,
                                                            const CancellationToken &cancellation) {
  CO_RETURN_ON_ERROR(job.valid());
  if (cancellation.isCancellationRequested()) throw OperationCancelled();
  if (pageLimit_ == 0 || pageLimit_ > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid JobPlanner page limit");
  }
  if (job.planningComplete) co_return job;
  if (job.state != cache::PrefetchJobState::PENDING && job.state != cache::PrefetchJobState::PLANNING) {
    co_return makeError(CacheCode::kStateConflict, "Job is not in a planning state");
  }
  if (job.plannerSourceIndex >= job.spec.sources.size()) {
    co_return makeError(CacheCode::kStateConflict, "Job planner source cursor is exhausted");
  }
  PlannerContext context{job.spec.jobId, job.plannerSourceIndex, job.spec.priority, pageLimit_, cancellation};
  auto planner = factory_->make(job.spec.sources[job.plannerSourceIndex], std::move(context));
  CO_RETURN_ON_ERROR(planner);
  auto page = co_await (*planner)->nextPage(job.plannerCursor);
  CO_RETURN_ON_ERROR(page);

  auto sourceIndex = job.plannerSourceIndex;
  auto cursor = page->nextCursor;
  if (page->done) {
    ++sourceIndex;
    cursor.clear();
  }
  const bool complete = sourceIndex == job.spec.sources.size();
  uint64_t pinExpiresAtMs = 0;
  if (activePinTtlMs_ != 0) {
    if (!wallClockMs_) co_return makeError(StatusCode::kInvalidConfig, "active Job pin clock is not configured");
    auto nowMs = wallClockMs_();
    if (nowMs == 0 || activePinTtlMs_ > std::numeric_limits<uint64_t>::max() - nowMs) {
      co_return makeError(CacheCode::kStateConflict, "active Job pin expiry overflow");
    }
    pinExpiresAtMs = nowMs + activePinTtlMs_;
  }
  CO_RETURN_ON_ERROR(
      co_await backend_
          ->append(job.spec.jobId, std::move(page->entries), sourceIndex, std::move(cursor), complete, pinExpiresAtMs));
  co_return co_await backend_->get(job.spec.jobId);
}

}  // namespace hf3fs::cache_manager
