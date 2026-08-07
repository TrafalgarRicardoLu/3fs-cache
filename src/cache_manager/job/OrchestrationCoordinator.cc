#include "cache_manager/job/OrchestrationCoordinator.h"

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {

CoTryTask<meta::ListPrefetchJobsRsp> MetaOrchestrationCoordinatorBackend::list(
    std::optional<cache::PrefetchJobId> after,
    uint32_t limit) {
  meta::ListPrefetchJobsReq request;
  request.service = service_;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  co_return co_await metaClient_->listPrefetchJobs(std::move(request));
}

OrchestrationCoordinator::OrchestrationCoordinator(std::shared_ptr<OrchestrationCoordinatorBackend> backend,
                                                   uint32_t jobPageLimit,
                                                   PlannerTick planner,
                                                   RunnerTick runner,
                                                   TrackerTick tracker,
                                                   RunnerTick recoveryRunner)
    : backend_(std::move(backend)),
      jobPageLimit_(jobPageLimit),
      planner_(std::move(planner)),
      runner_(std::move(runner)),
      recoveryRunner_(std::move(recoveryRunner)),
      tracker_(std::move(tracker)) {}

bool OrchestrationCoordinator::terminal(cache::PrefetchJobState state) {
  return state == cache::PrefetchJobState::READY || state == cache::PrefetchJobState::FAILED ||
         state == cache::PrefetchJobState::CANCELLED;
}

void OrchestrationCoordinator::remember(cache::PrefetchJobRecord job) {
  auto lock = std::scoped_lock(mutex_);
  auto key = job.spec.jobId.toUnderType();
  if (terminal(job.state))
    jobs_.erase(key);
  else
    jobs_[key] = std::move(job);
}

std::vector<cache::PrefetchJobRecord> OrchestrationCoordinator::snapshot() const {
  auto lock = std::scoped_lock(mutex_);
  std::vector<cache::PrefetchJobRecord> result;
  result.reserve(jobs_.size());
  for (const auto &[_, job] : jobs_) result.push_back(job);
  return result;
}

CoTryTask<void> OrchestrationCoordinator::refresh() {
  if (stopping_.load(std::memory_order_acquire)) throw OperationCancelled();
  if (jobPageLimit_ == 0 || jobPageLimit_ > meta::kMaxCacheBatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid orchestration Job page limit");
  }
  std::optional<cache::PrefetchJobId> after;
  do {
    auto page = co_await backend_->list(after, jobPageLimit_);
    CO_RETURN_ON_ERROR(page);
    if (page->jobs.size() > jobPageLimit_ || (page->more && page->jobs.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid orchestration Job page");
    }
    auto nextAfter = page->more ? std::optional{page->jobs.back().spec.jobId} : std::nullopt;
    for (auto &job : page->jobs) {
      CO_RETURN_ON_ERROR(job.valid());
      remember(std::move(job));
    }
    if (!page->more) break;
    after = nextAfter;
  } while (true);
  cache::metrics::setGauge(cache::metrics::Event::MANAGER_ACTIVE_JOBS, static_cast<int64_t>(activeJobs()));
  co_return Void{};
}

CoTryTask<uint32_t> OrchestrationCoordinator::recover() {
  CO_RETURN_ON_ERROR(co_await refresh());
  CO_RETURN_ON_ERROR(co_await runJobs(recoveryRunner_));
  co_return static_cast<uint32_t>(activeJobs());
}

CoTryTask<void> OrchestrationCoordinator::runPlannerOnce() {
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_ORCHESTRATION_TICK, 1, {.reason = "planner"});
  CO_RETURN_ON_ERROR(co_await refresh());
  if (!planner_) co_return Void{};
  for (const auto &job : snapshot()) {
    if (stopping_.load(std::memory_order_acquire)) throw OperationCancelled();
    if (job.planningComplete) continue;
    auto updated = co_await planner_(job, cancellation_.getToken());
    CO_RETURN_ON_ERROR(updated);
    remember(std::move(*updated));
  }
  co_return Void{};
}

CoTryTask<void> OrchestrationCoordinator::runJobs(const RunnerTick &runner) {
  if (!runner) co_return Void{};
  for (const auto &job : snapshot()) {
    if (stopping_.load(std::memory_order_acquire)) throw OperationCancelled();
    if (job.plannedBlocks == 0) continue;
    std::optional<cache::CacheBlockKey> after;
    do {
      auto page = co_await runner(job, after, cancellation_.getToken());
      CO_RETURN_ON_ERROR(page);
      if (page->throttled || !page->more) break;
      if (!page->nextAfter) co_return makeError(CacheCode::kInvalidResponse, "JobRunner page did not advance");
      after = page->nextAfter;
    } while (true);
  }
  co_return Void{};
}

CoTryTask<void> OrchestrationCoordinator::runRunnerOnce() {
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_ORCHESTRATION_TICK, 1, {.reason = "runner"});
  CO_RETURN_ON_ERROR(co_await refresh());
  co_return co_await runJobs(runner_);
}

CoTryTask<void> OrchestrationCoordinator::runTrackerOnce() {
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_ORCHESTRATION_TICK, 1, {.reason = "tracker"});
  CO_RETURN_ON_ERROR(co_await refresh());
  if (!tracker_) co_return Void{};
  for (const auto &job : snapshot()) {
    if (stopping_.load(std::memory_order_acquire)) throw OperationCancelled();
    CO_RETURN_ON_ERROR(co_await tracker_(job, cancellation_.getToken()));
  }
  co_return Void{};
}

void OrchestrationCoordinator::stop() {
  if (!stopping_.exchange(true, std::memory_order_acq_rel)) cancellation_.requestCancellation();
}

size_t OrchestrationCoordinator::activeJobs() const {
  auto lock = std::scoped_lock(mutex_);
  return jobs_.size();
}

}  // namespace hf3fs::cache_manager
