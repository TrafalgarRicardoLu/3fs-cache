#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>

#include "cache_manager/job/JobPlanner.h"
#include "cache_manager/job/JobRunner.h"

namespace hf3fs::cache_manager {

class OrchestrationCoordinatorBackend {
 public:
  virtual ~OrchestrationCoordinatorBackend() = default;
  virtual CoTryTask<meta::ListPrefetchJobsRsp> list(std::optional<cache::PrefetchJobId> after, uint32_t limit) = 0;
};

class MetaOrchestrationCoordinatorBackend final : public OrchestrationCoordinatorBackend {
 public:
  MetaOrchestrationCoordinatorBackend(std::shared_ptr<meta::client::MetaClient> metaClient,
                                      meta::CacheServiceIdentity service)
      : metaClient_(std::move(metaClient)),
        service_(std::move(service)) {}

  CoTryTask<meta::ListPrefetchJobsRsp> list(std::optional<cache::PrefetchJobId> after, uint32_t limit) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  meta::CacheServiceIdentity service_;
};

class OrchestrationCoordinator {
 public:
  using PlannerTick =
      std::function<CoTryTask<cache::PrefetchJobRecord>(const cache::PrefetchJobRecord &, const CancellationToken &)>;
  using RunnerTick = std::function<CoTryTask<JobRunnerPageResult>(const cache::PrefetchJobRecord &,
                                                                  std::optional<cache::CacheBlockKey>,
                                                                  const CancellationToken &)>;
  using TrackerTick = std::function<CoTryTask<void>(const cache::PrefetchJobRecord &, const CancellationToken &)>;

  OrchestrationCoordinator(std::shared_ptr<OrchestrationCoordinatorBackend> backend,
                           uint32_t jobPageLimit,
                           PlannerTick planner,
                           RunnerTick runner,
                           TrackerTick tracker = {});

  CoTryTask<uint32_t> recover();
  CoTryTask<void> runPlannerOnce();
  CoTryTask<void> runRunnerOnce();
  CoTryTask<void> runTrackerOnce();
  void stop();
  size_t activeJobs() const;

 private:
  static bool terminal(cache::PrefetchJobState state);
  CoTryTask<void> refresh();
  std::vector<cache::PrefetchJobRecord> snapshot() const;
  void remember(cache::PrefetchJobRecord job);

  std::shared_ptr<OrchestrationCoordinatorBackend> backend_;
  uint32_t jobPageLimit_;
  PlannerTick planner_;
  RunnerTick runner_;
  TrackerTick tracker_;
  CancellationSource cancellation_;
  std::atomic<bool> stopping_{false};
  mutable std::mutex mutex_;
  std::map<Uuid, cache::PrefetchJobRecord> jobs_;
};

}  // namespace hf3fs::cache_manager
