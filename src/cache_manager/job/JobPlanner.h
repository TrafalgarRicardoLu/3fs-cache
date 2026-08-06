#pragma once

#include "cache_manager/planner/SourcePlanner.h"
#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

class JobPlannerBackend {
 public:
  virtual ~JobPlannerBackend() = default;
  virtual CoTryTask<void> append(cache::PrefetchJobId jobId,
                                 std::vector<cache::PrefetchPlanEntry> entries,
                                 uint32_t sourceIndex,
                                 std::string cursor,
                                 bool complete,
                                 uint64_t activePinExpiresAtMs) = 0;
  virtual CoTryTask<cache::PrefetchJobRecord> get(cache::PrefetchJobId jobId) = 0;
};

class MetaJobPlannerBackend final : public JobPlannerBackend {
 public:
  MetaJobPlannerBackend(std::shared_ptr<meta::client::MetaClient> metaClient, meta::CacheServiceIdentity service)
      : metaClient_(std::move(metaClient)),
        service_(std::move(service)) {}

  CoTryTask<void> append(cache::PrefetchJobId jobId,
                         std::vector<cache::PrefetchPlanEntry> entries,
                         uint32_t sourceIndex,
                         std::string cursor,
                         bool complete,
                         uint64_t activePinExpiresAtMs) override;
  CoTryTask<cache::PrefetchJobRecord> get(cache::PrefetchJobId jobId) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  meta::CacheServiceIdentity service_;
};

class JobPlanner {
 public:
  using WallClockMs = std::function<uint64_t()>;

  JobPlanner(std::shared_ptr<JobPlannerBackend> backend,
             std::shared_ptr<SourcePlannerFactory> factory,
             uint32_t pageLimit,
             uint64_t activePinTtlMs = 0,
             WallClockMs wallClockMs = {})
      : backend_(std::move(backend)),
        factory_(std::move(factory)),
        pageLimit_(pageLimit),
        activePinTtlMs_(activePinTtlMs),
        wallClockMs_(std::move(wallClockMs)) {}

  CoTryTask<cache::PrefetchJobRecord> runNextPage(const cache::PrefetchJobRecord &job,
                                                  const CancellationToken &cancellation = {});

 private:
  std::shared_ptr<JobPlannerBackend> backend_;
  std::shared_ptr<SourcePlannerFactory> factory_;
  uint32_t pageLimit_;
  uint64_t activePinTtlMs_;
  WallClockMs wallClockMs_;
};

}  // namespace hf3fs::cache_manager
