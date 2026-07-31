#pragma once

#include <memory>

#include "cache_manager/capacity/PhysicalTopology.h"
#include "cache_manager/eviction/EvictionCandidateSource.h"
#include "cache_manager/eviction/EvictionPolicy.h"
#include "cache_manager/eviction/EvictionPressureState.h"

namespace hf3fs::cache_manager {

struct EvictionControllerConfig {
  double highWatermark{0.9};
  double lowWatermark{0.8};
  Duration snapshotMaxAge{15_s};
  Duration protectionPeriod{10_min};
  uint32_t candidatePageSize{1000};
  uint32_t batchSize{256};

  Result<Void> valid() const;
};

struct EvictionRunResult {
  std::set<storage::PhysicalDiskId> pressuredDisks;
  std::map<storage::PhysicalDiskId, uint64_t> deficits;
  size_t candidates{0};
  size_t selected{0};
  size_t begun{0};
  size_t conflicts{0};
  bool targetMet{false};
};

class EvictionController {
 public:
  EvictionController(std::shared_ptr<CacheManagerBackend> backend,
                     PhysicalTopology &topology,
                     EvictionPolicy &policy,
                     EvictionPressureState &pressure,
                     EvictionControllerConfig config);

  CoTryTask<EvictionRunResult> runOnce(SteadyTime steadyNow = SteadyClock::now(), UtcTime wallNow = UtcClock::now());

 private:
  Result<std::map<storage::PhysicalDiskId, uint64_t>> updatePressure(
      const std::map<storage::PhysicalDiskId, DiskSpaceSnapshot> &snapshots);
  CoTryTask<std::vector<EvictionCandidate>> collectCandidates(const std::set<storage::PhysicalDiskId> &pressuredDisks,
                                                              UtcTime wallNow);

  std::shared_ptr<CacheManagerBackend> backend_;
  PhysicalTopology &topology_;
  EvictionPolicy &policy_;
  EvictionPressureState &pressure_;
  EvictionControllerConfig config_;
  EvictionCandidateSource candidateSource_;
};

}  // namespace hf3fs::cache_manager
