#pragma once

#include <map>
#include <memory>
#include <set>

#include "storage/store/cache/LocalEvictionPolicy.h"

namespace hf3fs::storage {

struct Components;

struct LocalSafetyDisk {
  PhysicalDiskId diskId;
  uint64_t capacityBytes{0};
  uint64_t physicalUsedBytes{0};
  uint64_t reservedBytes{0};
};

struct LocalSafetyRunResult {
  std::set<PhysicalDiskId> pressuredDisks;
  size_t candidates{0};
  size_t selected{0};
  size_t retired{0};
  uint64_t releasedBytes{0};
};

class LocalSafetyBackend {
 public:
  virtual ~LocalSafetyBackend() = default;
  virtual Result<std::vector<LocalSafetyDisk>> diskSpace() = 0;
  virtual Result<std::vector<LocalEvictionCandidate>> candidates(const PhysicalDiskId &) = 0;
  // The implementation must durably PREPARE EMERGENCY_EVICTED before retiring
  // the exact descriptor generation, then make the event DELIVERABLE.
  virtual Result<bool> retire(const PhysicalDiskId &, const LocalEvictionCandidate &, Uuid operationId) = 0;
  virtual Result<size_t> recoverPrepared() { return size_t{0}; }
};

class LocalSafetyEvictor {
 public:
  LocalSafetyEvictor(std::shared_ptr<LocalSafetyBackend> backend,
                     std::unique_ptr<LocalEvictionPolicy> policy,
                     double highWatermark,
                     double lowWatermark,
                     uint64_t protectionPeriodNs)
      : backend_(std::move(backend)),
        policy_(std::move(policy)),
        highWatermark_(highWatermark),
        lowWatermark_(lowWatermark),
        protectionPeriodNs_(protectionPeriodNs) {}

  Result<LocalSafetyRunResult> runOnce(uint64_t nowNs);

 private:
  std::shared_ptr<LocalSafetyBackend> backend_;
  std::unique_ptr<LocalEvictionPolicy> policy_;
  double highWatermark_;
  double lowWatermark_;
  uint64_t protectionPeriodNs_;
  std::set<PhysicalDiskId> pressured_;
};

std::shared_ptr<LocalSafetyBackend> createStorageLocalSafetyBackend(Components &components);

}  // namespace hf3fs::storage
