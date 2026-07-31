#pragma once

#include <functional>
#include <memory>

#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/scheduler/HintCoalescer.h"

namespace hf3fs::cache_manager {

class PermitRecovery {
 public:
  using WallClockNsFn = std::function<uint64_t()>;

  PermitRecovery(std::shared_ptr<CacheManagerBackend> backend,
                 HintCoalescer &hints,
                 Uuid managerEpoch,
                 Duration permitTtl,
                 uint32_t pageSize = 1000,
                 WallClockNsFn wallClockNs = {});

  CoTryTask<void> run();

 private:
  CoTryTask<void> recover(const meta::RecoverableCachePermit &item, uint64_t nowNs, uint64_t expiresAtNs);
  CoTryTask<void> attach(const meta::RecoverableCachePermit &item);
  CoTryTask<void> cancel(const meta::RecoverableCachePermit &item);

  std::shared_ptr<CacheManagerBackend> backend_;
  HintCoalescer &hints_;
  Uuid managerEpoch_;
  Duration permitTtl_;
  uint32_t pageSize_;
  WallClockNsFn wallClockNs_;
};

}  // namespace hf3fs::cache_manager
