#pragma once

#include <atomic>
#include <functional>

#include "cache_manager/admission/AdmissionPolicy.h"
#include "cache_manager/capacity/PhysicalPreflight.h"
#include "cache_manager/cleanup/CacheCleanupWorker.h"
#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/scheduler/HintCoalescer.h"

namespace hf3fs::cache_manager {

class EnsureCached {
 public:
  using SteadyClockFn = std::function<SteadyTime()>;
  using WallClockNsFn = std::function<uint64_t()>;

  EnsureCached(std::shared_ptr<CacheManagerBackend> backend,
               HintCoalescer &hints,
               CacheCleanupWorker *cleanupWorker = nullptr)
      : backend_(std::move(backend)),
        hints_(hints),
        cleanupWorker_(cleanupWorker) {}

  EnsureCached(std::shared_ptr<CacheManagerBackend> backend,
               HintCoalescer &hints,
               CacheCleanupWorker *cleanupWorker,
               AdmissionPolicy &admissionPolicy,
               PhysicalPreflight &physicalPreflight,
               Uuid managerEpoch,
               Duration permitTtl,
               SteadyClockFn steadyClock = SteadyClock::now,
               WallClockNsFn wallClockNs = {});

  CoTryTask<EnsureCachedRsp> run(const EnsureCachedReq &req);
  BypassReason lastBypassReason() const { return lastBypassReason_.load(std::memory_order_relaxed); }

 private:
  CoTryTask<EnsureCachedRsp> runLegacy(const EnsureCachedReq &req,
                                       const meta::Inode &inode,
                                       const std::vector<meta::CacheBlockRequestBase> &items);
  CoTryTask<EnsureCachedRsp> runPhase2(const EnsureCachedReq &req,
                                       const meta::Inode &inode,
                                       const std::vector<meta::CacheBlockRequestBase> &items);
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueueWithRetry(const meta::CacheBlockRequestBase &item);
  CoTryTask<void> release(const storage::PermitIdentity &permit);
  EnsureCachedRsp respond(const meta::Inode &inode,
                          const EnsureCachedReq &req,
                          EnsureCachedStatus status,
                          BypassReason bypassReason = BypassReason::NONE);

  std::shared_ptr<CacheManagerBackend> backend_;
  HintCoalescer &hints_;
  CacheCleanupWorker *cleanupWorker_;
  AdmissionPolicy *admissionPolicy_{nullptr};
  PhysicalPreflight *physicalPreflight_{nullptr};
  Uuid managerEpoch_{Uuid::zero()};
  Duration permitTtl_{0_ns};
  SteadyClockFn steadyClock_{SteadyClock::now};
  WallClockNsFn wallClockNs_;
  std::atomic<BypassReason> lastBypassReason_{BypassReason::NONE};
};

}  // namespace hf3fs::cache_manager
