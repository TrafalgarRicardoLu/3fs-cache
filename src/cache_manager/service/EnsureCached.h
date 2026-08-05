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
  using AttachFn = std::function<Result<bool>(LoadHint)>;
  using PrefetchCompletion = std::function<void(const Status &)>;

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
               WallClockNsFn wallClockNs = {},
               AttachFn attach = {});

  CoTryTask<EnsureCachedRsp> run(const EnsureCachedReq &req);
  CoTryTask<EnsureCachedRsp> runPrefetch(const cache::PrefetchPlanEntry &entry, PrefetchCompletion completion);
  BypassReason lastBypassReason() const { return lastBypassReason_.load(std::memory_order_relaxed); }

 private:
  struct PrefetchContext {
    cache::PrefetchJobId jobId;
    Uuid attemptId;
    uint64_t blockLength{0};
    PrefetchCompletion completion;
  };

  CoTryTask<EnsureCachedRsp> run(const EnsureCachedReq &req, const PrefetchContext *prefetch);
  CoTryTask<EnsureCachedRsp> runLegacy(const EnsureCachedReq &req,
                                       const meta::Inode &inode,
                                       const std::vector<meta::CacheBlockRequestBase> &items);
  CoTryTask<EnsureCachedRsp> runPhase2(const EnsureCachedReq &req,
                                       const meta::Inode &inode,
                                       const std::vector<meta::CacheBlockRequestBase> &items,
                                       const PrefetchContext *prefetch);
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueueWithRetry(const meta::CacheBlockRequestBase &item);
  CoTryTask<void> release(const storage::PermitIdentity &permit);
  CoTryTask<void> cancelQueued(const cache::CacheBlockKey &key, const storage::PermitIdentity &permit);
  Result<bool> attach(LoadHint hint);
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
  AttachFn attach_;
  std::atomic<BypassReason> lastBypassReason_{BypassReason::NONE};
};

}  // namespace hf3fs::cache_manager
