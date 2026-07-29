#pragma once

#include <atomic>

#include "cache_manager/cleanup/CacheCleanupWorker.h"
#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/scheduler/HintCoalescer.h"

namespace hf3fs::cache_manager {

class EnsureCached {
 public:
  EnsureCached(std::shared_ptr<CacheManagerBackend> backend,
               HintCoalescer &hints,
               CacheCleanupWorker *cleanupWorker = nullptr)
      : backend_(std::move(backend)),
        hints_(hints),
        cleanupWorker_(cleanupWorker) {}

  CoTryTask<EnsureCachedRsp> run(const EnsureCachedReq &req);
  BypassReason lastBypassReason() const { return lastBypassReason_.load(std::memory_order_relaxed); }

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
  HintCoalescer &hints_;
  CacheCleanupWorker *cleanupWorker_;
  std::atomic<BypassReason> lastBypassReason_{BypassReason::NONE};
};

}  // namespace hf3fs::cache_manager
