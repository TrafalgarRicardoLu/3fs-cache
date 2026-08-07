#pragma once

#include "cache_manager/cleanup/CacheCleanupWorker.h"
#include "cache_manager/reconcile/ReconcileControl.h"

namespace hf3fs::cache_manager {

struct MetadataToStorageResult {
  uint64_t scanned{0};
  uint64_t matched{0};
  uint64_t missing{0};
  uint64_t mismatched{0};
  uint64_t repaired{0};
  uint64_t conflicts{0};
  uint64_t retryable{0};
  uint64_t deferred{0};
  bool stopped{false};
};

class MetadataToStorageChecker {
 public:
  MetadataToStorageChecker(std::shared_ptr<CacheManagerBackend> backend,
                           CacheCleanupWorker &cleanup,
                           uint32_t pageSize,
                           std::shared_ptr<ReconcileRunControl> control = nullptr)
      : backend_(std::move(backend)),
        cleanup_(cleanup),
        pageSize_(pageSize),
        control_(std::move(control)) {}

  CoTryTask<MetadataToStorageResult> run();

 private:
  static bool matches(const meta::ReconcileCacheBlockStatus &status, const CacheReplicaObservation &observation);
  static bool definitelyMissing(const Status &error);
  static bool definitelyMismatched(const Status &error);
  CoTask<void> repair(const meta::ReconcileCacheBlockStatus &status,
                      std::optional<cache::CacheGeneration> observedGeneration,
                      MetadataToStorageResult &result);
  CoTask<void> replay(const meta::ReconcileCacheBlockStatus &status, MetadataToStorageResult &result);

  std::shared_ptr<CacheManagerBackend> backend_;
  CacheCleanupWorker &cleanup_;
  uint32_t pageSize_;
  std::shared_ptr<ReconcileRunControl> control_;
};

}  // namespace hf3fs::cache_manager
