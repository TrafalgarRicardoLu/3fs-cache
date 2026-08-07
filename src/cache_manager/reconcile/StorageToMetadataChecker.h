#pragma once

#include <span>

#include "cache_manager/reconcile/ReconcileControl.h"
#include "cache_manager/reconcile/StorageInventory.h"

namespace hf3fs::cache_manager {

struct StorageToMetadataResult {
  uint64_t scanned{0};
  uint64_t delegated{0};
  uint64_t orphans{0};
  uint64_t older{0};
  uint64_t newer{0};
  uint64_t retired{0};
  uint64_t conflicts{0};
  uint64_t retryable{0};
  uint64_t deferred{0};
  bool stopped{false};
};

class StorageToMetadataChecker {
 public:
  StorageToMetadataChecker(std::shared_ptr<CacheManagerBackend> backend,
                           uint32_t batchSize,
                           bool dryRun = false,
                           std::shared_ptr<ReconcileRunControl> control = nullptr)
      : backend_(std::move(backend)),
        batchSize_(batchSize),
        dryRun_(dryRun),
        control_(std::move(control)) {}

  CoTryTask<StorageToMetadataResult> run(std::span<const TargetCacheInventory> inventories);

 private:
  static bool forwardOwned(cache::CacheBlockState state);
  CoTask<void> retire(const storage::CacheInventoryEntry &entry, StorageToMetadataResult &result);

  std::shared_ptr<CacheManagerBackend> backend_;
  uint32_t batchSize_;
  bool dryRun_;
  std::shared_ptr<ReconcileRunControl> control_;
};

}  // namespace hf3fs::cache_manager
