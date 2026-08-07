#pragma once

#include <span>

#include "cache_manager/loader/CacheLoader.h"

namespace hf3fs::cache_manager {

struct TargetCacheInventory {
  storage::TargetId targetId{};
  Uuid inventoryEpoch{Uuid::zero()};
  std::vector<storage::CacheInventoryEntry> entries;
};

class StorageInventoryReader {
 public:
  StorageInventoryReader(std::shared_ptr<CacheManagerBackend> backend, uint32_t pageSize, uint32_t maxConcurrency)
      : backend_(std::move(backend)),
        pageSize_(pageSize),
        maxConcurrency_(maxConcurrency) {}

  CoTryTask<TargetCacheInventory> readTarget(storage::TargetId targetId);
  CoTask<std::vector<Result<TargetCacheInventory>>> readTargets(std::span<const storage::TargetId> targetIds);

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
  uint32_t pageSize_;
  uint32_t maxConcurrency_;
};

}  // namespace hf3fs::cache_manager
