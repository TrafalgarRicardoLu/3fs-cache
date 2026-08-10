#pragma once

#include <functional>
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
  using ShouldStop = std::function<bool()>;
  using PageHandler = std::function<CoTryTask<void>(TargetCacheInventory)>;

  StorageInventoryReader(std::shared_ptr<CacheManagerBackend> backend,
                         uint32_t pageSize,
                         uint32_t maxConcurrency,
                         ShouldStop shouldStop = {})
      : backend_(std::move(backend)),
        pageSize_(pageSize),
        maxConcurrency_(maxConcurrency),
        shouldStop_(std::move(shouldStop)) {}

  CoTryTask<TargetCacheInventory> readTarget(storage::TargetId targetId);
  CoTryTask<void> readTargetPages(storage::TargetId targetId, PageHandler handler);
  CoTask<std::vector<Result<TargetCacheInventory>>> readTargets(std::span<const storage::TargetId> targetIds);

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
  uint32_t pageSize_;
  uint32_t maxConcurrency_;
  ShouldStop shouldStop_;
};

}  // namespace hf3fs::cache_manager
