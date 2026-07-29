#pragma once

#include "cache_manager/loader/CacheLoader.h"

namespace hf3fs::cache_manager {

class CacheCleanupWorker {
 public:
  explicit CacheCleanupWorker(std::shared_ptr<CacheManagerBackend> backend)
      : backend_(std::move(backend)) {}

  CoTryTask<void> clean(meta::BeginCleanCacheBlockItem item);

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
};

}  // namespace hf3fs::cache_manager
