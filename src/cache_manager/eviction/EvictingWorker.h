#pragma once

#include <memory>

#include "cache_manager/loader/CacheLoader.h"

namespace hf3fs::cache_manager {

struct EvictingRunResult {
  size_t scanned{0};
  size_t completed{0};
  size_t pending{0};
  size_t failed{0};
};

class EvictingWorker {
 public:
  EvictingWorker(std::shared_ptr<CacheManagerBackend> backend, uint32_t pageSize)
      : backend_(std::move(backend)),
        pageSize_(pageSize) {}

  CoTryTask<EvictingRunResult> runOnce();

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
  uint32_t pageSize_;
};

}  // namespace hf3fs::cache_manager
