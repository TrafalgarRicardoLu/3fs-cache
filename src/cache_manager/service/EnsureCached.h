#pragma once

#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/scheduler/HintCoalescer.h"

namespace hf3fs::cache_manager {

class EnsureCached {
 public:
  EnsureCached(std::shared_ptr<CacheManagerBackend> backend, HintCoalescer &hints)
      : backend_(std::move(backend)),
        hints_(hints) {}

  CoTryTask<EnsureCachedRsp> run(const EnsureCachedReq &req);

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
  HintCoalescer &hints_;
};

}  // namespace hf3fs::cache_manager
