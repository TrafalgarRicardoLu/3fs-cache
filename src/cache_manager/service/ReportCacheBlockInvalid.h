#pragma once

#include "cache_manager/cleanup/CacheCleanupWorker.h"

namespace hf3fs::cache_manager {

class ReportCacheBlockInvalid {
 public:
  ReportCacheBlockInvalid(std::shared_ptr<CacheManagerBackend> backend, CacheCleanupWorker &worker)
      : backend_(std::move(backend)),
        worker_(worker) {}
  CoTryTask<ReportCacheBlockInvalidRsp> run(const ReportCacheBlockInvalidReq &req);

 private:
  std::shared_ptr<CacheManagerBackend> backend_;
  CacheCleanupWorker &worker_;
};

}  // namespace hf3fs::cache_manager
