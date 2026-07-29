#pragma once

#include <functional>
#include <memory>
#include <mutex>

#include "cache_manager/config/Config.h"
#include "common/utils/BackgroundRunner.h"
#include "fbs/cache_manager/Service.h"

namespace hf3fs::cache_manager {

class CacheManagerOperator {
 public:
  using SchedulerStartHook = std::function<Result<Void>()>;
  using SchedulerStopHook = std::function<void()>;

  CacheManagerOperator(const Config &config,
                       std::shared_ptr<meta::client::MetaClient> metaClient,
                       std::shared_ptr<storage::client::StorageClient> storageClient);
  ~CacheManagerOperator();

  Result<Void> start(CPUExecutorGroup &executor);
  Result<Void> startForTest(SchedulerStartHook startHook, SchedulerStopHook stopHook = {});
  void stop();
  bool running() const;

  CoTryTask<EnsureCachedRsp> ensureCached(const EnsureCachedReq &req);
  CoTryTask<ReportCacheBlockInvalidRsp> reportCacheBlockInvalid(const ReportCacheBlockInvalidReq &req);
  CoTryTask<AdminCleanupCacheBlocksRsp> adminCleanupCacheBlocks(const AdminCleanupCacheBlocksReq &req);
  CoTryTask<GetCacheStatusRsp> getCacheStatus(const GetCacheStatusReq &req);

 private:
  Result<Void> checkProtocol(uint32_t version) const;
  Result<Void> checkService(const ServiceIdentity &service) const;

  const Config &config_;
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::shared_ptr<storage::client::StorageClient> storageClient_;
  std::unique_ptr<BackgroundRunner> scheduler_;
  SchedulerStopHook schedulerStopHook_;
  mutable std::mutex mutex_;
  bool running_ = false;
};

}  // namespace hf3fs::cache_manager
