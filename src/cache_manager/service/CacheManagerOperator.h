#pragma once

#include <functional>
#include <memory>
#include <mutex>

#include "cache_manager/access/AccessFlushWorker.h"
#include "cache_manager/admission/AdmissionPolicy.h"
#include "cache_manager/capacity/PhysicalPreflight.h"
#include "cache_manager/capacity/SpacePoller.h"
#include "cache_manager/cleanup/CacheCleanupWorker.h"
#include "cache_manager/config/Config.h"
#include "cache_manager/eviction/EvictingWorker.h"
#include "cache_manager/eviction/EvictionController.h"
#include "cache_manager/eviction/EvictionPolicy.h"
#include "cache_manager/eviction/EvictionPressureState.h"
#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/recovery/PermitRecovery.h"
#include "cache_manager/scheduler/LoaderScheduler.h"
#include "cache_manager/service/AdminCleanupCacheBlocks.h"
#include "cache_manager/service/EnsureCached.h"
#include "cache_manager/service/ReportCacheBlockInvalid.h"
#include "common/utils/BackgroundRunner.h"
#include "fbs/cache_manager/Service.h"

namespace hf3fs::cache_manager {

class CacheManagerOperator {
 public:
  using SchedulerStartHook = std::function<Result<Void>()>;
  using SchedulerStopHook = std::function<void()>;

  CacheManagerOperator(const Config &config,
                       std::shared_ptr<meta::client::MetaClient> metaClient,
                       std::shared_ptr<storage::client::StorageClient> storageClient,
                       std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient = nullptr,
                       RealCacheManagerBackend::Stores stores = {});
  ~CacheManagerOperator();

  Result<Void> start(CPUExecutorGroup &executor);
  Result<Void> startForTest(SchedulerStartHook startHook, SchedulerStopHook stopHook = {});
  void stop();
  bool running() const;

  CoTryTask<EnsureCachedRsp> ensureCached(const EnsureCachedReq &req);
  CoTryTask<ReportCacheBlockInvalidRsp> reportCacheBlockInvalid(const ReportCacheBlockInvalidReq &req);
  CoTryTask<AdminCleanupCacheBlocksRsp> adminCleanupCacheBlocks(const AdminCleanupCacheBlocksReq &req);
  CoTryTask<GetCacheStatusRsp> getCacheStatus(const GetCacheStatusReq &req);
  CoTryTask<ReportCacheAccessRsp> reportCacheAccess(const ReportCacheAccessReq &req);
  CoTryTask<GetPhase2CacheStatusRsp> getPhase2CacheStatus(const GetPhase2CacheStatusReq &req);
  CoTryTask<CreatePrefetchJobRsp> createPrefetchJob(const CreatePrefetchJobReq &req);
  CoTryTask<GetPrefetchJobRsp> getPrefetchJob(const GetPrefetchJobReq &req);
  CoTryTask<ListPrefetchJobsRsp> listPrefetchJobs(const ListPrefetchJobsReq &req);
  CoTryTask<CancelPrefetchJobRsp> cancelPrefetchJob(const CancelPrefetchJobReq &req);
  CoTryTask<PinDatasetRsp> pinDataset(const PinDatasetReq &req);
  CoTryTask<UnpinDatasetRsp> unpinDataset(const UnpinDatasetReq &req);
  CoTryTask<GetPinStatusRsp> getPinStatus(const GetPinStatusReq &req);

 private:
  Result<Void> checkProtocol(uint32_t version) const;
  Result<Void> checkPhase2Protocol(uint32_t version) const;
  Result<Void> checkPhase3Protocol(uint32_t version) const;
  Result<Void> checkService(const ServiceIdentity &service) const;

  const Config &config_;
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::shared_ptr<storage::client::StorageClient> storageClient_;
  std::shared_ptr<CacheManagerBackend> backend_;
  HintCoalescer hints_;
  std::unique_ptr<CapacityGate> capacityGate_;
  std::unique_ptr<CacheLoader> loader_;
  std::unique_ptr<LoaderScheduler> loaderScheduler_;
  std::unique_ptr<EnsureCached> ensureCached_;
  std::unique_ptr<CacheCleanupWorker> cleanupWorker_;
  std::unique_ptr<ReportCacheBlockInvalid> reportInvalid_;
  std::unique_ptr<AdminCleanupCacheBlocks> adminCleanup_;
  std::unique_ptr<PhysicalTopology> physicalTopology_;
  std::unique_ptr<SpacePoller> spacePoller_;
  std::unique_ptr<EvictionPressureState> evictionPressure_;
  std::unique_ptr<PhysicalPreflight> physicalPreflight_;
  std::unique_ptr<AdmissionPolicy> admissionPolicy_;
  std::unique_ptr<EvictionPolicy> evictionPolicy_;
  std::unique_ptr<EvictionController> evictionController_;
  std::unique_ptr<EvictingWorker> evictingWorker_;
  std::unique_ptr<PermitRecovery> permitRecovery_;
  std::unique_ptr<AccessFlushWorker> accessFlushWorker_;
  Uuid managerEpoch_{Uuid::zero()};
  std::unique_ptr<BackgroundRunner> scheduler_;
  SchedulerStopHook schedulerStopHook_;
  mutable std::mutex mutex_;
  bool running_ = false;
};

}  // namespace hf3fs::cache_manager
