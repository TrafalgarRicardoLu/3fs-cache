#pragma once

#include "cache_manager/service/CacheManagerOperator.h"
#include "common/serde/CallContext.h"

namespace hf3fs::cache_manager {

class CacheManagerSerdeService : public serde::ServiceWrapper<CacheManagerSerdeService, CacheManagerSerde> {
 public:
  explicit CacheManagerSerdeService(CacheManagerOperator &operator_)
      : operator_(operator_) {}

  CoTryTask<EnsureCachedRsp> ensureCached(serde::CallContext &, const EnsureCachedReq &req);
  CoTryTask<ReportCacheBlockInvalidRsp> reportCacheBlockInvalid(serde::CallContext &,
                                                                const ReportCacheBlockInvalidReq &req);
  CoTryTask<AdminCleanupCacheBlocksRsp> adminCleanupCacheBlocks(serde::CallContext &,
                                                                const AdminCleanupCacheBlocksReq &req);
  CoTryTask<GetCacheStatusRsp> getCacheStatus(serde::CallContext &, const GetCacheStatusReq &req);
  CoTryTask<ReportCacheAccessRsp> reportCacheAccess(serde::CallContext &, const ReportCacheAccessReq &req);
  CoTryTask<GetPhase2CacheStatusRsp> getPhase2CacheStatus(serde::CallContext &, const GetPhase2CacheStatusReq &req);
  CoTryTask<CreatePrefetchJobRsp> createPrefetchJob(serde::CallContext &, const CreatePrefetchJobReq &req);
  CoTryTask<GetPrefetchJobRsp> getPrefetchJob(serde::CallContext &, const GetPrefetchJobReq &req);
  CoTryTask<ListPrefetchJobsRsp> listPrefetchJobs(serde::CallContext &, const ListPrefetchJobsReq &req);
  CoTryTask<CancelPrefetchJobRsp> cancelPrefetchJob(serde::CallContext &, const CancelPrefetchJobReq &req);
  CoTryTask<PinDatasetRsp> pinDataset(serde::CallContext &, const PinDatasetReq &req);
  CoTryTask<UnpinDatasetRsp> unpinDataset(serde::CallContext &, const UnpinDatasetReq &req);
  CoTryTask<GetPinStatusRsp> getPinStatus(serde::CallContext &, const GetPinStatusReq &req);

 private:
  CacheManagerOperator &operator_;
};

}  // namespace hf3fs::cache_manager
