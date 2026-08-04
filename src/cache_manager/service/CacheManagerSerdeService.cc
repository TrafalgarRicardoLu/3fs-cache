#include "cache_manager/service/CacheManagerSerdeService.h"

namespace hf3fs::cache_manager {

CoTryTask<EnsureCachedRsp> CacheManagerSerdeService::ensureCached(serde::CallContext &, const EnsureCachedReq &req) {
  co_return co_await operator_.ensureCached(req);
}

CoTryTask<ReportCacheBlockInvalidRsp> CacheManagerSerdeService::reportCacheBlockInvalid(
    serde::CallContext &,
    const ReportCacheBlockInvalidReq &req) {
  co_return co_await operator_.reportCacheBlockInvalid(req);
}

CoTryTask<AdminCleanupCacheBlocksRsp> CacheManagerSerdeService::adminCleanupCacheBlocks(
    serde::CallContext &,
    const AdminCleanupCacheBlocksReq &req) {
  co_return co_await operator_.adminCleanupCacheBlocks(req);
}

CoTryTask<GetCacheStatusRsp> CacheManagerSerdeService::getCacheStatus(serde::CallContext &,
                                                                      const GetCacheStatusReq &req) {
  co_return co_await operator_.getCacheStatus(req);
}

CoTryTask<ReportCacheAccessRsp> CacheManagerSerdeService::reportCacheAccess(serde::CallContext &,
                                                                            const ReportCacheAccessReq &req) {
  co_return co_await operator_.reportCacheAccess(req);
}

CoTryTask<GetPhase2CacheStatusRsp> CacheManagerSerdeService::getPhase2CacheStatus(serde::CallContext &,
                                                                                  const GetPhase2CacheStatusReq &req) {
  co_return co_await operator_.getPhase2CacheStatus(req);
}

#define FORWARD_PHASE3_METHOD(NAME, REQ, RSP)                                           \
  CoTryTask<RSP> CacheManagerSerdeService::NAME(serde::CallContext &, const REQ &req) { \
    co_return co_await operator_.NAME(req);                                             \
  }
FORWARD_PHASE3_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
FORWARD_PHASE3_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
FORWARD_PHASE3_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
FORWARD_PHASE3_METHOD(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
FORWARD_PHASE3_METHOD(pinDataset, PinDatasetReq, PinDatasetRsp);
FORWARD_PHASE3_METHOD(unpinDataset, UnpinDatasetReq, UnpinDatasetRsp);
FORWARD_PHASE3_METHOD(getPinStatus, GetPinStatusReq, GetPinStatusRsp);
#undef FORWARD_PHASE3_METHOD

}  // namespace hf3fs::cache_manager
