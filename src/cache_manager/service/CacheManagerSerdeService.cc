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

}  // namespace hf3fs::cache_manager
