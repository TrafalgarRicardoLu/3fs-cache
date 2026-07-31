#include "stubs/cache_manager/CacheManagerServiceStub.h"

#include "common/serde/ClientMockContext.h"

namespace hf3fs::cache_manager {

#define CACHE_MANAGER_STUB_METHOD(NAME, REQ, RSP)                                                                 \
  template <typename Context>                                                                                     \
  CoTryTask<RSP> CacheManagerServiceStub<Context>::NAME(const REQ &req, const net::UserRequestOptions &options) { \
    co_return co_await CacheManagerSerde<>::NAME<Context>(context_, req, &options);                               \
  }
CACHE_MANAGER_STUB_METHOD(ensureCached, EnsureCachedReq, EnsureCachedRsp);
CACHE_MANAGER_STUB_METHOD(reportCacheBlockInvalid, ReportCacheBlockInvalidReq, ReportCacheBlockInvalidRsp);
CACHE_MANAGER_STUB_METHOD(adminCleanupCacheBlocks, AdminCleanupCacheBlocksReq, AdminCleanupCacheBlocksRsp);
CACHE_MANAGER_STUB_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
CACHE_MANAGER_STUB_METHOD(reportCacheAccess, ReportCacheAccessReq, ReportCacheAccessRsp);
CACHE_MANAGER_STUB_METHOD(getPhase2CacheStatus, GetPhase2CacheStatusReq, GetPhase2CacheStatusRsp);
#undef CACHE_MANAGER_STUB_METHOD

template class CacheManagerServiceStub<serde::ClientContext>;
template class CacheManagerServiceStub<serde::ClientMockContext>;

}  // namespace hf3fs::cache_manager
