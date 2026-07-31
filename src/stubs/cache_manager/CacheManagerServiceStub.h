#pragma once

#include "stubs/cache_manager/ICacheManagerServiceStub.h"

namespace hf3fs::cache_manager {

template <typename Context>
class CacheManagerServiceStub : public ICacheManagerServiceStub {
 public:
  explicit CacheManagerServiceStub(Context context)
      : context_(std::move(context)) {}

#define CACHE_MANAGER_STUB_METHOD(NAME, REQ, RSP) \
  CoTryTask<RSP> NAME(const REQ &req, const net::UserRequestOptions &options = {}) override
  CACHE_MANAGER_STUB_METHOD(ensureCached, EnsureCachedReq, EnsureCachedRsp);
  CACHE_MANAGER_STUB_METHOD(reportCacheBlockInvalid, ReportCacheBlockInvalidReq, ReportCacheBlockInvalidRsp);
  CACHE_MANAGER_STUB_METHOD(adminCleanupCacheBlocks, AdminCleanupCacheBlocksReq, AdminCleanupCacheBlocksRsp);
  CACHE_MANAGER_STUB_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  CACHE_MANAGER_STUB_METHOD(reportCacheAccess, ReportCacheAccessReq, ReportCacheAccessRsp);
  CACHE_MANAGER_STUB_METHOD(getPhase2CacheStatus, GetPhase2CacheStatusReq, GetPhase2CacheStatusRsp);
#undef CACHE_MANAGER_STUB_METHOD

 private:
  Context context_;
};

}  // namespace hf3fs::cache_manager
