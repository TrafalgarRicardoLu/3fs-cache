#pragma once

#include "common/serde/ClientContext.h"
#include "fbs/cache_manager/Service.h"

namespace hf3fs::cache_manager {

class ICacheManagerServiceStub {
 public:
  virtual ~ICacheManagerServiceStub() = default;

#define CACHE_MANAGER_STUB_METHOD(NAME, REQ, RSP) \
  virtual CoTryTask<RSP> NAME(const REQ &, const net::UserRequestOptions & = {}) = 0
  CACHE_MANAGER_STUB_METHOD(ensureCached, EnsureCachedReq, EnsureCachedRsp);
  CACHE_MANAGER_STUB_METHOD(reportCacheBlockInvalid, ReportCacheBlockInvalidReq, ReportCacheBlockInvalidRsp);
  CACHE_MANAGER_STUB_METHOD(adminCleanupCacheBlocks, AdminCleanupCacheBlocksReq, AdminCleanupCacheBlocksRsp);
  CACHE_MANAGER_STUB_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  CACHE_MANAGER_STUB_METHOD(reportCacheAccess, ReportCacheAccessReq, ReportCacheAccessRsp);
  CACHE_MANAGER_STUB_METHOD(getPhase2CacheStatus, GetPhase2CacheStatusReq, GetPhase2CacheStatusRsp);
  CACHE_MANAGER_STUB_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  CACHE_MANAGER_STUB_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  CACHE_MANAGER_STUB_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  CACHE_MANAGER_STUB_METHOD(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  CACHE_MANAGER_STUB_METHOD(pinDataset, PinDatasetReq, PinDatasetRsp);
  CACHE_MANAGER_STUB_METHOD(unpinDataset, UnpinDatasetReq, UnpinDatasetRsp);
  CACHE_MANAGER_STUB_METHOD(getPinStatus, GetPinStatusReq, GetPinStatusRsp);
#undef CACHE_MANAGER_STUB_METHOD
};

}  // namespace hf3fs::cache_manager
