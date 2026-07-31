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

 private:
  CacheManagerOperator &operator_;
};

}  // namespace hf3fs::cache_manager
