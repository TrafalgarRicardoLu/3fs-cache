#include "cache_manager/service/ReportCacheBlockInvalid.h"

namespace hf3fs::cache_manager {

CoTryTask<ReportCacheBlockInvalidRsp> ReportCacheBlockInvalid::run(const ReportCacheBlockInvalidReq &req) {
  CO_RETURN_ON_ERROR(co_await backend_->validateReport(req));
  meta::BeginCleanCacheBlockItem item{{req.inode.u64(), req.block},
                                      req.expectedReady,
                                      req.observedGeneration,
                                      cache::CleanupTerminalState::REENQUEUE};
  auto result = co_await worker_.clean(std::move(item));
  if (result.hasError() && result.error().code() == CacheCode::kStateConflict) {
    co_return ReportCacheBlockInvalidRsp{false};
  }
  CO_RETURN_ON_ERROR(result);
  co_return ReportCacheBlockInvalidRsp{true};
}

}  // namespace hf3fs::cache_manager
