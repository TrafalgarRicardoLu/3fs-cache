#include "cache_manager/service/AdminCleanupCacheBlocks.h"

namespace hf3fs::cache_manager {

CoTryTask<AdminCleanupCacheBlocksRsp> AdminCleanupCacheBlocks::run(const AdminCleanupCacheBlocksReq &req) {
  CO_RETURN_ON_ERROR(co_await backend_->authorizeAdmin(req.user, req.inode));
  AdminCleanupCacheBlocksRsp response;
  response.results.reserve(req.blockCount);
  for (uint32_t i = 0; i < req.blockCount; ++i) {
    auto block = cache::CacheBlockIndex{req.beginBlock.toUnderType() + i};
    meta::BeginCleanCacheBlockItem item{{req.inode.u64(), block},
                                        req.expectedReady,
                                        std::nullopt,
                                        cache::CleanupTerminalState::NONE};
    auto result = co_await worker_.clean(std::move(item));
    auto status = CleanupBlockStatus::CLEANED;
    if (result.hasError()) {
      status = result.error().code() == CacheCode::kNotFound        ? CleanupBlockStatus::NOT_FOUND
               : result.error().code() == CacheCode::kStateConflict ? CleanupBlockStatus::CONFLICT
                                                                    : CleanupBlockStatus::RETRYING;
    }
    response.results.emplace_back(AdminCleanupBlockResult{block, status});
  }
  co_return response;
}

}  // namespace hf3fs::cache_manager
