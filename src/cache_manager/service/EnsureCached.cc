#include "cache_manager/service/EnsureCached.h"

#include <algorithm>
#include <limits>

namespace hf3fs::cache_manager {

CoTryTask<EnsureCachedRsp> EnsureCached::run(const EnsureCachedReq &req) {
  auto inode = co_await backend_->stat(req.inode);
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile, "cache hint inode is not an OriginFile");
  const auto &origin = inode->asOriginFile();
  if (origin.superseded || origin.cacheAdmissionDisabled) co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED};
  if (req.beginBlock.toUnderType() > std::numeric_limits<uint32_t>::max() - req.blockCount) {
    co_return makeError(StatusCode::kInvalidArg, "cache hint block range overflow");
  }

  auto blockSize = uint64_t{inode->fileLayout().chunkSize};
  std::vector<meta::CacheBlockRequestBase> items;
  items.reserve(req.blockCount);
  for (uint32_t i = 0; i < req.blockCount; ++i) {
    auto block = cache::CacheBlockIndex{req.beginBlock.toUnderType() + i};
    auto offset = uint64_t{block.toUnderType()} * blockSize;
    if (offset >= inode->fileLength()) break;
    items.push_back({{inode->id.u64(), block}, std::min(blockSize, inode->fileLength() - offset)});
  }
  if (items.empty()) co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED};

  auto enqueued = co_await backend_->enqueue(items);
  CO_RETURN_ON_ERROR(enqueued);
  if (enqueued->results.size() != items.size()) {
    co_return makeError(CacheCode::kInvalidResponse, "invalid enqueue result count");
  }
  bool accepted = false;
  bool attached = false;
  for (size_t i = 0; i < items.size(); ++i) {
    const auto &result = enqueued->results[i];
    if (result.hasError()) {
      if (result.error().code() == CacheCode::kCapacityExceeded) continue;
      co_return makeError(result.error());
    }
    switch (result->state) {
      case cache::CacheBlockState::QUEUED: {
        auto fresh = hints_.enqueue({inode->id, items[i].key.block, items[i].blockLength, req.reason, req.priority});
        accepted = accepted || fresh;
        attached = attached || !fresh;
        break;
      }
      case cache::CacheBlockState::LOADING:
      case cache::CacheBlockState::READY:
        attached = true;
        break;
      case cache::CacheBlockState::CLEANING:
        attached = true;
        if (cleanupWorker_) {
          (void)co_await cleanupWorker_->clean(
              {items[i].key, std::nullopt, std::nullopt, cache::CleanupTerminalState::FAILED});
        }
        break;
      default:
        break;
    }
  }
  if (accepted) co_return EnsureCachedRsp{EnsureCachedStatus::ACCEPTED};
  if (attached) co_return EnsureCachedRsp{EnsureCachedStatus::ATTACHED};
  co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED};
}

}  // namespace hf3fs::cache_manager
