#include "cache_manager/cleanup/CacheCleanupWorker.h"

#include <folly/ScopeGuard.h>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {

CoTryTask<void> CacheCleanupWorker::clean(meta::BeginCleanCacheBlockItem item) {
  auto outcome = std::string{"failure"};
  auto metric = folly::makeGuard([&] {
    cache::metrics::recordCount(
        cache::metrics::Event::MANAGER_CLEANUP_RESULT,
        1,
        {.inode = item.key.inode, .block = item.key.block.toUnderType(), .reason = std::move(outcome)});
  });
  auto begun = co_await backend_->beginClean(item);
  CO_RETURN_ON_ERROR(begun);
  auto inode = co_await backend_->stat(meta::InodeId{item.key.inode});
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile);

  std::optional<cache::CacheGeneration> retired;
  for (size_t attempt = 0; begun->deleteGeneration != cache::CacheGeneration{} && attempt < 8; ++attempt) {
    auto result =
        begun->placement
            ? co_await backend_->retirePlaced(*inode, item.key.block, begun->deleteGeneration, *begun->placement)
            : co_await backend_->retire(*inode, item.key.block, begun->deleteGeneration);
    if (result.hasValue()) {
      if (!result->retired || result->cacheGeneration < begun->deleteGeneration || result->length != 0) {
        co_return makeError(CacheCode::kInvalidResponse, "Storage did not confirm a cache tombstone");
      }
      retired = result->cacheGeneration;
      break;
    }
    if (result.error().code() != CacheCode::kGenerationAdvanced) CO_RETURN_ERROR(result);
    auto current = begun->placement ? co_await backend_->queryPlaced(*inode, item.key.block, *begun->placement)
                                    : co_await backend_->query(*inode, item.key.block);
    CO_RETURN_ON_ERROR(current);
    if (current->cacheGeneration <= begun->deleteGeneration) {
      co_return makeError(CacheCode::kInvalidResponse, "Storage generation advanced without a newer generation");
    }
    item.observedGeneration = current->cacheGeneration;
    auto updated = co_await backend_->beginClean(item);
    CO_RETURN_ON_ERROR(updated);
    if (updated->cleanupEpoch != begun->cleanupEpoch || updated->deleteGeneration < current->cacheGeneration) {
      co_return makeError(CacheCode::kStateConflict, "cache cleanup fence changed");
    }
    begun = std::move(updated);
  }
  if (begun->deleteGeneration != cache::CacheGeneration{} && !retired) {
    co_return makeError(CacheCode::kUnavailable, "cache cleanup generation kept advancing");
  }
  auto finished = co_await backend_->finishClean({item.key, begun->cleanupEpoch, retired});
  outcome = finished.hasValue() ? "success" : StatusCode::toString(finished.error().code());
  co_return finished;
}

}  // namespace hf3fs::cache_manager
