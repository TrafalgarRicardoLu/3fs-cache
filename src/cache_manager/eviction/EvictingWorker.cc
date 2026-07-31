#include "cache_manager/eviction/EvictingWorker.h"

#include <folly/logging/xlog.h>

namespace hf3fs::cache_manager {

CoTryTask<EvictingRunResult> EvictingWorker::runOnce() {
  if (!backend_ || pageSize_ == 0 || pageSize_ > cache::kMaxPhase2BatchItems)
    co_return makeError(StatusCode::kInvalidConfig, "invalid EVICTING worker configuration");
  EvictingRunResult result;
  std::optional<cache::CacheBlockKey> after;
  bool more = true;
  while (more) {
    auto page = co_await backend_->listEvicting(after, pageSize_);
    CO_RETURN_ON_ERROR(page);
    if (page->items.empty()) {
      if (page->more) co_return makeError(CacheCode::kInvalidResponse, "empty EVICTING page has continuation");
      break;
    }
    for (const auto &identity : page->items) {
      ++result.scanned;
      auto retired = co_await backend_->coordinateRetire(identity);
      if (retired.hasError()) {
        ++result.failed;
        XLOGF(WARN, "Cache retire operation {} failed: {}", identity.retireOperationId, retired.error());
      } else if (*retired) {
        ++result.completed;
      } else {
        ++result.pending;
      }
    }
    after = page->items.back().key;
    more = page->more;
  }
  co_return result;
}

}  // namespace hf3fs::cache_manager
