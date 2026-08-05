#include "cache_manager/scheduler/LoaderScheduler.h"

#include <folly/logging/xlog.h>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {

CoTask<void> LoaderScheduler::runOne() {
  auto hints = hints_.popBatch(maxRangeBytes_);
  if (hints.empty()) co_return;
  auto first = hints.front();
  auto result = co_await loader_.loadBatch(hints);
  const auto status = result.hasValue() ? Status::OK : result.error();
  for (const auto &hint : hints) hint.notify(status);
  cache::metrics::setGauge(cache::metrics::Event::MANAGER_QUEUE, hints_.size());
  cache::metrics::recordCount(
      cache::metrics::Event::MANAGER_LOADER_RESULT,
      1,
      {.inode = first.inode.u64(),
       .block = first.block.toUnderType(),
       .reason = result.hasValue() ? "success" : std::string(StatusCode::toString(result.error().code()))});
  XLOGF_IF(WARN,
           result.hasError(),
           "Cache load failed for inode {} block {}: {}",
           first.inode,
           first.block,
           result.error());
}

}  // namespace hf3fs::cache_manager
