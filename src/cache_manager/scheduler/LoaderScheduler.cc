#include "cache_manager/scheduler/LoaderScheduler.h"

#include <folly/logging/xlog.h>

namespace hf3fs::cache_manager {

CoTask<void> LoaderScheduler::runOne() {
  auto hints = hints_.popBatch(maxRangeBytes_);
  if (hints.empty()) co_return;
  auto first = hints.front();
  auto result = co_await loader_.loadBatch(std::move(hints));
  XLOGF_IF(WARN,
           result.hasError(),
           "Cache load failed for inode {} block {}: {}",
           first.inode,
           first.block,
           result.error());
}

}  // namespace hf3fs::cache_manager
