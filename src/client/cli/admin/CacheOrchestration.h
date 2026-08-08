#pragma once

#include "client/cli/common/Dispatcher.h"
#include "fbs/cache/Common.h"
#include "fbs/cache_manager/Service.h"

namespace hf3fs::client::cli {
class Dispatcher;
uint32_t cacheReadyBps(uint64_t readyBytes, uint64_t plannedBytes);
Dispatcher::OutputTable cacheReconcileTable(const cache::ReconcileProgress &progress, bool dryRun);
Dispatcher::OutputRow cacheUploadJobRow(const cache::UploadJobRecord &job, bool more);
CoTryTask<void> registerCachePrefetchHandler(Dispatcher &dispatcher);
CoTryTask<void> registerCachePinHandler(Dispatcher &dispatcher);
CoTryTask<void> registerCacheReconcileHandler(Dispatcher &dispatcher);
CoTryTask<void> registerCacheUploadHandler(Dispatcher &dispatcher);
}  // namespace hf3fs::client::cli
