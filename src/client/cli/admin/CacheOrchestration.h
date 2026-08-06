#pragma once

#include "common/utils/Coroutine.h"

namespace hf3fs::client::cli {
class Dispatcher;
uint32_t cacheReadyBps(uint64_t readyBytes, uint64_t plannedBytes);
CoTryTask<void> registerCachePrefetchHandler(Dispatcher &dispatcher);
CoTryTask<void> registerCachePinHandler(Dispatcher &dispatcher);
}  // namespace hf3fs::client::cli
