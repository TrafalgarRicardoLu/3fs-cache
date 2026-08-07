#pragma once

#include "common/utils/Coroutine.h"
#include "fbs/cache_manager/Service.h"

namespace hf3fs::client::cli {
class Dispatcher;
bool cachePhase4EnableHealthy(const cache_manager::GetCacheStatusRsp &status);
bool cachePhase4DrainHealthy(const cache_manager::GetCacheStatusRsp &status);
bool cachePhase4DrainTimedOut(uint64_t elapsedMs, uint64_t timeoutMs);
CoTryTask<void> registerCachePhase4RolloutHandler(Dispatcher &dispatcher);
}  // namespace hf3fs::client::cli
