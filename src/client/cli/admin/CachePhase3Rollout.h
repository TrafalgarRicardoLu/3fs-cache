#pragma once

#include "common/utils/Coroutine.h"
#include "fbs/cache_manager/Service.h"

namespace hf3fs::client::cli {
class Dispatcher;
bool cachePhase3InventoryHealthy(const cache_manager::GetCacheStatusRsp &status);
CoTryTask<void> registerCachePhase3RolloutHandler(Dispatcher &dispatcher);
}  // namespace hf3fs::client::cli
