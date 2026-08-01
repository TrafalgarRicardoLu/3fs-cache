#pragma once

#include "common/utils/Coroutine.h"
#include "fbs/cache_manager/Service.h"
#include "fbs/mgmtd/RoutingInfo.h"

namespace hf3fs::client::cli {
class Dispatcher;
bool cachePhase2InventoryHealthy(size_t metadataRecords, const cache_manager::GetPhase2CacheStatusRsp &status);
bool cachePhase2RoutingHealthy(const flat::RoutingInfo &routing, const cache_manager::GetPhase2CacheStatusRsp &status);
CoTryTask<void> registerCachePhase2RolloutHandler(Dispatcher &dispatcher);
}  // namespace hf3fs::client::cli
