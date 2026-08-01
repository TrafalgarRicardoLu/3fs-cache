#pragma once

#include <functional>

#include "cache/metrics/CacheMetrics.h"
#include "cache_manager/capacity/PhysicalTopology.h"
#include "common/utils/Coroutine.h"

namespace hf3fs::cache_manager {

class SpacePoller {
 public:
  using Query = std::function<CoTryTask<storage::QueryCacheSpaceRsp>(const storage::QueryCacheSpaceReq &)>;
  using Clock = std::function<SteadyTime()>;

  explicit SpacePoller(PhysicalTopology &topology, Query query = {}, Clock clock = SteadyClock::now)
      : topology_(topology),
        query_(std::move(query)),
        clock_(std::move(clock)) {}

  CoTryTask<Void> poll(flat::NodeId nodeId,
                       std::vector<flat::TargetId> targetIds,
                       std::vector<storage::CacheFootprintQuery> footprints = {}) {
    if (!query_) co_return makeError(StatusCode::kInvalidConfig, "cache space query is unavailable");
    storage::QueryCacheSpaceReq request;
    request.targetIds = std::move(targetIds);
    request.footprints = std::move(footprints);
    request.cacheProtocolVersion = cache::kCacheProtocolVersion;
    CO_RETURN_ON_ERROR(request.valid());
    auto started = clock_();
    auto response = co_await query_(request);
    auto received = clock_();
    if (response.hasError()) {
      cache::metrics::recordCount(cache::metrics::Event::MANAGER_SPACE_QUERY, 1, {.reason = "rpc_error"});
      co_return makeError(response.error());
    }
    auto updated = topology_.updateSpace(nodeId, *response, started, received);
    if (updated.hasError()) {
      cache::metrics::recordCount(cache::metrics::Event::MANAGER_SPACE_QUERY, 1, {.reason = "invalid_snapshot"});
      co_return makeError(updated.error());
    }
    cache::metrics::recordCount(cache::metrics::Event::MANAGER_SPACE_QUERY, 1, {.reason = "success"});
    for (const auto &item : response->results) {
      if (item.hasError()) continue;
      cache::metrics::Tags tags{.diskId = item->physicalDiskId.uuid.toHexString()};
      cache::metrics::setGauge(cache::metrics::Event::MANAGER_PHYSICAL_USED_BYTES, item->physicalUsedBytes, tags);
      cache::metrics::setGauge(cache::metrics::Event::MANAGER_ALLOCATABLE_BYTES, item->allocatableBytes, tags);
      cache::metrics::setGauge(cache::metrics::Event::MANAGER_RESERVED_BYTES, item->reservedBytes, tags);
      auto age = received >= started ? std::chrono::duration_cast<std::chrono::nanoseconds>(received - started).count()
                                     : int64_t{0};
      cache::metrics::setGauge(cache::metrics::Event::MANAGER_SNAPSHOT_AGE_NS, age, tags);
    }
    co_return Void{};
  }

  Result<Void> observe(flat::NodeId nodeId,
                       const storage::QueryCacheSpaceRsp &response,
                       SteadyTime requestStarted,
                       SteadyTime receivedAt) {
    return topology_.updateSpace(nodeId, response, requestStarted, receivedAt);
  }

 private:
  PhysicalTopology &topology_;
  Query query_;
  Clock clock_;
};

}  // namespace hf3fs::cache_manager
