#pragma once

#include <functional>

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
    CO_RETURN_ON_ERROR(response);
    co_return topology_.updateSpace(nodeId, *response, started, received);
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
