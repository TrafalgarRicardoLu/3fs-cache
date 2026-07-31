#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "client/mgmtd/RoutingInfo.h"
#include "common/utils/Duration.h"
#include "common/utils/UtcTime.h"
#include "fbs/storage/Cache.h"

namespace hf3fs::cache_manager {

struct DiskSpaceSnapshot {
  flat::NodeId nodeId{};
  storage::CacheSpaceInfo space;
  SteadyTime requestStarted{};
  SteadyTime receivedAt{};
};

struct ReplicaPhysicalLocation {
  flat::TargetId targetId{};
  flat::NodeId nodeId{};
  storage::PhysicalDiskId diskId;
};

struct ResolvedPhysicalChain {
  storage::VersionedChainId versionedChain;
  std::vector<ReplicaPhysicalLocation> replicas;
  std::map<storage::PhysicalDiskId, DiskSpaceSnapshot> disks;
};

class PhysicalTopology {
 public:
  void updateRouting(std::shared_ptr<client::RoutingInfo> routing);
  Result<Void> updateSpace(flat::NodeId nodeId,
                           const storage::QueryCacheSpaceRsp &response,
                           SteadyTime requestStarted,
                           SteadyTime receivedAt);
  Result<ResolvedPhysicalChain> resolve(flat::ChainId chainId,
                                        SteadyTime now,
                                        Duration maxAge,
                                        double expectedHighWatermark) const;

 private:
  mutable std::mutex mutex_;
  std::shared_ptr<client::RoutingInfo> routing_;
  std::map<storage::PhysicalDiskId, DiskSpaceSnapshot> disks_;
  std::map<flat::TargetId, storage::PhysicalDiskId> targetToDisk_;
};

}  // namespace hf3fs::cache_manager
