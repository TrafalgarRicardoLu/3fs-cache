#include "cache_manager/capacity/PhysicalTopology.h"

#include <cmath>
#include <iterator>
#include <limits>

namespace hf3fs::cache_manager {

void PhysicalTopology::updateRouting(std::shared_ptr<client::RoutingInfo> routing) {
  std::scoped_lock lock(mutex_);
  routing_ = std::move(routing);
}

Result<Void> PhysicalTopology::updateSpace(flat::NodeId nodeId,
                                           const storage::QueryCacheSpaceRsp &response,
                                           SteadyTime requestStarted,
                                           SteadyTime receivedAt) {
  if (nodeId == flat::NodeId{} || receivedAt < requestStarted) {
    return makeError(StatusCode::kInvalidArg, "invalid cache space observation");
  }
  std::vector<DiskSpaceSnapshot> snapshots;
  std::map<flat::TargetId, storage::PhysicalDiskId> observedTargets;
  for (const auto &result : response.results) {
    if (result.hasError()) continue;
    const auto &space = *result;
    RETURN_ON_ERROR(space.physicalDiskId.valid());
    if (space.role != storage::StorageRole::CACHE_ONLY || space.targets.empty() || space.capacityBytes == 0 ||
        !std::isfinite(space.enforcedHighWatermark) || space.enforcedHighWatermark <= 0.0 ||
        space.enforcedHighWatermark > 1.0) {
      return makeError(CacheCode::kRoleMismatch, "invalid cache-only disk space response");
    }
    snapshots.push_back({nodeId, space, requestStarted, receivedAt});
    for (auto targetId : space.targets) {
      if (targetId == flat::TargetId{}) return makeError(StatusCode::kInvalidArg, "invalid cache target");
      auto [found, inserted] = observedTargets.emplace(targetId, space.physicalDiskId);
      if (!inserted && found->second != space.physicalDiskId) {
        return makeError(CacheCode::kPlacementMismatch, "cache target maps to multiple physical disks");
      }
    }
  }

  std::scoped_lock lock(mutex_);
  for (const auto &snapshot : snapshots) {
    const auto &space = snapshot.space;
    for (auto it = targetToDisk_.begin(); it != targetToDisk_.end();) {
      it = it->second == space.physicalDiskId ? targetToDisk_.erase(it) : std::next(it);
    }
    disks_[space.physicalDiskId] = snapshot;
  }
  for (const auto &[targetId, diskId] : observedTargets) targetToDisk_[targetId] = diskId;
  return Void{};
}

Result<ResolvedPhysicalChain> PhysicalTopology::resolve(flat::ChainId chainId,
                                                        SteadyTime now,
                                                        Duration maxAge,
                                                        double expectedHighWatermark) const {
  std::scoped_lock lock(mutex_);
  if (!routing_) return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  auto chain = routing_->getChain(chainId);
  if (!chain || chain->targets.empty()) return makeError(CacheCode::kUnavailable, "cache chain is unavailable");

  ResolvedPhysicalChain resolved;
  resolved.versionedChain = {chainId, chain->chainVersion};
  for (const auto &replica : chain->targets) {
    if (replica.publicState != flat::PublicTargetState::SERVING) {
      return makeError(CacheCode::kUnavailable, "cache replica is not serving");
    }
    auto target = routing_->getTarget(replica.targetId);
    if (!target) return makeError(CacheCode::kUnavailable, "cache target is missing from routing");
    if (!target->nodeId.has_value()) return makeError(CacheCode::kUnavailable, "cache target node is missing");
    if (target->storageRole != storage::StorageRole::CACHE_ONLY)
      return makeError(CacheCode::kRoleMismatch, "cache target role is not cache-only");
    auto diskValid = target->physicalDiskId.valid();
    if (diskValid.hasError()) return makeError(CacheCode::kUnavailable, "cache target disk identity is missing");
    auto diskId = targetToDisk_.find(replica.targetId);
    auto snapshot = disks_.find(target->physicalDiskId);
    if (diskId == targetToDisk_.end() || diskId->second != target->physicalDiskId || snapshot == disks_.end() ||
        snapshot->second.nodeId != *target->nodeId) {
      return makeError(CacheCode::kUnavailable, "cache disk space snapshot is missing");
    }
    const auto &observed = snapshot->second;
    if (observed.receivedAt < observed.requestStarted || now < observed.receivedAt ||
        observed.receivedAt - observed.requestStarted > maxAge || now - observed.receivedAt > maxAge) {
      return makeError(CacheCode::kUnavailable, "cache disk space snapshot is stale");
    }
    if (std::abs(observed.space.enforcedHighWatermark - expectedHighWatermark) >
        std::numeric_limits<double>::epsilon()) {
      return makeError(CacheCode::kStateConflict, "cache high watermark differs from Storage enforcement");
    }
    resolved.replicas.push_back({replica.targetId, *target->nodeId, target->physicalDiskId});
    resolved.disks.emplace(target->physicalDiskId, observed);
  }
  return resolved;
}

Result<std::map<storage::PhysicalDiskId, DiskSpaceSnapshot>>
PhysicalTopology::freshDiskSnapshots(SteadyTime now, Duration maxAge, double expectedHighWatermark) const {
  if (maxAge <= 0_ns || !std::isfinite(expectedHighWatermark) || expectedHighWatermark <= 0.0 ||
      expectedHighWatermark >= 1.0) {
    return makeError(StatusCode::kInvalidArg, "invalid cache disk snapshot request");
  }
  std::scoped_lock lock(mutex_);
  if (disks_.empty()) return makeError(CacheCode::kUnavailable, "cache disk space snapshots are unavailable");
  for (const auto &[_, snapshot] : disks_) {
    if (snapshot.receivedAt < snapshot.requestStarted || now < snapshot.receivedAt ||
        snapshot.receivedAt - snapshot.requestStarted > maxAge || now - snapshot.receivedAt > maxAge) {
      return makeError(CacheCode::kUnavailable, "cache disk space snapshot is stale");
    }
    if (std::abs(snapshot.space.enforcedHighWatermark - expectedHighWatermark) >
        std::numeric_limits<double>::epsilon()) {
      return makeError(CacheCode::kStateConflict, "cache high watermark differs from Storage enforcement");
    }
  }
  return disks_;
}

Result<storage::PhysicalDiskId> PhysicalTopology::persistedDisk(flat::TargetId targetId) const {
  if (targetId == flat::TargetId{}) return makeError(StatusCode::kInvalidArg, "invalid cache target");
  std::scoped_lock lock(mutex_);
  auto target = targetToDisk_.find(targetId);
  if (target == targetToDisk_.end() || !disks_.contains(target->second)) {
    return makeError(CacheCode::kUnavailable, "persisted cache replica disk is unknown");
  }
  return target->second;
}

}  // namespace hf3fs::cache_manager
