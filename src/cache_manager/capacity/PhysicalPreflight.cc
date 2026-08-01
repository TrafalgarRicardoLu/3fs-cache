#include "cache_manager/capacity/PhysicalPreflight.h"

#include <limits>
#include <set>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {
namespace {

bool addOverflows(uint64_t lhs, uint64_t rhs) { return rhs > std::numeric_limits<uint64_t>::max() - lhs; }

}  // namespace

PhysicalPreflight::Reservation::Reservation(std::shared_ptr<SharedState> state,
                                            std::map<storage::PhysicalDiskId, uint64_t> bytesByDisk)
    : state_(std::move(state)),
      bytesByDisk_(std::move(bytesByDisk)) {}

PhysicalPreflight::Reservation::Reservation(Reservation &&other) noexcept
    : state_(std::move(other.state_)),
      bytesByDisk_(std::move(other.bytesByDisk_)) {}

PhysicalPreflight::Reservation &PhysicalPreflight::Reservation::operator=(Reservation &&other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
    bytesByDisk_ = std::move(other.bytesByDisk_);
  }
  return *this;
}

PhysicalPreflight::Reservation::~Reservation() { release(); }

void PhysicalPreflight::Reservation::release() {
  if (!state_) return;
  std::scoped_lock lock(state_->mutex);
  for (const auto &[diskId, bytes] : bytesByDisk_) {
    auto found = state_->reserved.find(diskId);
    if (found == state_->reserved.end() || found->second < bytes) continue;
    found->second -= bytes;
    if (found->second == 0) state_->reserved.erase(found);
  }
  state_.reset();
  bytesByDisk_.clear();
}

PhysicalPreflight::PhysicalPreflight(const PhysicalTopology &topology,
                                     Duration maxAge,
                                     double highWatermark,
                                     const EvictionPressureState *pressure)
    : topology_(topology),
      maxAge_(maxAge),
      highWatermark_(highWatermark),
      pressure_(pressure),
      state_(std::make_shared<SharedState>()) {}

Result<PhysicalPreflight::Reservation> PhysicalPreflight::tryReserve(flat::ChainId chainId,
                                                                     const storage::FootprintByTarget &footprints,
                                                                     SteadyTime now) {
  auto resolved = topology_.resolve(chainId, now, maxAge_, highWatermark_);
  if (resolved.hasError()) {
    cache::metrics::recordCount(cache::metrics::Event::MANAGER_PREFLIGHT_RESULT, 1, {.reason = "topology_rejected"});
    return makeError(resolved.error());
  }
  if (footprints.size() != resolved->replicas.size()) {
    return makeError(CacheCode::kPlacementMismatch, "cache footprint replica set is incomplete");
  }
  std::map<storage::PhysicalDiskId, uint64_t> byDisk;
  std::set<flat::TargetId> replicas;
  for (const auto &replica : resolved->replicas) {
    replicas.insert(replica.targetId);
    auto footprint = footprints.find(replica.targetId);
    if (footprint == footprints.end() || footprint->second == 0 ||
        addOverflows(byDisk[replica.diskId], footprint->second)) {
      return makeError(CacheCode::kPlacementMismatch, "invalid cache replica footprint");
    }
    byDisk[replica.diskId] += footprint->second;
  }
  for (const auto &[targetId, _] : footprints)
    if (!replicas.contains(targetId))
      return makeError(CacheCode::kPlacementMismatch, "unknown cache replica footprint");

  std::scoped_lock lock(state_->mutex);
  for (const auto &[diskId, requested] : byDisk) {
    if (pressure_ && pressure_->contains(diskId)) {
      cache::metrics::recordCount(cache::metrics::Event::MANAGER_PREFLIGHT_RESULT,
                                  1,
                                  {.diskId = diskId.uuid.toHexString(), .reason = "pressured"});
      return makeError(CacheCode::kCapacityExceeded, "cache admission paused for pressured disk");
    }
    const auto &space = resolved->disks.at(diskId).space;
    auto localReservation = state_->reserved.find(diskId);
    auto local = localReservation == state_->reserved.end() ? 0 : localReservation->second;
    if (addOverflows(space.physicalUsedBytes, space.reservedBytes) ||
        addOverflows(space.physicalUsedBytes + space.reservedBytes, local) ||
        addOverflows(space.physicalUsedBytes + space.reservedBytes + local, requested)) {
      return makeError(CacheCode::kCapacityExceeded, "cache preflight usage overflow");
    }
    auto projected = space.physicalUsedBytes + space.reservedBytes + local + requested;
    auto projectedRatio = static_cast<double>(projected) / static_cast<double>(space.capacityBytes);
    if (projectedRatio >= highWatermark_ || addOverflows(local, requested) ||
        local + requested > space.allocatableBytes) {
      cache::metrics::recordCount(cache::metrics::Event::MANAGER_PREFLIGHT_RESULT,
                                  1,
                                  {.diskId = diskId.uuid.toHexString(), .reason = "capacity"});
      return makeError(CacheCode::kCapacityExceeded, "cache physical preflight rejected");
    }
  }
  for (const auto &[diskId, bytes] : byDisk) state_->reserved[diskId] += bytes;
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_PREFLIGHT_RESULT, 1, {.reason = "reserved"});
  return Reservation(state_, std::move(byDisk));
}

uint64_t PhysicalPreflight::locallyReserved(storage::PhysicalDiskId diskId) const {
  std::scoped_lock lock(state_->mutex);
  auto found = state_->reserved.find(diskId);
  return found == state_->reserved.end() ? 0 : found->second;
}

}  // namespace hf3fs::cache_manager
