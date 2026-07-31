#include "cache_manager/eviction/EvictionCandidateSource.h"

#include <folly/logging/xlog.h>
#include <limits>

namespace hf3fs::cache_manager {

Result<EvictionCandidate> EvictionCandidateSource::convert(const meta::ReadyCacheBlockStatus &status) const {
  RETURN_ON_ERROR(status.valid());
  EvictionCandidate candidate;
  candidate.key = status.key;
  candidate.ready = status.ready;
  candidate.chainId = status.chainId;
  candidate.logicalBytes = status.chargedBytes;
  candidate.readyAt = status.readyAt;
  candidate.lastAccessAt = status.lastAccessAt;
  for (const auto targetId : status.placement.expectedReplicaTargets) {
    auto footprint = status.committedPermit.footprintByTarget.find(targetId);
    if (footprint == status.committedPermit.footprintByTarget.end() || footprint->second == 0) {
      return makeError(CacheCode::kPlacementMismatch, "READY permit footprint does not cover persisted placement");
    }
    auto disk = topology_.persistedDisk(targetId);
    if (disk.hasError()) {
      XLOGF(ERR,
            "Cannot resolve persisted cache replica target {} for inode {} block {}: {}",
            targetId,
            status.key.inode,
            status.key.block,
            disk.error());
      return makeError(CacheCode::kUnavailable, "persisted cache replica disk cannot be resolved");
    }
    auto &bytes = candidate.physicalFootprintByDisk[*disk];
    bytes += std::min(footprint->second, std::numeric_limits<uint64_t>::max() - bytes);
  }
  return candidate;
}

CoTryTask<EvictionCandidatePage> EvictionCandidateSource::next(
    std::optional<cache::CacheBlockKey> after,
    const std::set<storage::PhysicalDiskId> &pressuredDisks) {
  if (!backend_ || pageSize_ == 0 || pageSize_ > cache::kMaxPhase2BatchItems || pressuredDisks.empty()) {
    co_return makeError(StatusCode::kInvalidArg, "invalid eviction candidate source request");
  }
  auto response = co_await backend_->listReadyCacheBlocks(after, pageSize_);
  CO_RETURN_ON_ERROR(response);
  if (response->more && response->items.empty()) {
    co_return makeError(CacheCode::kInvalidResponse, "READY cache block page cannot advance");
  }

  EvictionCandidatePage page;
  page.more = response->more;
  if (page.more) page.nextAfter = response->items.back().key;
  page.candidates.reserve(response->items.size());
  for (const auto &status : response->items) {
    auto candidate = convert(status);
    CO_RETURN_ON_ERROR(candidate);
    bool touchesPressure = false;
    for (const auto &[disk, _] : candidate->physicalFootprintByDisk) {
      if (pressuredDisks.contains(disk)) {
        touchesPressure = true;
        break;
      }
    }
    if (touchesPressure) page.candidates.push_back(std::move(*candidate));
  }
  co_return page;
}

}  // namespace hf3fs::cache_manager
