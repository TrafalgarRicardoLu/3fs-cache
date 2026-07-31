#include <limits>
#include <set>

#include "cache_manager/eviction/EvictionPolicy.h"
#include "cache_manager/eviction/LRUEvictionPolicy.h"

namespace hf3fs::cache_manager {

bool protectedFromEviction(const EvictionCandidate &candidate, const EvictionContext &context) {
  auto effective = std::max(candidate.readyAt, candidate.lastAccessAt);
  return effective > context.now || context.now - effective < context.protectionPeriod;
}

Result<std::unique_ptr<EvictionPolicy>> createEvictionPolicy(std::string_view name) {
  if (name == "lru") return std::make_unique<LRUEvictionPolicy>();
  return makeError(StatusCode::kInvalidConfig, "unknown cache eviction policy");
}

Result<std::map<storage::PhysicalDiskId, uint64_t>> validateEvictionSelection(
    std::span<const EvictionCandidate> candidates,
    const EvictionContext &context,
    std::span<const size_t> selected,
    size_t batchLimit) {
  if (batchLimit == 0 || selected.size() > batchLimit) {
    return makeError(StatusCode::kInvalidArg, "eviction selection exceeds batch limit");
  }
  std::set<size_t> unique;
  std::map<storage::PhysicalDiskId, uint64_t> released;
  for (auto index : selected) {
    if (index >= candidates.size()) return makeError(StatusCode::kInvalidArg, "eviction index out of range");
    if (!unique.emplace(index).second) return makeError(StatusCode::kInvalidArg, "duplicate eviction index");
    const auto &candidate = candidates[index];
    if (protectedFromEviction(candidate, context)) {
      return makeError(StatusCode::kInvalidArg, "eviction selection violates protection period");
    }
    bool contributes = false;
    for (const auto &[disk, footprint] : candidate.physicalFootprintByDisk) {
      if (footprint == 0 || !context.bytesToReleaseByDisk.contains(disk)) continue;
      contributes = true;
      released[disk] += std::min(footprint, std::numeric_limits<uint64_t>::max() - released[disk]);
    }
    if (!contributes) return makeError(StatusCode::kInvalidArg, "eviction selection does not release pressured disk");
  }
  return released;
}

}  // namespace hf3fs::cache_manager
