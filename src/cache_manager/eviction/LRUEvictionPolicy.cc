#include "cache_manager/eviction/LRUEvictionPolicy.h"

#include <algorithm>
#include <limits>
#include <numeric>

namespace hf3fs::cache_manager {
namespace {

UtcTime effectiveAccessTime(const EvictionCandidate &candidate) {
  return std::max(candidate.readyAt, candidate.lastAccessAt);
}

bool deficitsMet(const std::map<storage::PhysicalDiskId, uint64_t> &released, const EvictionContext &context) {
  for (const auto &[disk, deficit] : context.bytesToReleaseByDisk) {
    auto it = released.find(disk);
    if (deficit != 0 && (it == released.end() || it->second < deficit)) return false;
  }
  return true;
}

}  // namespace

Result<std::vector<size_t>> LRUEvictionPolicy::select(std::span<const EvictionCandidate> candidates,
                                                      const EvictionContext &context) {
  std::vector<size_t> order(candidates.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::stable_sort(order.begin(), order.end(), [&](size_t left, size_t right) {
    return effectiveAccessTime(candidates[left]) < effectiveAccessTime(candidates[right]);
  });

  std::map<storage::PhysicalDiskId, uint64_t> released;
  std::vector<size_t> selected;
  for (auto index : order) {
    if (deficitsMet(released, context)) break;
    const auto &candidate = candidates[index];
    if (protectedFromEviction(candidate, context)) continue;
    bool contributes = false;
    for (const auto &[disk, footprint] : candidate.physicalFootprintByDisk) {
      auto deficit = context.bytesToReleaseByDisk.find(disk);
      if (footprint == 0 || deficit == context.bytesToReleaseByDisk.end() || released[disk] >= deficit->second)
        continue;
      contributes = true;
    }
    if (!contributes) continue;
    selected.push_back(index);
    for (const auto &[disk, footprint] : candidate.physicalFootprintByDisk) {
      if (!context.bytesToReleaseByDisk.contains(disk)) continue;
      released[disk] += std::min(footprint, std::numeric_limits<uint64_t>::max() - released[disk]);
    }
  }
  return selected;
}

}  // namespace hf3fs::cache_manager
