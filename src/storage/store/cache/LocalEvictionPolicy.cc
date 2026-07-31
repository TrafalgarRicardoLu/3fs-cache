#include "storage/store/cache/LocalEvictionPolicy.h"

#include <algorithm>
#include <limits>
#include <numeric>

namespace hf3fs::storage {
namespace {

class LocalLRUEvictionPolicy final : public LocalEvictionPolicy {
 public:
  Result<std::vector<size_t>> select(std::span<const LocalEvictionCandidate> candidates,
                                     const LocalEvictionContext &context) override {
    std::vector<size_t> order(candidates.size());
    std::iota(order.begin(), order.end(), size_t{0});
    std::stable_sort(order.begin(), order.end(), [&](size_t left, size_t right) {
      const auto &lhs = candidates[left].descriptor;
      const auto &rhs = candidates[right].descriptor;
      return std::max(lhs.createdAtNs, lhs.lastAccessAtNs) < std::max(rhs.createdAtNs, rhs.lastAccessAtNs);
    });

    uint64_t released = 0;
    std::vector<size_t> selected;
    for (auto index : order) {
      if (released >= context.bytesToRelease) break;
      const auto &candidate = candidates[index];
      auto effective = std::max(candidate.descriptor.createdAtNs, candidate.descriptor.lastAccessAtNs);
      if (effective > context.nowNs || context.nowNs - effective < context.protectionPeriodNs) continue;
      if (candidate.footprintBytes == 0) continue;
      selected.push_back(index);
      released += std::min(candidate.footprintBytes, std::numeric_limits<uint64_t>::max() - released);
    }
    return selected;
  }
};

}  // namespace

Result<std::unique_ptr<LocalEvictionPolicy>> createLocalEvictionPolicy(std::string_view name) {
  if (name == "lru") return std::make_unique<LocalLRUEvictionPolicy>();
  return makeError(StatusCode::kInvalidConfig, "unknown local cache eviction policy");
}

}  // namespace hf3fs::storage
