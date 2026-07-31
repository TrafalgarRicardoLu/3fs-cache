#include "cache_manager/access/AccessAggregator.h"

#include <algorithm>

namespace hf3fs::cache_manager {

bool AccessAggregator::record(const CacheAccessReportItem &item, uint64_t managerReceiveTimeNs) {
  if (maxEntries_ == 0 || managerReceiveTimeNs == 0 || item.valid().hasError()) return false;
  auto lock = std::unique_lock(mutex_);
  Identity identity{item.key, item.generation};
  auto existing = entries_.find(identity);
  if (existing != entries_.end()) {
    existing->second = std::max(existing->second, managerReceiveTimeNs);
    ++stats_.merged;
    return true;
  }
  if (entries_.size() >= maxEntries_) {
    auto oldest = std::min_element(entries_.begin(), entries_.end(), [](const auto &lhs, const auto &rhs) {
      return lhs.second < rhs.second;
    });
    entries_.erase(oldest);
    ++stats_.capacityDropped;
  }
  entries_.emplace(std::move(identity), managerReceiveTimeNs);
  ++stats_.inserted;
  return true;
}

std::vector<meta::UpdateCacheBlockAccessItem> AccessAggregator::take(size_t maxItems) {
  auto lock = std::unique_lock(mutex_);
  std::vector<meta::UpdateCacheBlockAccessItem> result;
  result.reserve(std::min(maxItems, entries_.size()));
  while (!entries_.empty() && result.size() < maxItems) {
    auto node = entries_.extract(entries_.begin());
    result.push_back({node.key().key, node.key().generation, node.mapped()});
  }
  return result;
}

size_t AccessAggregator::size() const {
  auto lock = std::unique_lock(mutex_);
  return entries_.size();
}

AccessAggregator::Stats AccessAggregator::stats() const {
  auto lock = std::unique_lock(mutex_);
  return stats_;
}

}  // namespace hf3fs::cache_manager
