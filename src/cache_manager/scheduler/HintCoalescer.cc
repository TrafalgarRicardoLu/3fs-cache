#include "cache_manager/scheduler/HintCoalescer.h"

#include <algorithm>
#include <limits>

namespace hf3fs::cache_manager {

bool HintCoalescer::EntryLess::operator()(const Entry &lhs, const Entry &rhs) const {
  if (lhs.hint.priority != rhs.hint.priority) return lhs.hint.priority > rhs.hint.priority;
  if (lhs.sequence != rhs.sequence) return lhs.sequence < rhs.sequence;
  if (lhs.hint.inode != rhs.hint.inode) return lhs.hint.inode < rhs.hint.inode;
  return lhs.hint.block < rhs.hint.block;
}

bool HintCoalescer::enqueue(LoadHint hint) {
  auto lock = std::unique_lock(mutex_);
  auto key = hint.key();
  auto existing = entries_.find(key);
  if (existing == entries_.end()) {
    Entry entry{std::move(hint), nextSequence_++};
    entries_.emplace(key, entry);
    ordered_.emplace(std::move(entry));
    return true;
  }
  auto merged = existing->second;
  if (hint.priority <= merged.hint.priority) return false;
  ordered_.erase(merged);
  merged.hint.priority = hint.priority;
  merged.hint.reason = hint.reason;
  merged.hint.blockLength = std::max(merged.hint.blockLength, hint.blockLength);
  existing->second = merged;
  ordered_.emplace(std::move(merged));
  return false;
}

std::optional<LoadHint> HintCoalescer::pop() {
  auto lock = std::unique_lock(mutex_);
  if (ordered_.empty()) return std::nullopt;
  auto entry = *ordered_.begin();
  ordered_.erase(ordered_.begin());
  entries_.erase(entry.hint.key());
  return std::move(entry.hint);
}

std::vector<LoadHint> HintCoalescer::popBatch(uint64_t maxBytes) {
  auto lock = std::unique_lock(mutex_);
  std::vector<LoadHint> result;
  if (ordered_.empty()) return result;
  auto first = *ordered_.begin();
  ordered_.erase(ordered_.begin());
  entries_.erase(first.hint.key());
  auto bytes = first.hint.blockLength;
  result.push_back(std::move(first.hint));
  while (result.back().block.toUnderType() != std::numeric_limits<uint32_t>::max()) {
    auto nextBlock = cache::CacheBlockIndex{result.back().block.toUnderType() + 1};
    auto next = entries_.find(cache::CacheBlockKey{result.front().inode.u64(), nextBlock});
    if (next == entries_.end() ||
        (maxBytes != 0 && (bytes >= maxBytes || next->second.hint.blockLength > maxBytes - bytes))) {
      break;
    }
    auto entry = next->second;
    ordered_.erase(entry);
    entries_.erase(next);
    bytes += entry.hint.blockLength;
    result.push_back(std::move(entry.hint));
  }
  return result;
}

size_t HintCoalescer::size() const {
  auto lock = std::unique_lock(mutex_);
  return entries_.size();
}

}  // namespace hf3fs::cache_manager
