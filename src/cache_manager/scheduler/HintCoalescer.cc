#include "cache_manager/scheduler/HintCoalescer.h"

#include <algorithm>
#include <limits>

namespace hf3fs::cache_manager {

void LoadHint::notify(const Status &status) const noexcept {
  for (const auto &claim : jobClaims) {
    if (!claim.completion) continue;
    try {
      claim.completion(status);
    } catch (...) {
    }
  }
}

bool HintCoalescer::EntryLess::operator()(const Entry &lhs, const Entry &rhs) const {
  if (lhs.hint.priority != rhs.hint.priority) return lhs.hint.priority > rhs.hint.priority;
  if (lhs.sequence != rhs.sequence) return lhs.sequence < rhs.sequence;
  if (lhs.hint.inode != rhs.hint.inode) return lhs.hint.inode < rhs.hint.inode;
  return lhs.hint.block < rhs.hint.block;
}

void HintCoalescer::rebuild(Entry &entry) {
  int32_t priority = std::numeric_limits<int32_t>::min();
  EnsureReason reason = EnsureReason::PREFETCH;
  if (entry.baseClaim) {
    priority = entry.baseClaim->first;
    reason = entry.baseClaim->second;
  }
  for (const auto &claim : entry.hint.jobClaims) {
    if (claim.priority > priority) {
      priority = claim.priority;
      reason = EnsureReason::PREFETCH;
    }
  }
  entry.hint.priority = priority;
  entry.hint.reason = reason;
}

bool HintCoalescer::enqueue(LoadHint hint) {
  auto result = attach(std::move(hint));
  return result.hasValue() && *result;
}

Result<bool> HintCoalescer::attach(LoadHint hint) {
  auto lock = std::unique_lock(mutex_);
  auto key = hint.key();
  auto existing = entries_.find(key);
  if (existing == entries_.end()) {
    if (hint.jobClaims.size() > kMaxJobClaimsPerHint) {
      return makeError(StatusCode::kQueueConflict, "too many Job claims for one cache block");
    }
    std::optional<std::pair<int32_t, EnsureReason>> baseClaim;
    if (hint.jobClaims.empty() || hint.reason != EnsureReason::PREFETCH) {
      baseClaim = std::pair{hint.priority, hint.reason};
    }
    Entry entry{std::move(hint), nextSequence_++, baseClaim};
    rebuild(entry);
    entries_.emplace(key, entry);
    ordered_.emplace(std::move(entry));
    return true;
  }
  auto merged = existing->second;
  ordered_.erase(merged);
  merged.hint.blockLength = std::max(merged.hint.blockLength, hint.blockLength);
  if (hint.jobClaims.empty() || hint.reason != EnsureReason::PREFETCH) {
    if (!merged.baseClaim || hint.priority > merged.baseClaim->first) {
      merged.baseClaim = std::pair{hint.priority, hint.reason};
    }
  }
  for (auto &claim : hint.jobClaims) {
    auto found = std::find_if(merged.hint.jobClaims.begin(), merged.hint.jobClaims.end(), [&](const auto &current) {
      return current.jobId == claim.jobId;
    });
    if (found == merged.hint.jobClaims.end()) {
      if (merged.hint.jobClaims.size() >= kMaxJobClaimsPerHint) {
        ordered_.emplace(existing->second);
        return makeError(StatusCode::kQueueConflict, "too many Job claims for one cache block");
      }
      merged.hint.jobClaims.push_back(std::move(claim));
    } else {
      found->priority = std::max(found->priority, claim.priority);
      if (claim.completion) found->completion = std::move(claim.completion);
    }
  }
  rebuild(merged);
  existing->second = merged;
  ordered_.emplace(std::move(merged));
  return false;
}

bool HintCoalescer::cancel(const cache::CacheBlockKey &key, cache::PrefetchJobId jobId) {
  auto lock = std::unique_lock(mutex_);
  auto found = entries_.find(key);
  if (found == entries_.end()) return false;
  auto updated = found->second;
  auto claim = std::find_if(updated.hint.jobClaims.begin(), updated.hint.jobClaims.end(), [&](const auto &current) {
    return current.jobId == jobId;
  });
  if (claim == updated.hint.jobClaims.end()) return false;
  ordered_.erase(updated);
  updated.hint.jobClaims.erase(claim);
  if (updated.hint.jobClaims.empty() && !updated.baseClaim) {
    entries_.erase(found);
    return true;
  }
  rebuild(updated);
  found->second = updated;
  ordered_.emplace(std::move(updated));
  return true;
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
