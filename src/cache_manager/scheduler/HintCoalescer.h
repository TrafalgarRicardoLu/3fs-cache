#pragma once

#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "fbs/cache_manager/Service.h"

namespace hf3fs::cache_manager {

inline constexpr size_t kMaxJobClaimsPerHint = 64;

struct JobLoadClaim {
  cache::PrefetchJobId jobId;
  int32_t priority{0};
  std::function<void(const Status &)> completion;
};

struct LoadHint {
  meta::InodeId inode{};
  cache::CacheBlockIndex block{};
  uint64_t blockLength{0};
  EnsureReason reason{EnsureReason::FOREGROUND_MISS};
  int32_t priority{0};
  std::vector<JobLoadClaim> jobClaims;

  LoadHint(meta::InodeId inode = meta::InodeId{},
           cache::CacheBlockIndex block = cache::CacheBlockIndex{},
           uint64_t blockLength = 0,
           EnsureReason reason = EnsureReason::FOREGROUND_MISS,
           int32_t priority = 0,
           std::vector<JobLoadClaim> jobClaims = {})
      : inode(inode),
        block(block),
        blockLength(blockLength),
        reason(reason),
        priority(priority),
        jobClaims(std::move(jobClaims)) {}

  cache::CacheBlockKey key() const { return {inode.u64(), block}; }
  void notify(const Status &status) const noexcept;
};

class HintCoalescer {
 public:
  bool enqueue(LoadHint hint);
  Result<bool> attach(LoadHint hint);
  bool cancel(const cache::CacheBlockKey &key, cache::PrefetchJobId jobId);
  std::optional<LoadHint> pop();
  std::vector<LoadHint> popBatch(uint64_t maxBytes);
  size_t size() const;

 private:
  struct Entry {
    LoadHint hint;
    uint64_t sequence{0};
    std::optional<std::pair<int32_t, EnsureReason>> baseClaim;
  };
  struct EntryLess {
    bool operator()(const Entry &lhs, const Entry &rhs) const;
  };
  struct KeyLess {
    bool operator()(const cache::CacheBlockKey &lhs, const cache::CacheBlockKey &rhs) const {
      if (lhs.inode != rhs.inode) return lhs.inode < rhs.inode;
      return lhs.block < rhs.block;
    }
  };

  mutable std::mutex mutex_;
  uint64_t nextSequence_{0};
  std::map<cache::CacheBlockKey, Entry, KeyLess> entries_;
  std::set<Entry, EntryLess> ordered_;

  static void rebuild(Entry &entry);
  static bool batchCompatible(const LoadHint &first, const LoadHint &next);
};

}  // namespace hf3fs::cache_manager
