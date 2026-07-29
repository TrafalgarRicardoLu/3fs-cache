#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

#include "fbs/cache_manager/Service.h"

namespace hf3fs::cache_manager {

struct LoadHint {
  meta::InodeId inode{};
  cache::CacheBlockIndex block{};
  uint64_t blockLength{0};
  EnsureReason reason{EnsureReason::FOREGROUND_MISS};
  int32_t priority{0};

  cache::CacheBlockKey key() const { return {inode.u64(), block}; }
};

class HintCoalescer {
 public:
  bool enqueue(LoadHint hint);
  std::optional<LoadHint> pop();
  std::vector<LoadHint> popBatch(uint64_t maxBytes);
  size_t size() const;

 private:
  struct Entry {
    LoadHint hint;
    uint64_t sequence{0};
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
};

}  // namespace hf3fs::cache_manager
