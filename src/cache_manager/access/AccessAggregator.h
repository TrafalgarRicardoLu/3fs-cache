#pragma once

#include <map>
#include <mutex>
#include <span>

#include "fbs/cache_manager/Service.h"
#include "fbs/meta/Service.h"

namespace hf3fs::cache_manager {

class AccessAggregator {
 public:
  struct Stats {
    uint64_t inserted{0};
    uint64_t merged{0};
    uint64_t capacityDropped{0};
  };

  explicit AccessAggregator(size_t maxEntries)
      : maxEntries_(maxEntries) {}

  bool record(const CacheAccessReportItem &item, uint64_t managerReceiveTimeNs);
  std::vector<meta::UpdateCacheBlockAccessItem> take(size_t maxItems);
  size_t size() const;
  Stats stats() const;

 private:
  struct Identity {
    cache::CacheBlockKey key;
    cache::CacheGeneration generation;
  };
  struct IdentityLess {
    bool operator()(const Identity &lhs, const Identity &rhs) const {
      if (lhs.key.inode != rhs.key.inode) return lhs.key.inode < rhs.key.inode;
      if (lhs.key.block != rhs.key.block) return lhs.key.block < rhs.key.block;
      return lhs.generation < rhs.generation;
    }
  };

  using Entries = std::map<Identity, uint64_t, IdentityLess>;

  const size_t maxEntries_;
  mutable std::mutex mutex_;
  Entries entries_;
  Stats stats_;
};

}  // namespace hf3fs::cache_manager
