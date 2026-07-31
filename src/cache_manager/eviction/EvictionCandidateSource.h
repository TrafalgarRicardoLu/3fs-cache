#pragma once

#include <memory>
#include <optional>
#include <set>

#include "cache_manager/capacity/PhysicalTopology.h"
#include "cache_manager/eviction/EvictionPolicy.h"
#include "cache_manager/loader/CacheLoader.h"

namespace hf3fs::cache_manager {

struct EvictionCandidatePage {
  std::vector<EvictionCandidate> candidates;
  std::optional<cache::CacheBlockKey> nextAfter;
  bool more{false};
};

class EvictionCandidateSource {
 public:
  EvictionCandidateSource(std::shared_ptr<CacheManagerBackend> backend,
                          const PhysicalTopology &topology,
                          uint32_t pageSize)
      : backend_(std::move(backend)),
        topology_(topology),
        pageSize_(pageSize) {}

  CoTryTask<EvictionCandidatePage> next(std::optional<cache::CacheBlockKey> after,
                                        const std::set<storage::PhysicalDiskId> &pressuredDisks);

 private:
  Result<EvictionCandidate> convert(const meta::ReadyCacheBlockStatus &status) const;

  std::shared_ptr<CacheManagerBackend> backend_;
  const PhysicalTopology &topology_;
  uint32_t pageSize_;
};

}  // namespace hf3fs::cache_manager
