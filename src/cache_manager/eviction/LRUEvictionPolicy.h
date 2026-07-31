#pragma once

#include "cache_manager/eviction/EvictionPolicy.h"

namespace hf3fs::cache_manager {

class LRUEvictionPolicy final : public EvictionPolicy {
 public:
  Result<std::vector<size_t>> select(std::span<const EvictionCandidate> candidates,
                                     const EvictionContext &context) override;
};

}  // namespace hf3fs::cache_manager
