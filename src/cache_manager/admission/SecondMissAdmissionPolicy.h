#pragma once

#include <map>
#include <mutex>
#include <tuple>

#include "cache_manager/admission/AdmissionPolicy.h"

namespace hf3fs::cache_manager {

struct CacheBlockKeyLess {
  bool operator()(const cache::CacheBlockKey &lhs, const cache::CacheBlockKey &rhs) const {
    return std::tie(lhs.inode, lhs.block.toUnderType()) < std::tie(rhs.inode, rhs.block.toUnderType());
  }
};

class SecondMissAdmissionPolicy final : public AdmissionPolicy {
 public:
  SecondMissAdmissionPolicy(uint64_t windowNs, uint32_t maxEntries)
      : windowNs_(windowNs),
        maxEntries_(maxEntries) {}

  AdmissionDecision evaluate(const AdmissionContext &context) override;
  size_t size() const;

 private:
  void evictOldest();

  const uint64_t windowNs_;
  const uint32_t maxEntries_;
  mutable std::mutex mutex_;
  std::map<cache::CacheBlockKey, uint64_t, CacheBlockKeyLess> firstMisses_;
};

}  // namespace hf3fs::cache_manager
