#pragma once

#include <cstdint>
#include <map>
#include <mutex>

#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache_manager {

class CapacityGate {
 public:
  struct Limit {
    uint32_t concurrency{1};
    uint64_t bytes{1};
  };

  class Permit {
   public:
    Permit() = default;
    Permit(Permit &&other) noexcept;
    Permit &operator=(Permit &&other) noexcept;
    Permit(const Permit &) = delete;
    Permit &operator=(const Permit &) = delete;
    ~Permit();

   private:
    Permit(CapacityGate *gate, cache::OriginId origin, uint64_t bytes);
    void release();
    CapacityGate *gate_{nullptr};
    cache::OriginId origin_{};
    uint64_t bytes_{0};
    friend class CapacityGate;
  };

  CapacityGate(Limit global, std::map<cache::OriginId, Limit> origins);
  Result<Permit> tryAcquire(cache::OriginId origin, uint64_t bytes);
  uint64_t inflightBytes() const;

 private:
  struct Usage {
    uint32_t concurrency{0};
    uint64_t bytes{0};
  };
  void release(cache::OriginId origin, uint64_t bytes);

  Limit globalLimit_;
  std::map<cache::OriginId, Limit> originLimits_;
  mutable std::mutex mutex_;
  Usage globalUsage_;
  std::map<cache::OriginId, Usage> originUsage_;
};

}  // namespace hf3fs::cache_manager
