#include "cache_manager/admission/CapacityGate.h"

#include <utility>

namespace hf3fs::cache_manager {

CapacityGate::Permit::Permit(CapacityGate *gate, cache::OriginId origin, uint64_t bytes)
    : gate_(gate),
      origin_(origin),
      bytes_(bytes) {}

CapacityGate::Permit::Permit(Permit &&other) noexcept
    : gate_(std::exchange(other.gate_, nullptr)),
      origin_(other.origin_),
      bytes_(other.bytes_) {}

CapacityGate::Permit &CapacityGate::Permit::operator=(Permit &&other) noexcept {
  if (this != &other) {
    release();
    gate_ = std::exchange(other.gate_, nullptr);
    origin_ = other.origin_;
    bytes_ = other.bytes_;
  }
  return *this;
}

CapacityGate::Permit::~Permit() { release(); }

void CapacityGate::Permit::release() {
  if (gate_ != nullptr) {
    gate_->release(origin_, bytes_);
    gate_ = nullptr;
  }
}

CapacityGate::CapacityGate(Limit global, std::map<cache::OriginId, Limit> origins)
    : globalLimit_(global),
      originLimits_(std::move(origins)) {}

Result<CapacityGate::Permit> CapacityGate::tryAcquire(cache::OriginId origin, uint64_t bytes) {
  auto lock = std::unique_lock(mutex_);
  auto limit = originLimits_.find(origin);
  if (limit == originLimits_.end()) return makeError(StatusCode::kInvalidConfig, "origin is not configured");
  auto &originUsage = originUsage_[origin];
  if (globalUsage_.concurrency >= globalLimit_.concurrency || bytes > globalLimit_.bytes - globalUsage_.bytes ||
      originUsage.concurrency >= limit->second.concurrency || bytes > limit->second.bytes - originUsage.bytes) {
    return makeError(CacheCode::kCapacityExceeded, "cache loader inflight limit exceeded");
  }
  ++globalUsage_.concurrency;
  globalUsage_.bytes += bytes;
  ++originUsage.concurrency;
  originUsage.bytes += bytes;
  return Permit(this, origin, bytes);
}

uint64_t CapacityGate::inflightBytes() const {
  auto lock = std::unique_lock(mutex_);
  return globalUsage_.bytes;
}

void CapacityGate::release(cache::OriginId origin, uint64_t bytes) {
  auto lock = std::unique_lock(mutex_);
  auto &originUsage = originUsage_.at(origin);
  --globalUsage_.concurrency;
  globalUsage_.bytes -= bytes;
  --originUsage.concurrency;
  originUsage.bytes -= bytes;
}

}  // namespace hf3fs::cache_manager
