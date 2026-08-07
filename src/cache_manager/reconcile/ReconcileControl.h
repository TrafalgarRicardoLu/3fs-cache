#pragma once

#include <atomic>
#include <functional>

#include "common/utils/Duration.h"
#include "common/utils/UtcTime.h"

namespace hf3fs::cache_manager {

enum class ReconcileMutationDecision { ALLOW, DRY_RUN, STOP };

class ReconcileRunControl {
 public:
  using Clock = std::function<SteadyTime()>;

  ReconcileRunControl(uint64_t maxMutations, Duration maxRuntime, bool dryRun, Clock clock = SteadyClock::now)
      : maxMutations_(maxMutations),
        dryRun_(dryRun),
        clock_(std::move(clock)),
        deadline_(clock_() + maxRuntime.asUs()) {}

  bool shouldStop() const {
    return stopping_.load(std::memory_order_acquire) || clock_() >= deadline_ ||
           (!dryRun_ && mutations_.load(std::memory_order_relaxed) >= maxMutations_);
  }

  ReconcileMutationDecision requestMutation() {
    if (stopping_.load(std::memory_order_acquire) || clock_() >= deadline_) {
      return ReconcileMutationDecision::STOP;
    }
    if (dryRun_) return ReconcileMutationDecision::DRY_RUN;
    auto current = mutations_.load(std::memory_order_relaxed);
    while (current < maxMutations_) {
      if (mutations_.compare_exchange_weak(current, current + 1, std::memory_order_relaxed)) {
        return ReconcileMutationDecision::ALLOW;
      }
    }
    return ReconcileMutationDecision::STOP;
  }

  void stop() { stopping_.store(true, std::memory_order_release); }
  uint64_t mutations() const { return mutations_.load(std::memory_order_relaxed); }
  bool dryRun() const { return dryRun_; }

 private:
  uint64_t maxMutations_;
  bool dryRun_;
  Clock clock_;
  SteadyTime deadline_;
  std::atomic<uint64_t> mutations_{0};
  std::atomic<bool> stopping_{false};
};

}  // namespace hf3fs::cache_manager
