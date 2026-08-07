#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <folly/ScopeGuard.h>
#include <functional>
#include <utility>

#include "common/utils/Coroutine.h"

namespace hf3fs::cache_manager {

enum class StartupRecoveryStage : uint8_t {
  ROUTING,
  PERMIT_AND_LOADING,
  EVICTION_AND_CLEANUP,
  JOBS,
  RECONCILE,
};

class StartupRecoveryCoordinator {
 public:
  using Recover = std::function<CoTryTask<void>()>;
  using Cleanup = std::function<void()>;

  struct Step {
    StartupRecoveryStage stage;
    Recover recover;
    Cleanup cleanup;
  };

  explicit StartupRecoveryCoordinator(std::array<Step, 5> steps)
      : steps_(std::move(steps)) {}

  CoTryTask<void> run() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      co_return makeError(StatusCode::kQueueConflict, "startup recovery is already running");
    }
    auto finish = folly::makeGuard([this] { running_.store(false, std::memory_order_release); });
    constexpr std::array expectedStages{StartupRecoveryStage::ROUTING,
                                        StartupRecoveryStage::PERMIT_AND_LOADING,
                                        StartupRecoveryStage::EVICTION_AND_CLEANUP,
                                        StartupRecoveryStage::JOBS,
                                        StartupRecoveryStage::RECONCILE};
    for (size_t index = 0; index < steps_.size(); ++index) {
      if (steps_[index].stage != expectedStages[index] || !steps_[index].recover) {
        co_return makeError(StatusCode::kInvalidConfig, "invalid startup recovery sequence");
      }
    }
    for (size_t index = 0; index < steps_.size(); ++index) {
      auto &step = steps_[index];
      auto recovered = co_await step.recover();
      if (recovered.hasError()) {
        rollback(index);
        co_return makeError(recovered.error());
      }
    }
    co_return Void{};
  }

  bool running() const { return running_.load(std::memory_order_acquire); }

 private:
  void rollback(size_t failed) {
    for (size_t count = failed + 1; count > 0; --count) {
      auto &cleanup = steps_[count - 1].cleanup;
      if (cleanup) cleanup();
    }
  }

  std::array<Step, 5> steps_;
  std::atomic<bool> running_{false};
};

}  // namespace hf3fs::cache_manager
