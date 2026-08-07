#include "cache_manager/reconcile/CacheReconciler.h"

#include <folly/ScopeGuard.h>
#include <set>

namespace hf3fs::cache_manager {

Result<Void> CacheReconcilerConfig::valid() const {
  if (pageSize == 0 || pageSize > storage::kMaxCacheStorageBatchItems || maxTargetConcurrency == 0 ||
      maxMutations == 0 || maxRuntime <= 0_ns) {
    return makeError(StatusCode::kInvalidConfig, "invalid cache reconciler configuration");
  }
  return Void{};
}

CoTask<void> CacheReconciler::runMetadata(const std::shared_ptr<ReconcileRunControl> &control,
                                          CacheReconcileRunResult &result) {
  if (control->shouldStop()) co_return;
  MetadataToStorageChecker checker(backend_, cleanup_, config_.pageSize, control);
  auto checked = co_await checker.run();
  if (checked.hasError()) {
    result.metadataError = checked.error();
  } else {
    result.metadataToStorage = std::move(*checked);
  }
}

CoTask<void> CacheReconciler::runStorage(std::span<const storage::TargetId> targetIds,
                                         const std::shared_ptr<ReconcileRunControl> &control,
                                         CacheReconcileRunResult &result) {
  if (control->shouldStop()) co_return;
  StorageInventoryReader reader(backend_, config_.pageSize, config_.maxTargetConcurrency, [control] {
    return control->shouldStop();
  });
  auto loaded = co_await reader.readTargets(targetIds);
  std::vector<TargetCacheInventory> inventories;
  inventories.reserve(loaded.size());
  for (size_t index = 0; index < loaded.size(); ++index) {
    if (control->shouldStop()) break;
    if (loaded[index].hasError() && loaded[index].error().code() == CacheCode::kInvalidResponse) {
      ++result.inventoryRestarts;
      loaded[index] = co_await reader.readTarget(targetIds[index]);
    }
    if (loaded[index].hasError()) {
      result.targetFailures.push_back({targetIds[index], loaded[index].error()});
    } else {
      ++result.targetsSucceeded;
      inventories.push_back(std::move(*loaded[index]));
    }
  }
  if (control->shouldStop() || inventories.empty()) co_return;
  StorageToMetadataChecker checker(backend_, config_.pageSize, config_.dryRun, control);
  auto checked = co_await checker.run(inventories);
  if (checked.hasError()) {
    result.storageError = checked.error();
  } else {
    result.storageToMetadata = std::move(*checked);
  }
}

CoTryTask<CacheReconcileRunResult> CacheReconciler::run(std::span<const storage::TargetId> targetIds) {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_) co_return makeError(StatusCode::kInvalidConfig, "cache reconciler backend is missing");
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(CacheCode::kUnavailable, "cache reconciler is stopped");
  }
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    co_return makeError(StatusCode::kQueueConflict, "cache reconciliation is already running");
  }
  auto finish = folly::makeGuard([this] {
    std::scoped_lock lock(mutex_);
    activeControl_.reset();
    running_.store(false, std::memory_order_release);
  });

  std::set<storage::TargetId> uniqueTargets;
  for (auto targetId : targetIds) {
    if (targetId == storage::TargetId{} || !uniqueTargets.emplace(targetId).second) {
      co_return makeError(StatusCode::kInvalidArg, "invalid or duplicate reconcile target");
    }
  }
  auto control =
      std::make_shared<ReconcileRunControl>(config_.maxMutations, config_.maxRuntime, config_.dryRun, clock_);
  {
    std::scoped_lock lock(mutex_);
    activeControl_ = control;
  }
  if (stopping_.load(std::memory_order_acquire)) control->stop();

  CacheReconcileRunResult result;
  result.storageFirst = rounds_.fetch_add(1, std::memory_order_relaxed) % 2 == 1;
  if (result.storageFirst) {
    co_await runStorage(targetIds, control, result);
    co_await runMetadata(control, result);
  } else {
    co_await runMetadata(control, result);
    co_await runStorage(targetIds, control, result);
  }
  result.mutations = control->mutations();
  result.stopped = control->shouldStop();
  {
    std::scoped_lock lock(mutex_);
    lastResult_ = result;
  }
  co_return result;
}

void CacheReconciler::stop() {
  stopping_.store(true, std::memory_order_release);
  std::shared_ptr<ReconcileRunControl> control;
  {
    std::scoped_lock lock(mutex_);
    control = activeControl_;
  }
  if (control) control->stop();
}

std::optional<CacheReconcileRunResult> CacheReconciler::lastResult() const {
  std::scoped_lock lock(mutex_);
  return lastResult_;
}

}  // namespace hf3fs::cache_manager
