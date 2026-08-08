#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <span>

#include "cache_manager/reconcile/MetadataToStorageChecker.h"
#include "cache_manager/reconcile/StorageToMetadataChecker.h"

namespace hf3fs::cache_manager {

struct CacheReconcilerConfig {
  uint32_t pageSize{256};
  uint32_t maxTargetConcurrency{8};
  uint64_t maxMutations{1000};
  Duration maxRuntime{5_min};
  bool dryRun{false};

  Result<Void> valid() const;
};

struct TargetReconcileFailure {
  storage::TargetId targetId{};
  Status error;
};

struct CacheReconcileRunResult {
  std::optional<MetadataToStorageResult> metadataToStorage;
  std::optional<StorageToMetadataResult> storageToMetadata;
  std::optional<Status> metadataError;
  std::optional<Status> storageError;
  std::vector<TargetReconcileFailure> targetFailures;
  uint64_t targetsSucceeded{0};
  uint64_t inventoryRestarts{0};
  uint64_t mutations{0};
  bool storageFirst{false};
  bool stopped{false};
};

class CacheReconciler {
 public:
  using WallClock = std::function<uint64_t()>;

  CacheReconciler(
      std::shared_ptr<CacheManagerBackend> backend,
      CacheCleanupWorker &cleanup,
      CacheReconcilerConfig config,
      ReconcileRunControl::Clock clock = SteadyClock::now,
      WallClock wallClock = [] { return static_cast<uint64_t>(UtcClock::now().toMicroseconds()) / 1000; });

  CoTryTask<CacheReconcileRunResult> run(std::span<const storage::TargetId> targetIds,
                                         std::optional<bool> dryRun = std::nullopt);
  void stop();
  bool running() const { return running_.load(std::memory_order_acquire); }
  std::optional<CacheReconcileRunResult> lastResult() const;
  std::optional<bool> lastRunDryRun() const;
  cache::ReconcileProgress status() const;

 private:
  CoTask<void> runMetadata(const std::shared_ptr<ReconcileRunControl> &control, CacheReconcileRunResult &result);
  CoTask<void> runStorage(std::span<const storage::TargetId> targetIds,
                          const std::shared_ptr<ReconcileRunControl> &control,
                          CacheReconcileRunResult &result);

  std::shared_ptr<CacheManagerBackend> backend_;
  CacheCleanupWorker &cleanup_;
  CacheReconcilerConfig config_;
  ReconcileRunControl::Clock clock_;
  WallClock wallClock_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<uint64_t> rounds_{0};
  mutable std::mutex mutex_;
  std::shared_ptr<ReconcileRunControl> activeControl_;
  std::optional<CacheReconcileRunResult> lastResult_;
  std::optional<bool> lastRunDryRun_;
  cache::ReconcileProgress progress_;
};

}  // namespace hf3fs::cache_manager
