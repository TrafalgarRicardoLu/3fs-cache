#include "cache_manager/reconcile/CacheReconciler.h"

#include <algorithm>
#include <array>
#include <folly/ScopeGuard.h>
#include <folly/experimental/coro/Collect.h>
#include <folly/logging/xlog.h>
#include <limits>
#include <set>
#include <string_view>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {
namespace {

uint64_t addSaturated(uint64_t left, uint64_t right) {
  return left + std::min(right, std::numeric_limits<uint64_t>::max() - left);
}

std::string errorCategory(const Status &error) { return std::string(StatusCode::toString(error.code())); }

void appendCategory(std::string &summary, std::string_view scope, const Status &error) {
  if (!summary.empty()) summary += ',';
  summary += scope;
  summary += ':';
  summary += errorCategory(error);
}

cache::ReconcileProgress summarize(const CacheReconcileRunResult &result,
                                   cache::ReconcileRunId runId,
                                   uint64_t startedAtMs,
                                   uint64_t completedAtMs,
                                   uint64_t lastSuccessAtMs) {
  cache::ReconcileProgress progress;
  progress.runId = runId;
  progress.startedAtMs = startedAtMs;
  progress.updatedAtMs = std::max(startedAtMs, completedAtMs);
  progress.lastSuccessAtMs = lastSuccessAtMs;
  if (result.metadataToStorage) {
    progress.scanned = addSaturated(progress.scanned, result.metadataToStorage->scanned);
    progress.repaired = addSaturated(progress.repaired, result.metadataToStorage->repaired);
    progress.missing = addSaturated(progress.missing, result.metadataToStorage->missing);
    progress.conflicts = addSaturated(progress.conflicts, result.metadataToStorage->conflicts);
    progress.retryable = addSaturated(progress.retryable, result.metadataToStorage->retryable);
  }
  if (result.storageToMetadata) {
    progress.scanned = addSaturated(progress.scanned, result.storageToMetadata->scanned);
    progress.repaired = addSaturated(progress.repaired, result.storageToMetadata->retired);
    progress.orphaned = addSaturated(progress.orphaned, result.storageToMetadata->orphans);
    progress.conflicts = addSaturated(progress.conflicts, result.storageToMetadata->conflicts);
    progress.retryable = addSaturated(progress.retryable, result.storageToMetadata->retryable);
  }
  progress.retryable = addSaturated(progress.retryable, result.targetFailures.size());
  if (result.metadataError) {
    progress.retryable = addSaturated(progress.retryable, 1);
    appendCategory(progress.error, "metadata", *result.metadataError);
  }
  if (result.storageError) {
    progress.retryable = addSaturated(progress.retryable, 1);
    appendCategory(progress.error, "storage", *result.storageError);
  }
  if (!result.targetFailures.empty()) appendCategory(progress.error, "target", result.targetFailures.front().error);
  auto deferred = result.metadataToStorage ? result.metadataToStorage->deferred : 0;
  deferred = addSaturated(deferred, result.storageToMetadata ? result.storageToMetadata->deferred : 0);
  if (result.stopped && progress.error.empty()) progress.error = "run:stopped";
  auto degraded =
      result.stopped || deferred != 0 || progress.conflicts != 0 || progress.retryable != 0 || !progress.error.empty();
  progress.state = degraded ? cache::ReconcileRunState::DEGRADED : cache::ReconcileRunState::HEALTHY;
  if (!degraded) progress.lastSuccessAtMs = progress.updatedAtMs;
  return progress;
}

void recordMetrics(const cache::ReconcileProgress &progress) {
  cache::metrics::Tags tags;
  tags.reason = progress.state == cache::ReconcileRunState::HEALTHY ? "healthy" : "degraded";
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_RUN, 1, tags);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_SCANNED, progress.scanned);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_ORPHAN, progress.orphaned);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_MISSING, progress.missing);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_CONFLICT, progress.conflicts);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_REPAIRED, progress.repaired);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_RECONCILE_RETRYABLE, progress.retryable);
  if (progress.lastSuccessAtMs != 0) {
    cache::metrics::setGauge(cache::metrics::Event::MANAGER_RECONCILE_LAST_SUCCESS_MS, progress.lastSuccessAtMs);
  }
}

void add(StorageToMetadataResult &total, const StorageToMetadataResult &page) {
  total.scanned = addSaturated(total.scanned, page.scanned);
  total.delegated = addSaturated(total.delegated, page.delegated);
  total.orphans = addSaturated(total.orphans, page.orphans);
  total.older = addSaturated(total.older, page.older);
  total.newer = addSaturated(total.newer, page.newer);
  total.retired = addSaturated(total.retired, page.retired);
  total.conflicts = addSaturated(total.conflicts, page.conflicts);
  total.retryable = addSaturated(total.retryable, page.retryable);
  total.deferred = addSaturated(total.deferred, page.deferred);
  total.stopped = total.stopped || page.stopped;
}

struct TargetStreamResult {
  storage::TargetId targetId{};
  Result<StorageToMetadataResult> result;
  uint64_t restarts{0};
};

CoTask<TargetStreamResult> streamTarget(StorageInventoryReader &reader,
                                        const std::shared_ptr<CacheManagerBackend> &backend,
                                        uint32_t pageSize,
                                        const std::shared_ptr<ReconcileRunControl> &control,
                                        storage::TargetId targetId) {
  StorageToMetadataResult targetTotal;
  Result<Void> streamed = makeError(CacheCode::kUnavailable, "cache inventory was not scanned");
  uint64_t restarts = 0;
  for (uint32_t attempt = 0; attempt < 2; ++attempt) {
    targetTotal = {};
    streamed = co_await reader.readTargetPages(targetId, [&](TargetCacheInventory page) -> CoTryTask<void> {
      StorageToMetadataChecker checker(backend, pageSize, control->dryRun(), control);
      const std::array inventories{std::move(page)};
      auto checked = co_await checker.run(inventories);
      CO_RETURN_ON_ERROR(checked);
      add(targetTotal, *checked);
      co_return Void{};
    });
    if (streamed.hasValue() || streamed.error().code() != CacheCode::kInvalidResponse || attempt != 0) break;
    ++restarts;
  }
  if (streamed.hasError()) co_return TargetStreamResult{targetId, makeError(streamed.error()), restarts};
  co_return TargetStreamResult{targetId, std::move(targetTotal), restarts};
}

}  // namespace

CacheReconciler::CacheReconciler(std::shared_ptr<CacheManagerBackend> backend,
                                 CacheCleanupWorker &cleanup,
                                 CacheReconcilerConfig config,
                                 ReconcileRunControl::Clock clock,
                                 WallClock wallClock)
    : backend_(std::move(backend)),
      cleanup_(cleanup),
      config_(config),
      clock_(std::move(clock)),
      wallClock_(std::move(wallClock)) {
  progress_.state = cache::ReconcileRunState::NEVER_RUN;
}

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
  StorageToMetadataResult total;
  for (size_t begin = 0; begin < targetIds.size(); begin += config_.maxTargetConcurrency) {
    if (control->shouldStop()) break;
    auto end = std::min(targetIds.size(), begin + config_.maxTargetConcurrency);
    std::vector<CoTask<TargetStreamResult>> tasks;
    tasks.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
      auto targetId = targetIds[index];
      tasks.push_back(streamTarget(reader, backend_, config_.pageSize, control, targetId));
    }
    auto targets = co_await folly::coro::collectAllRange(std::move(tasks));
    for (auto &target : targets) {
      result.inventoryRestarts = addSaturated(result.inventoryRestarts, target.restarts);
      if (target.result.hasError()) {
        result.targetFailures.push_back({target.targetId, target.result.error()});
      } else {
        ++result.targetsSucceeded;
        add(total, *target.result);
      }
    }
  }
  if (total.scanned != 0 || total.deferred != 0 || total.stopped) result.storageToMetadata = total;
}

CoTryTask<CacheReconcileRunResult> CacheReconciler::run(std::span<const storage::TargetId> targetIds,
                                                        std::optional<bool> dryRunOverride) {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_) co_return makeError(StatusCode::kInvalidConfig, "cache reconciler backend is missing");
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(CacheCode::kUnavailable, "cache reconciler is stopped");
  }
  std::set<storage::TargetId> uniqueTargets;
  for (auto targetId : targetIds) {
    if (targetId == storage::TargetId{} || !uniqueTargets.emplace(targetId).second) {
      co_return makeError(StatusCode::kInvalidArg, "invalid or duplicate reconcile target");
    }
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

  const auto runId = cache::ReconcileRunId{Uuid::random()};
  auto startedAtMs = wallClock_();
  {
    std::scoped_lock lock(mutex_);
    const auto lastSuccessAtMs = progress_.lastSuccessAtMs;
    startedAtMs = std::max(startedAtMs, lastSuccessAtMs);
    progress_ = {};
    progress_.runId = runId;
    progress_.state = cache::ReconcileRunState::RUNNING;
    progress_.startedAtMs = startedAtMs;
    progress_.updatedAtMs = startedAtMs;
    progress_.lastSuccessAtMs = lastSuccessAtMs;
    lastRunDryRun_ = dryRunOverride.value_or(config_.dryRun);
  }
  cache::metrics::setGauge(cache::metrics::Event::MANAGER_RECONCILE_LAST_START_MS, startedAtMs);
  const auto dryRun = dryRunOverride.value_or(config_.dryRun);
  XLOGF(INFO, "Cache reconcile started: run_id={}, targets={}, dry_run={}", runId, targetIds.size(), dryRun);
  auto control = std::make_shared<ReconcileRunControl>(config_.maxMutations, config_.maxRuntime, dryRun, clock_);
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
  const auto completedAtMs = wallClock_();
  cache::ReconcileProgress progress;
  {
    std::scoped_lock lock(mutex_);
    progress = summarize(result, runId, startedAtMs, completedAtMs, progress_.lastSuccessAtMs);
    progress_ = progress;
    lastResult_ = result;
  }
  recordMetrics(progress);
  XLOGF(INFO,
        "Cache reconcile completed: run_id={}, state={}, scanned={}, orphan={}, missing={}, conflict={}, repaired={}, "
        "retryable={}, error={}",
        runId,
        magic_enum::enum_name(progress.state),
        progress.scanned,
        progress.orphaned,
        progress.missing,
        progress.conflicts,
        progress.repaired,
        progress.retryable,
        progress.error);
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

std::optional<bool> CacheReconciler::lastRunDryRun() const {
  std::scoped_lock lock(mutex_);
  return lastRunDryRun_;
}

cache::ReconcileProgress CacheReconciler::status() const {
  std::scoped_lock lock(mutex_);
  return progress_;
}

}  // namespace hf3fs::cache_manager
