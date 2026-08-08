#include <folly/experimental/coro/Baton.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <future>
#include <gtest/gtest.h>
#include <map>

#include "cache/metrics/CacheMetrics.h"
#include "cache_manager/reconcile/CacheReconciler.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PlacementIdentity placement() {
  return {{flat::ChainId{1}, flat::ChainVersion{1}}, {flat::TargetId{1}}, flat::TargetId{1}, Uuid::from(1, 1)};
}

meta::ReconcileCacheBlockStatus evictingStatus(uint32_t block) {
  meta::ReconcileCacheBlockStatus status;
  status.key = {7, cache::CacheBlockIndex{block}};
  status.state = cache::CacheBlockState::EVICTING;
  status.blockLength = 4096;
  status.cacheGeneration = cache::CacheGeneration{1};
  status.ready = cache::ReadyIdentity{1, cache::CacheGeneration{1}, 1, block + 1, 4096};
  status.placement = placement();
  status.permit = storage::PermitIdentity{Uuid::from(2, 1), placement(), 1, {{flat::TargetId{1}, 4096}}};
  status.evictionEpoch = cache::EvictionEpoch{1};
  status.retireOperationId = Uuid::from(3, block + 1);
  status.evictionReason = cache::EvictionReason::CAPACITY_WATERMARK;
  return status;
}

meta::ReconcileCacheBlockStatus readyStatus(uint32_t block) {
  auto status = evictingStatus(block);
  status.state = cache::CacheBlockState::READY;
  status.evictionEpoch = {};
  status.retireOperationId = Uuid::zero();
  status.evictionReason = cache::EvictionReason::INVALID;
  return status;
}

storage::CacheInventoryEntry inventoryEntry(uint32_t block,
                                            flat::TargetId target = flat::TargetId{1},
                                            cache::CacheGeneration generation = cache::CacheGeneration{1}) {
  auto placed = placement();
  placed.expectedReplicaTargets = {target};
  placed.coordinatorTargetId = target;
  storage::CacheChunkDescriptor descriptor{{8, cache::CacheBlockIndex{block}}, generation, placed, target, 1, 1};
  return {target,
          storage::PhysicalDiskId{Uuid::from(4, target.toUnderType())},
          {placed.versionedChain, storage::ChunkId{block + 1, 0}},
          descriptor,
          {generation, false, 4096, {storage::ChecksumType::CRC32C, block + 1}}};
}

class ReconcilerBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final { co_return makeError(StatusCode::kNotImplemented); }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &,
                                                       cache::CacheBlockIndex,
                                                       const meta::CacheBlockLease &,
                                                       std::vector<uint8_t>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &,
                         const meta::CacheBlockLease &,
                         const storage::CacheChunkGenerationInfo &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::ListReconcileCacheBlocksRsp> listReconcileCacheBlocks(std::optional<cache::CacheBlockKey>,
                                                                        uint32_t) final {
    ++metadataLists;
    if (entered) entered->set_value();
    if (release) co_await *release;
    meta::ListReconcileCacheBlocksRsp response;
    response.items = metadata;
    response.more = false;
    co_return response;
  }
  CoTryTask<bool> coordinateRetire(const meta::CacheEvictionIdentity &) final {
    ++metadataMutations;
    co_return true;
  }
  CoTryTask<storage::ListCacheInventoryRsp> listCacheInventory(storage::ListCacheInventoryReq request) final {
    auto &responses = inventory[request.targetId];
    if (responses.empty()) co_return makeError(CacheCode::kUnavailable, "target unavailable");
    auto response = std::move(responses.front());
    responses.erase(responses.begin());
    co_return response;
  }
  CoTryTask<meta::ReconcileCacheBlocksRsp> reconcileCacheBlocks(std::vector<cache::CacheBlockKey> keys) final {
    meta::ReconcileCacheBlocksRsp response;
    for (const auto &key : keys) {
      auto status = std::find_if(reconcileStatuses.begin(), reconcileStatuses.end(), [&](const auto &candidate) {
        return candidate.key == key;
      });
      response.results.emplace_back(status == reconcileStatuses.end() ? meta::ReconcileCacheBlockStatus{key} : *status);
    }
    co_return response;
  }
  CoTryTask<storage::RetireCacheReplicasRsp> retireCacheReplicas(
      std::vector<storage::RetireCacheReplicaItem> items) final {
    storageMutations += items.size();
    storage::RetireCacheReplicasRsp response;
    for (const auto &item : items) {
      response.results.emplace_back(storage::RetireCacheReplicaResult{item.operationId, true});
    }
    co_return response;
  }
  CoTryTask<CacheReplicaObservation> queryReconcile(const meta::ReconcileCacheBlockStatus &) final {
    auto index = queryCalls++;
    if (index >= observations.size()) co_return makeError(CacheCode::kUnavailable);
    co_return std::move(observations[index]);
  }

  std::vector<meta::ReconcileCacheBlockStatus> metadata;
  std::vector<meta::ReconcileCacheBlockStatus> reconcileStatuses;
  std::vector<Result<CacheReplicaObservation>> observations;
  std::map<flat::TargetId, std::vector<Result<storage::ListCacheInventoryRsp>>> inventory;
  uint64_t metadataMutations{0};
  uint64_t storageMutations{0};
  uint64_t metadataLists{0};
  uint64_t queryCalls{0};
  std::promise<void> *entered{nullptr};
  folly::coro::Baton *release{nullptr};
};

storage::ListCacheInventoryRsp inventoryPage(std::vector<storage::CacheInventoryEntry> entries,
                                             Uuid epoch = Uuid::from(5, 1)) {
  return {std::move(entries), {}, true, epoch};
}

CacheReconcilerConfig config(uint64_t maxMutations = 100) { return {8, 2, maxMutations, 1_min, false}; }

TEST(TestCacheReconciler, AlternatesDirectionsWhenMutationBudgetIsExhausted) {
  auto backend = std::make_shared<ReconcilerBackend>();
  backend->metadata = {evictingStatus(0)};
  backend->inventory[flat::TargetId{1}] = {inventoryPage({inventoryEntry(0)}), inventoryPage({inventoryEntry(0)})};
  CacheCleanupWorker cleanup(backend);
  CacheReconciler reconciler(backend, cleanup, config(1));
  const std::vector targets{flat::TargetId{1}};

  auto first = folly::coro::blockingWait(reconciler.run(targets));
  ASSERT_OK(first);
  EXPECT_FALSE(first->storageFirst);
  EXPECT_EQ(backend->metadataMutations, 1);
  EXPECT_EQ(backend->storageMutations, 0);
  auto second = folly::coro::blockingWait(reconciler.run(targets));
  ASSERT_OK(second);
  EXPECT_TRUE(second->storageFirst);
  EXPECT_EQ(backend->metadataMutations, 1);
  EXPECT_EQ(backend->storageMutations, 1);
}

TEST(TestCacheReconciler, BoundsMutationsAndSupportsDryRun) {
  auto backend = std::make_shared<ReconcilerBackend>();
  backend->inventory[flat::TargetId{1}] = {inventoryPage({inventoryEntry(0), inventoryEntry(1)}),
                                           inventoryPage({inventoryEntry(0)})};
  CacheCleanupWorker cleanup(backend);
  CacheReconciler reconciler(backend, cleanup, config(1));
  const std::vector targets{flat::TargetId{1}};
  auto limited = folly::coro::blockingWait(reconciler.run(targets));
  ASSERT_OK(limited);
  EXPECT_EQ(limited->mutations, 1);
  EXPECT_TRUE(limited->stopped);
  EXPECT_EQ(backend->storageMutations, 1);

  auto reported = folly::coro::blockingWait(reconciler.run(targets, true));
  ASSERT_OK(reported);
  ASSERT_TRUE(reported->storageToMetadata.has_value());
  EXPECT_EQ(reported->storageToMetadata->orphans, 1);
  EXPECT_EQ(reported->storageToMetadata->deferred, 1);
  EXPECT_EQ(backend->storageMutations, 1);
  ASSERT_TRUE(reconciler.lastRunDryRun().has_value());
  EXPECT_TRUE(*reconciler.lastRunDryRun());
}

TEST(TestCacheReconciler, RetriesChangedEpochAndPreservesPartialTargetFailure) {
  auto backend = std::make_shared<ReconcilerBackend>();
  auto first = inventoryPage({inventoryEntry(0)}, Uuid::from(6, 1));
  first.done = false;
  first.nextCursor = "next";
  auto changed = inventoryPage({inventoryEntry(1)}, Uuid::from(6, 2));
  backend->inventory[flat::TargetId{1}] = {first, changed, inventoryPage({inventoryEntry(0)})};
  CacheCleanupWorker cleanup(backend);
  auto dryConfig = config();
  dryConfig.dryRun = true;
  CacheReconciler reconciler(backend, cleanup, dryConfig);
  const std::vector targets{flat::TargetId{1}, flat::TargetId{2}};
  auto result = folly::coro::blockingWait(reconciler.run(targets));
  ASSERT_OK(result);
  EXPECT_EQ(result->inventoryRestarts, 1);
  EXPECT_EQ(result->targetsSucceeded, 1);
  ASSERT_EQ(result->targetFailures.size(), 1);
  EXPECT_EQ(result->targetFailures.front().targetId, flat::TargetId{2});
  ASSERT_TRUE(result->storageToMetadata.has_value());
  EXPECT_EQ(result->storageToMetadata->scanned, 1);
  auto retained = reconciler.lastResult();
  ASSERT_TRUE(retained.has_value());
  ASSERT_EQ(retained->targetFailures.size(), 1);
  EXPECT_EQ(retained->targetFailures.front().error.code(), CacheCode::kUnavailable);
}

TEST(TestCacheReconciler, RejectsReentryAndStopPreventsFutureRuns) {
  auto backend = std::make_shared<ReconcilerBackend>();
  std::promise<void> entered;
  auto enteredFuture = entered.get_future();
  folly::coro::Baton release;
  backend->entered = &entered;
  backend->release = &release;
  CacheCleanupWorker cleanup(backend);
  CacheReconciler reconciler(backend, cleanup, config());
  const std::span<const storage::TargetId> noTargets;
  auto running = std::async(std::launch::async, [&] { return folly::coro::blockingWait(reconciler.run(noTargets)); });
  enteredFuture.wait();
  EXPECT_EQ(reconciler.status().state, cache::ReconcileRunState::RUNNING);
  ASSERT_ERROR(folly::coro::blockingWait(reconciler.run(noTargets)), StatusCode::kQueueConflict);
  reconciler.stop();
  release.post();
  ASSERT_OK(running.get());
  EXPECT_EQ(reconciler.status().state, cache::ReconcileRunState::DEGRADED);
  ASSERT_ERROR(folly::coro::blockingWait(reconciler.run(noTargets)), CacheCode::kUnavailable);
}

TEST(TestCacheReconciler, ReportsExactMetricsAndDegradedProgress) {
  cache::metrics::resetForTest();
  auto backend = std::make_shared<ReconcilerBackend>();
  backend->metadata = {evictingStatus(0), readyStatus(1), readyStatus(2)};
  backend->observations = {makeError(CacheCode::kNotFound), makeError(CacheCode::kUnavailable)};
  auto newerOwner = readyStatus(4);
  newerOwner.key.inode = 8;
  backend->reconcileStatuses = {newerOwner};
  backend->inventory[flat::TargetId{1}] = {
      inventoryPage({inventoryEntry(3), inventoryEntry(4, flat::TargetId{1}, cache::CacheGeneration{2})})};
  CacheCleanupWorker cleanup(backend);
  uint64_t wallClockMs = 100;
  CacheReconciler reconciler(backend, cleanup, config(), SteadyClock::now, [&] {
    auto value = wallClockMs;
    wallClockMs += 100;
    return value;
  });
  EXPECT_EQ(reconciler.status().state, cache::ReconcileRunState::NEVER_RUN);

  const std::vector targets{flat::TargetId{1}};
  ASSERT_OK(folly::coro::blockingWait(reconciler.run(targets)));
  auto progress = reconciler.status();
  ASSERT_OK(progress.valid());
  EXPECT_EQ(progress.state, cache::ReconcileRunState::DEGRADED);
  EXPECT_EQ(progress.startedAtMs, 100);
  EXPECT_EQ(progress.updatedAtMs, 200);
  EXPECT_EQ(progress.lastSuccessAtMs, 0);
  EXPECT_EQ(progress.scanned, 5);
  EXPECT_EQ(progress.orphaned, 1);
  EXPECT_EQ(progress.repaired, 2);
  EXPECT_EQ(progress.missing, 1);
  EXPECT_EQ(progress.conflicts, 1);
  EXPECT_EQ(progress.retryable, 2);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_RUN), 1);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_SCANNED), 5);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_ORPHAN), 1);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_MISSING), 1);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_CONFLICT), 1);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_REPAIRED), 2);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_RETRYABLE), 2);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_LAST_START_MS), 100);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_RECONCILE_LAST_SUCCESS_MS), 0);
  EXPECT_EQ(cache::metrics::lastTagsForTest(cache::metrics::Event::MANAGER_RECONCILE_RUN).reason, "degraded");
}

TEST(TestCacheReconciler, RedactsErrorsAndResetsOnlyAcrossProcessObjectRestart) {
  auto backend = std::make_shared<ReconcilerBackend>();
  backend->inventory[flat::TargetId{1}] = {inventoryPage({})};
  CacheCleanupWorker cleanup(backend);
  uint64_t wallClockMs = 100;
  auto nextTime = [&] {
    auto value = wallClockMs;
    wallClockMs += 100;
    return value;
  };
  CacheReconciler reconciler(backend, cleanup, config(), SteadyClock::now, nextTime);
  const std::vector healthyTarget{flat::TargetId{1}};
  ASSERT_OK(folly::coro::blockingWait(reconciler.run(healthyTarget)));
  ASSERT_EQ(reconciler.status().lastSuccessAtMs, 200);

  backend->inventory[flat::TargetId{2}] = {
      makeError(CacheCode::kUnavailable, "bucket=private access_key=secret signed_url=token")};
  const std::vector failingTarget{flat::TargetId{2}};
  ASSERT_OK(folly::coro::blockingWait(reconciler.run(failingTarget)));
  auto degraded = reconciler.status();
  ASSERT_OK(degraded.valid());
  EXPECT_EQ(degraded.state, cache::ReconcileRunState::DEGRADED);
  EXPECT_EQ(degraded.lastSuccessAtMs, 200);
  EXPECT_EQ(degraded.retryable, 1);
  EXPECT_NE(degraded.error.find("Cache::Unavailable"), std::string::npos);
  EXPECT_EQ(degraded.error.find("private"), std::string::npos);
  EXPECT_EQ(degraded.error.find("secret"), std::string::npos);
  EXPECT_EQ(degraded.error.find("token"), std::string::npos);

  CacheReconciler restarted(backend, cleanup, config());
  auto reset = restarted.status();
  ASSERT_OK(reset.valid());
  EXPECT_EQ(reset.state, cache::ReconcileRunState::NEVER_RUN);
  EXPECT_EQ(reset.lastSuccessAtMs, 0);
  EXPECT_EQ(reset.scanned, 0);
}

TEST(TestCacheReconciler, StopsAtRuntimeDeadlineBeforeStartingMoreWork) {
  auto backend = std::make_shared<ReconcilerBackend>();
  CacheCleanupWorker cleanup(backend);
  auto deadlineConfig = config();
  deadlineConfig.maxRuntime = 1_s;
  auto start = SteadyClock::now();
  uint64_t calls = 0;
  CacheReconciler reconciler(backend, cleanup, deadlineConfig, [start, calls]() mutable {
    auto now = start + std::chrono::seconds(calls * 2);
    ++calls;
    return now;
  });
  const std::span<const storage::TargetId> noTargets;
  auto result = folly::coro::blockingWait(reconciler.run(noTargets));
  ASSERT_OK(result);
  EXPECT_TRUE(result->stopped);
  EXPECT_EQ(result->mutations, 0);
  EXPECT_EQ(backend->metadataLists, 0);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
