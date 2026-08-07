#include <folly/experimental/coro/Baton.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <future>
#include <gtest/gtest.h>
#include <map>

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

storage::CacheInventoryEntry inventoryEntry(uint32_t block, flat::TargetId target = flat::TargetId{1}) {
  auto placed = placement();
  placed.expectedReplicaTargets = {target};
  placed.coordinatorTargetId = target;
  storage::CacheChunkDescriptor descriptor{{8, cache::CacheBlockIndex{block}},
                                           cache::CacheGeneration{1},
                                           placed,
                                           target,
                                           1,
                                           1};
  return {target,
          storage::PhysicalDiskId{Uuid::from(4, target.toUnderType())},
          {placed.versionedChain, storage::ChunkId{block + 1, 0}},
          descriptor,
          {cache::CacheGeneration{1}, false, 4096, {storage::ChecksumType::CRC32C, block + 1}}};
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
    for (const auto &key : keys) response.results.emplace_back(meta::ReconcileCacheBlockStatus{key});
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

  std::vector<meta::ReconcileCacheBlockStatus> metadata;
  std::map<flat::TargetId, std::vector<Result<storage::ListCacheInventoryRsp>>> inventory;
  uint64_t metadataMutations{0};
  uint64_t storageMutations{0};
  uint64_t metadataLists{0};
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

  auto dryConfig = config(1);
  dryConfig.dryRun = true;
  CacheReconciler dryRun(backend, cleanup, dryConfig);
  auto reported = folly::coro::blockingWait(dryRun.run(targets));
  ASSERT_OK(reported);
  ASSERT_TRUE(reported->storageToMetadata.has_value());
  EXPECT_EQ(reported->storageToMetadata->orphans, 1);
  EXPECT_EQ(reported->storageToMetadata->deferred, 1);
  EXPECT_EQ(backend->storageMutations, 1);
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
  ASSERT_ERROR(folly::coro::blockingWait(reconciler.run(noTargets)), StatusCode::kQueueConflict);
  reconciler.stop();
  release.post();
  ASSERT_OK(running.get());
  ASSERT_ERROR(folly::coro::blockingWait(reconciler.run(noTargets)), CacheCode::kUnavailable);
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
