#include <folly/experimental/TestUtil.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kv/MemDBStore.h"
#include "storage/cache/retire/RetireCoordinator.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

PhysicalDiskId disk(uint64_t id = 1) { return PhysicalDiskId{Uuid::from(60, id)}; }

CoordinateCacheRetireItem retireItem(uint64_t operation = 1) {
  CoordinateCacheRetireItem item;
  item.logicalKey = {700, cache::CacheBlockIndex{2}};
  item.key = {{ChainId{9}, ChainVer{4}}, ChunkId{0xEC, 2}};
  item.expectedGeneration = cache::CacheGeneration{8};
  item.placement = *PlacementIdentity::create(item.key.vChainId,
                                              {TargetId{1}, TargetId{2}, TargetId{3}},
                                              TargetId{1},
                                              Uuid::from(61, operation));
  item.evictionEpoch = cache::EvictionEpoch{5};
  item.operationId = Uuid::from(62, operation);
  return item;
}

struct MemoryStores {
  kv::KVStore::Config config;
  CacheEventJournal journal{std::make_unique<kv::MemDBStore>(config), 100, 1_MB};
  RetireOperationStore operations{journal};

  MemoryStores() {
    EXPECT_TRUE(journal.init());
    EXPECT_TRUE(operations.init());
  }
};

TEST(TestRetireCoordinator, PersistsFullOperationBeforeDeletionAndRetriesOnlyMissingReplica) {
  MemoryStores stores;
  auto item = retireItem();
  std::vector<TargetId> calls;
  bool targetTwoAvailable = false;
  RetireCoordinator coordinator(
      stores.operations,
      [&](TargetId targetId, const RetireCacheReplicaItem &replica) -> CoTryTask<RetireCacheReplicaResult> {
        calls.push_back(targetId);
        EXPECT_EQ(replica.targetId, targetId);
        EXPECT_EQ(replica.placement, item.placement);
        auto persisted = stores.operations.get(item.operationId);
        EXPECT_TRUE(persisted);
        EXPECT_EQ(persisted->state, RetireOperationState::PREPARED);
        if (targetId == TargetId{2} && !targetTwoAvailable) co_return makeError(CacheCode::kUnavailable);
        co_return RetireCacheReplicaResult{item.operationId, true};
      });

  auto partial = folly::coro::blockingWait(coordinator.coordinate(item, disk()));
  ASSERT_ERROR(partial, CacheCode::kUnavailable);
  ASSERT_EQ(calls, (std::vector<TargetId>{TargetId{1}, TargetId{2}, TargetId{3}}));
  auto persisted = stores.operations.get(item.operationId);
  ASSERT_OK(persisted);
  EXPECT_EQ(persisted->durableRetiredTargets, (std::vector<TargetId>{TargetId{1}, TargetId{3}}));
  EXPECT_EQ(persisted->item.placement, item.placement);
  EXPECT_TRUE(stores.journal.deliveryBatch(10)->empty());
  ASSERT_EQ(stores.journal.prepared()->size(), size_t{1});

  calls.clear();
  targetTwoAvailable = true;
  auto completed = folly::coro::blockingWait(coordinator.coordinate(item, disk()));
  ASSERT_OK(completed);
  EXPECT_TRUE(completed->allReplicasDurableRetired);
  EXPECT_EQ(calls, (std::vector<TargetId>{TargetId{2}}));
  persisted = stores.operations.get(item.operationId);
  ASSERT_OK(persisted);
  EXPECT_EQ(persisted->state, RetireOperationState::COMPLETED);
  auto deliveries = stores.journal.deliveryBatch(10);
  ASSERT_OK(deliveries);
  ASSERT_EQ(deliveries->size(), size_t{1});
  const auto &event = deliveries->front().intent;
  EXPECT_EQ(event.type, cache::CacheStorageEventType::DELETED);
  EXPECT_EQ(event.logicalRetireOperationId, std::optional<Uuid>{item.operationId});
  EXPECT_EQ(event.logicalKey, item.logicalKey);
  EXPECT_EQ(event.storageKey.vChainId, item.key.vChainId);
  EXPECT_EQ(event.storageKey.chunkId, item.key.chunkId);
  EXPECT_EQ(event.placement, item.placement);
  EXPECT_EQ(event.evictionEpoch, std::optional<cache::EvictionEpoch>{item.evictionEpoch});

  calls.clear();
  ASSERT_OK(stores.journal.acknowledge(deliveries->front().sequence));
  ASSERT_OK(folly::coro::blockingWait(coordinator.coordinate(item, disk())));
  EXPECT_TRUE(calls.empty());
  EXPECT_TRUE(stores.journal.deliveryBatch(10)->empty());
}

TEST(TestRetireCoordinator, ActiveOldReplicaPreventsLogicalDeletion) {
  MemoryStores stores;
  auto item = retireItem(2);
  RetireCoordinator coordinator(
      stores.operations,
      [&](TargetId targetId, const RetireCacheReplicaItem &) -> CoTryTask<RetireCacheReplicaResult> {
        if (targetId == TargetId{3}) co_return makeError(CacheCode::kStateConflict, "old replica remains active");
        co_return RetireCacheReplicaResult{item.operationId, true};
      });
  ASSERT_ERROR(folly::coro::blockingWait(coordinator.coordinate(item, disk())), CacheCode::kStateConflict);
  ASSERT_TRUE(stores.journal.deliveryBatch(10)->empty());
  auto operation = stores.operations.get(item.operationId);
  ASSERT_OK(operation);
  EXPECT_EQ(operation->state, RetireOperationState::PREPARED);
  EXPECT_EQ(operation->durableRetiredTargets, (std::vector<TargetId>{TargetId{1}, TargetId{2}}));
}

TEST(TestRetireCoordinator, JournalReservationFailsBeforeAnyReplicaDeletion) {
  kv::KVStore::Config config;
  CacheEventJournal journal(std::make_unique<kv::MemDBStore>(config), 1, 1_MB);
  ASSERT_OK(journal.init());
  RetireOperationStore operations(journal);
  ASSERT_OK(operations.init());
  size_t calls = 0;
  RetireCoordinator coordinator(operations,
                                [&](TargetId, const RetireCacheReplicaItem &) -> CoTryTask<RetireCacheReplicaResult> {
                                  ++calls;
                                  co_return RetireCacheReplicaResult{};
                                });
  ASSERT_ERROR(folly::coro::blockingWait(coordinator.coordinate(retireItem(20), disk())), CacheCode::kJournalFull);
  EXPECT_EQ(calls, size_t{0});
  EXPECT_TRUE(journal.prepared()->empty());
}

TEST(TestRetireCoordinator, RejectsOperationIdentityReuseAndIgnoresRouteOrder) {
  MemoryStores stores;
  auto item = retireItem(3);
  std::vector<TargetId> routed{TargetId{3}, TargetId{1}, TargetId{2}};
  RetireCoordinator coordinator(
      stores.operations,
      [&](TargetId targetId, const RetireCacheReplicaItem &) -> CoTryTask<RetireCacheReplicaResult> {
        EXPECT_NE(std::find(routed.begin(), routed.end(), targetId), routed.end());
        co_return RetireCacheReplicaResult{item.operationId, true};
      });
  ASSERT_OK(folly::coro::blockingWait(coordinator.coordinate(item, disk())));

  auto reused = item;
  reused.expectedGeneration = cache::CacheGeneration{9};
  ASSERT_ERROR(folly::coro::blockingWait(coordinator.coordinate(reused, disk())), CacheCode::kStateConflict);
  ASSERT_EQ(stores.journal.deliveryBatch(10)->size(), size_t{1});
}

TEST(TestRetireCoordinator, RecoversPreparedAcknowledgementsAcrossRestart) {
  folly::test::TemporaryDirectory directory;
  kv::KVStore::Config config;
  config.set_type(kv::KVStore::Type::LevelDB);
  auto open = [&](bool create) {
    kv::KVStore::Options options;
    options.type = kv::KVStore::Type::LevelDB;
    options.path = directory.path() / "retire-events";
    options.createIfMissing = create;
    auto journal = std::make_unique<CacheEventJournal>(kv::KVStore::create(config, options), 100, 1_MB);
    EXPECT_TRUE(journal->init());
    auto operations = std::make_unique<RetireOperationStore>(*journal);
    EXPECT_TRUE(operations->init());
    return std::pair{std::move(journal), std::move(operations)};
  };

  auto item = retireItem(4);
  {
    auto [journal, operations] = open(true);
    RetireCoordinator coordinator(
        *operations,
        [&](TargetId targetId, const RetireCacheReplicaItem &) -> CoTryTask<RetireCacheReplicaResult> {
          if (targetId == TargetId{2}) co_return makeError(CacheCode::kUnavailable);
          co_return RetireCacheReplicaResult{item.operationId, true};
        });
    ASSERT_ERROR(folly::coro::blockingWait(coordinator.coordinate(item, disk(4))), CacheCode::kUnavailable);
    ASSERT_TRUE(journal->deliveryBatch(10)->empty());
  }
  {
    auto [journal, operations] = open(false);
    std::vector<TargetId> retried;
    RetireCoordinator coordinator(
        *operations,
        [&](TargetId targetId, const RetireCacheReplicaItem &) -> CoTryTask<RetireCacheReplicaResult> {
          retried.push_back(targetId);
          co_return RetireCacheReplicaResult{item.operationId, true};
        });
    ASSERT_OK(folly::coro::blockingWait(coordinator.coordinate(item, disk(4))));
    EXPECT_EQ(retried, (std::vector<TargetId>{TargetId{2}}));
    ASSERT_EQ(journal->deliveryBatch(10)->size(), size_t{1});
  }
}

}  // namespace
}  // namespace hf3fs::storage::test
