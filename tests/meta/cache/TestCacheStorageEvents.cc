#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "meta/store/cache/CacheEventStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr uint64_t kBlockBytes = 4096;

class TestCacheStorageEvents : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster() {
    static MockCluster::Config config = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().event_trace_log().set_enabled(false);
      value.mock_meta().set_enable_cache_phase2(true);
      return value;
    }();
    return createMockCluster(config);
  }
};

void enableCacheFeature(MockCluster &cluster) {
  auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
  for (auto &[_, node] : routing->raw()->nodes) {
    if (node.type == flat::NodeType::META) {
      node.cacheSchemaVersion = cache::kCacheSchemaVersion;
      node.cacheProtocolVersion = cache::kCacheProtocolVersion;
    }
  }
  cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
}

storage::PhysicalDiskId disk(uint64_t id) { return {Uuid::from(20, id)}; }

storage::PlacementIdentity placement(uint64_t id) {
  return *storage::PlacementIdentity::create({flat::ChainId{1}, flat::ChainVersion{1}},
                                             {flat::TargetId{1}},
                                             flat::TargetId{1},
                                             Uuid::from(21, id));
}

storage::PermitIdentity permit(uint64_t id) {
  return {Uuid::from(22, id), placement(id), 1, {{flat::TargetId{1}, kBlockBytes}}};
}

CacheBlockRecord recordFor(cache::CacheBlockState state, uint64_t id) {
  CacheBlockRecord record;
  record.key = {1000 + id, cache::CacheBlockIndex{0}};
  record.state = state;
  record.chainId = flat::ChainId{1};
  record.blockLength = kBlockBytes;
  record.cacheGeneration = cache::CacheGeneration{1};
  if (state == cache::CacheBlockState::QUEUED || state == cache::CacheBlockState::LOADING) {
    record.chargeKind = cache::ChargeKind::RESERVED;
    record.chargedBytes = kBlockBytes;
    record.permit = permit(id);
  }
  if (state == cache::CacheBlockState::LOADING) {
    record.loaderId = Uuid::from(23, id);
    record.loadEpoch = 1;
    record.leaseExpiresAt = UtcTime::fromMicroseconds(1000);
  }
  if (state == cache::CacheBlockState::READY || state == cache::CacheBlockState::CLEANING ||
      state == cache::CacheBlockState::EVICTING) {
    record.chargeKind = cache::ChargeKind::COMMITTED;
    record.chargedBytes = kBlockBytes;
    record.ready = cache::ReadyIdentity{1, record.cacheGeneration, 1, 1234, kBlockBytes};
    record.placement = placement(id);
    record.committedPermit = permit(id);
    record.readyAt = UtcTime::fromMicroseconds(10);
    record.lastAccessAt = record.readyAt;
  }
  if (state == cache::CacheBlockState::CLEANING) {
    record.cleanupEpoch = cache::CleanupEpoch{1};
    record.deleteGeneration = record.cacheGeneration;
  }
  if (state == cache::CacheBlockState::EVICTING) {
    record.evictionEpoch = cache::EvictionEpoch{1};
    record.retireOperationId = Uuid::from(24, id);
    record.evictionReason = cache::EvictionReason::CAPACITY_WATERMARK;
  }
  return record;
}

CoTryTask<Void> seed(MockCluster &cluster, const CacheBlockRecord &record) {
  auto txn = cluster.kvEngine()->createReadWriteTransaction();
  if (record.chargeKind != cache::ChargeKind::NONE) {
    CO_RETURN_ON_ERROR(co_await CacheCapacityStore::reserve(*txn, record.chargedBytes));
    if (record.chargeKind == cache::ChargeKind::COMMITTED) {
      CO_RETURN_ON_ERROR(co_await CacheCapacityStore::commit(*txn, record.chargedBytes));
    }
  }
  CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(*txn, record));
  co_return co_await txn->commit();
}

CacheStorageEvent eventFor(const CacheBlockRecord &record,
                           uint64_t source,
                           cache::CacheStorageEventType type,
                           uint64_t sequence = 1) {
  CacheStorageEvent event;
  event.sourceId = disk(source);
  event.sequence = sequence;
  event.type = type;
  event.storageOperationId = Uuid::from(25, source);
  event.logicalKey = record.key;
  event.storageKey = {placement(source).versionedChain, storage::ChunkId{0xCA, source}};
  event.generation = record.cacheGeneration;
  event.placement = record.placement.value_or(record.permit.has_value() ? record.permit->placement : placement(source));
  event.storageTargetId = event.placement.coordinatorTargetId;
  event.diskId = disk(999);
  event.timestamp = UtcTime::fromMicroseconds(static_cast<int64_t>(source));
  if (type == cache::CacheStorageEventType::DELETED) {
    event.logicalRetireOperationId =
        record.retireOperationId == Uuid::zero() ? Uuid::from(26, source) : record.retireOperationId;
    event.evictionEpoch =
        record.evictionEpoch == cache::EvictionEpoch{} ? cache::EvictionEpoch{1} : record.evictionEpoch;
  }
  event.storageKey.vChainId = event.placement.versionedChain;
  return event;
}

CoTryTask<CacheStorageEventAck> report(MockCluster &cluster, CacheStorageEvent event) {
  ReportCacheStorageEventsReq request;
  request.events.push_back(std::move(event));
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto response = co_await cluster.meta().getOperator().reportCacheStorageEvents(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->results.front());
}

TEST_F(TestCacheStorageEvents, AppliesCompleteStateEventMatrix) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableCacheFeature(cluster);
    const std::array states{cache::CacheBlockState::READY,
                            cache::CacheBlockState::EVICTING,
                            cache::CacheBlockState::CLEANING,
                            cache::CacheBlockState::LOADING,
                            cache::CacheBlockState::QUEUED,
                            cache::CacheBlockState::INVALID,
                            cache::CacheBlockState::FAILED,
                            cache::CacheBlockState::NONE};
    uint64_t id = 1;
    size_t deadLetters = 0;
    for (auto state : states) {
      for (auto type : {cache::CacheStorageEventType::DELETED, cache::CacheStorageEventType::EMERGENCY_EVICTED}) {
        auto record = recordFor(state, id);
        if (state != cache::CacheBlockState::NONE) CO_ASSERT_OK(co_await seed(cluster, record));
        auto acknowledged = co_await report(cluster, eventFor(record, id, type));
        CO_ASSERT_OK(acknowledged);
        if (!acknowledged) co_return;

        const bool expectedDeadLetter =
            state == cache::CacheBlockState::QUEUED ||
            (state == cache::CacheBlockState::READY && type == cache::CacheStorageEventType::DELETED);
        CO_ASSERT_EQ(acknowledged->deadLettered, expectedDeadLetter);
        deadLetters += expectedDeadLetter;

        auto read = cluster.kvEngine()->createReadonlyTransaction();
        auto stored = co_await CacheBlockStore::snapshotLoad(*read, record.key);
        CO_ASSERT_OK(stored);
        if (state == cache::CacheBlockState::NONE ||
            (state == cache::CacheBlockState::EVICTING && type == cache::CacheStorageEventType::DELETED)) {
          CO_ASSERT_FALSE(stored->has_value());
        } else if (state == cache::CacheBlockState::READY) {
          CO_ASSERT_EQ((*stored)->state,
                       type == cache::CacheStorageEventType::DELETED ? cache::CacheBlockState::CLEANING
                                                                     : cache::CacheBlockState::EVICTING);
        } else if (state == cache::CacheBlockState::LOADING) {
          CO_ASSERT_EQ((*stored)->state, cache::CacheBlockState::CLEANING);
          CO_ASSERT_EQ((*stored)->terminalState, cache::CleanupTerminalState::FAILED);
          CO_ASSERT_EQ((*stored)->loaderId, record.loaderId);
        } else {
          CO_ASSERT_EQ((*stored)->state, state);
        }
        ++id;
      }
    }

    ListCacheEventDeadLettersReq list;
    list.limit = cache::kMaxPhase2BatchItems;
    list.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto listed = co_await cluster.meta().getOperator().listCacheEventDeadLetters(list);
    CO_ASSERT_OK(listed);
    CO_ASSERT_EQ(listed->items.size(), deadLetters);
    CO_ASSERT_FALSE(listed->more);

    list.sourceId = disk(9);
    list.beginSequence = 1;
    auto sourcePage = co_await cluster.meta().getOperator().listCacheEventDeadLetters(list);
    CO_ASSERT_OK(sourcePage);
    CO_ASSERT_EQ(sourcePage->items.size(), size_t{1});
    CO_ASSERT_EQ(sourcePage->items.front().event.sourceId, *list.sourceId);
  }());
}

TEST_F(TestCacheStorageEvents, EnforcesSequenceAndGenerationWithoutDoubleRelease) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableCacheFeature(cluster);
    auto ready = recordFor(cache::CacheBlockState::READY, 100);
    ready.cacheGeneration = cache::CacheGeneration{2};
    ready.ready->cacheGeneration = ready.cacheGeneration;
    CO_ASSERT_OK(co_await seed(cluster, ready));

    auto stale = eventFor(ready, 500, cache::CacheStorageEventType::EMERGENCY_EVICTED, 1);
    stale.generation = cache::CacheGeneration{1};
    auto staleAck = co_await report(cluster, stale);
    CO_ASSERT_OK(staleAck);
    CO_ASSERT_FALSE(staleAck->deadLettered);

    auto future = eventFor(ready, 500, cache::CacheStorageEventType::EMERGENCY_EVICTED, 2);
    future.generation = cache::CacheGeneration{3};
    auto futureAck = co_await report(cluster, future);
    CO_ASSERT_OK(futureAck);
    CO_ASSERT_TRUE(futureAck->deadLettered);

    auto gap = eventFor(ready, 500, cache::CacheStorageEventType::EMERGENCY_EVICTED, 4);
    CO_ASSERT_ERROR(co_await report(cluster, gap), CacheCode::kEventGap);

    auto duplicate = co_await report(cluster, stale);
    CO_ASSERT_OK(duplicate);
    CO_ASSERT_EQ(duplicate->sequence, uint64_t{2});

    auto exact = eventFor(ready, 500, cache::CacheStorageEventType::EMERGENCY_EVICTED, 3);
    CO_ASSERT_OK(co_await report(cluster, exact));
    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto stored = co_await CacheBlockStore::snapshotLoad(*read, ready.key);
    CO_ASSERT_OK(stored);
    CO_ASSERT_EQ((*stored)->state, cache::CacheBlockState::EVICTING);
  }());
}

TEST_F(TestCacheStorageEvents, ExactDeletedReleasesCommittedBytesOnce) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableCacheFeature(cluster);
    auto evicting = recordFor(cache::CacheBlockState::EVICTING, 200);
    CO_ASSERT_OK(co_await seed(cluster, evicting));
    auto wrongPlacement = eventFor(evicting, 600, cache::CacheStorageEventType::DELETED);
    wrongPlacement.placement = placement(201);
    wrongPlacement.storageKey.vChainId = wrongPlacement.placement.versionedChain;
    auto placementAck = co_await report(cluster, wrongPlacement);
    CO_ASSERT_OK(placementAck);
    CO_ASSERT_TRUE(placementAck->deadLettered);

    auto wrongOperation = eventFor(evicting, 600, cache::CacheStorageEventType::DELETED, 2);
    wrongOperation.logicalRetireOperationId = Uuid::from(99, 99);
    auto operationAck = co_await report(cluster, wrongOperation);
    CO_ASSERT_OK(operationAck);
    CO_ASSERT_TRUE(operationAck->deadLettered);

    auto beforeDelete = cluster.kvEngine()->createReadonlyTransaction();
    auto retained = co_await CacheBlockStore::snapshotLoad(*beforeDelete, evicting.key);
    CO_ASSERT_OK(retained);
    CO_ASSERT_TRUE(retained->has_value());
    auto retainedCapacity = co_await CacheCapacityStore::snapshotLoad(*beforeDelete);
    CO_ASSERT_OK(retainedCapacity);
    CO_ASSERT_EQ(retainedCapacity->committedBytes, kBlockBytes);

    auto deletion = eventFor(evicting, 600, cache::CacheStorageEventType::DELETED, 3);
    CO_ASSERT_OK(co_await report(cluster, deletion));
    CO_ASSERT_OK(co_await report(cluster, deletion));

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{0});
    CO_ASSERT_EQ(capacity->committedBytes, uint64_t{0});
    auto stored = co_await CacheBlockStore::snapshotLoad(*read, evicting.key);
    CO_ASSERT_OK(stored);
    CO_ASSERT_FALSE(stored->has_value());
  }());
}

TEST(TestCacheStorageEventWire, RequiresCompleteEventIdentity) {
  auto record = recordFor(cache::CacheBlockState::EVICTING, 300);
  auto event = eventFor(record, 700, cache::CacheStorageEventType::DELETED);
  ASSERT_OK(event.valid());
  event.logicalRetireOperationId.reset();
  ASSERT_ERROR(event.valid(), StatusCode::kInvalidArg);
  event = eventFor(record, 700, cache::CacheStorageEventType::EMERGENCY_EVICTED);
  event.evictionEpoch = cache::EvictionEpoch{1};
  ASSERT_ERROR(event.valid(), StatusCode::kInvalidArg);
}

}  // namespace
}  // namespace hf3fs::meta::server
