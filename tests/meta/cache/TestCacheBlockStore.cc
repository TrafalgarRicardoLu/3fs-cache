#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "common/kv/mem/MemKVEngine.h"
#include "common/serde/Serde.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

class TestCacheBlockStore : public ::testing::Test {
 protected:
  kv::MemKVEngine engine_;
};

cache::CacheBlockKey key(uint64_t inode, uint32_t block) {
  return cache::CacheBlockKey{inode, cache::CacheBlockIndex{block}};
}

storage::PermitIdentity permit() {
  auto placement = storage::PlacementIdentity::create({flat::ChainId{1}, flat::ChainVersion{1}},
                                                      {flat::TargetId{1}},
                                                      flat::TargetId{1},
                                                      Uuid::from(1, 2));
  return storage::PermitIdentity{Uuid::from(3, 4), *placement, 1, {{flat::TargetId{1}, 4096}}};
}

TEST_F(TestCacheBlockStore, NoneIsNotPersistedAndFailedHasNoCharge) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto txn = engine_.createReadWriteTransaction();
    CacheBlockRecord none;
    none.key = key(1, 2);
    CO_ASSERT_ERROR(co_await CacheBlockStore::store(*txn, none), StatusCode::kInvalidArg);

    CacheBlockRecord failed;
    failed.key = key(1, 2);
    failed.state = cache::CacheBlockState::FAILED;
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, failed));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto loaded = co_await CacheBlockStore::snapshotLoad(*read, failed.key);
    CO_ASSERT_OK(loaded);
    CO_ASSERT_TRUE(loaded->has_value());
    CO_ASSERT_EQ((*loaded)->chargeKind, cache::ChargeKind::NONE);
    CO_ASSERT_EQ((*loaded)->chargedBytes, uint64_t{0});
  }());
}

TEST_F(TestCacheBlockStore, GenerationSurvivesRecordRemovalAndRejectsOverflow) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto block = key(8, 9);
    for (uint64_t expected = 1; expected <= 2; ++expected) {
      auto txn = engine_.createReadWriteTransaction();
      auto generation = co_await CacheBlockStore::allocateGeneration(*txn, block);
      CO_ASSERT_OK(generation);
      CO_ASSERT_EQ(generation->toUnderType(), expected);
      CO_ASSERT_OK(co_await CacheBlockStore::remove(*txn, block));
      CO_ASSERT_OK(co_await txn->commit());
    }

    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await txn->set(CacheBlockStore::generationKey(block),
                                   serde::serialize(cache::CacheGeneration{std::numeric_limits<uint64_t>::max()})));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await CacheBlockStore::allocateGeneration(*txn, block), CacheCode::kStateConflict);
  }());
}

TEST_F(TestCacheBlockStore, ValidatesPhase2PermitAndPlacementStateMatrix) {
  CacheBlockRecord record;
  record.key = key(10, 1);
  record.state = cache::CacheBlockState::QUEUED;
  record.chainId = flat::ChainId{1};
  record.blockLength = 4096;
  record.chargeKind = cache::ChargeKind::RESERVED;
  record.chargedBytes = 4096;
  record.permit = permit();
  ASSERT_OK(record.valid());

  record.placement = record.permit->placement;
  ASSERT_ERROR(record.valid(), StatusCode::kInvalidArg);
  record.placement.reset();

  record.state = cache::CacheBlockState::READY;
  record.ready = cache::ReadyIdentity{1, cache::CacheGeneration{1}, 1, 1234, 4096};
  record.chargeKind = cache::ChargeKind::COMMITTED;
  record.placement = record.permit->placement;
  record.committedPermit = record.permit;
  record.permit.reset();
  ASSERT_OK(record.valid());

  record.placement.reset();
  ASSERT_ERROR(record.valid(), StatusCode::kInvalidArg);
  record.placement = record.committedPermit->placement;
  record.state = cache::CacheBlockState::FAILED;
  record.ready.reset();
  record.chargeKind = cache::ChargeKind::NONE;
  record.chargedBytes = 0;
  ASSERT_ERROR(record.valid(), StatusCode::kInvalidArg);
}

TEST_F(TestCacheBlockStore, PagesOnlyRecoverableCacheLoadsInStableOrder) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto storeRecord = [&](CacheBlockRecord record) -> CoTask<void> {
      auto txn = engine_.createReadWriteTransaction();
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, record));
      CO_ASSERT_OK(co_await txn->commit());
    };

    CacheBlockRecord failed;
    failed.key = key(1, 0);
    failed.state = cache::CacheBlockState::FAILED;
    co_await storeRecord(failed);

    CacheBlockRecord queued;
    queued.key = key(2, 0);
    queued.state = cache::CacheBlockState::QUEUED;
    queued.chainId = flat::ChainId{1};
    queued.blockLength = 4096;
    queued.chargeKind = cache::ChargeKind::RESERVED;
    queued.chargedBytes = 4096;
    queued.permit = permit();
    co_await storeRecord(queued);

    CacheBlockRecord loading = queued;
    loading.key = key(3, 0);
    loading.state = cache::CacheBlockState::LOADING;
    loading.loaderId = Uuid::from(5, 6);
    loading.loadEpoch = 1;
    loading.cacheGeneration = cache::CacheGeneration{1};
    loading.leaseExpiresAt = UtcTime::fromMicroseconds(100);
    co_await storeRecord(loading);

    auto read = engine_.createReadonlyTransaction();
    auto first = co_await CacheBlockStore::snapshotListRecoverable(*read, std::nullopt, 1);
    CO_ASSERT_OK(first);
    CO_ASSERT_TRUE(first->more);
    CO_ASSERT_EQ(first->records.size(), size_t{1});
    CO_ASSERT_EQ(first->records.front().key, queued.key);

    auto second = co_await CacheBlockStore::snapshotListRecoverable(*read, first->records.front().key, 1);
    CO_ASSERT_OK(second);
    CO_ASSERT_FALSE(second->more);
    CO_ASSERT_EQ(second->records.size(), size_t{1});
    CO_ASSERT_EQ(second->records.front().key, loading.key);
    CO_ASSERT_EQ(second->records.front().leaseExpiresAt, loading.leaseExpiresAt);
  }());
}

TEST_F(TestCacheBlockStore, ReconcileBackfillsStateIndexAndCommitsMigrationMarker) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    CacheBlockRecord legacy;
    legacy.key = key(9, 1);
    legacy.state = cache::CacheBlockState::FAILED;
    auto setup = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await setup->set(CacheBlockStore::recordKey(legacy.key), serde::serialize(legacy)));
    CO_ASSERT_OK(co_await setup->commit());

    auto before = engine_.createReadonlyTransaction();
    auto ready = co_await CacheBlockStore::snapshotStateIndexReady(*before);
    CO_ASSERT_OK(ready);
    CO_ASSERT_FALSE(*ready);

    auto migrate = engine_.createReadWriteTransaction();
    auto reconciled = co_await CacheBlockStore::snapshotListReconcile(*migrate, std::nullopt, 10);
    CO_ASSERT_OK(reconciled);
    CO_ASSERT_TRUE(reconciled->records.empty());
    CO_ASSERT_OK(co_await migrate->commit());

    auto after = engine_.createReadonlyTransaction();
    ready = co_await CacheBlockStore::snapshotStateIndexReady(*after);
    CO_ASSERT_OK(ready);
    CO_ASSERT_TRUE(*ready);
    auto failed = co_await CacheBlockStore::snapshotListState(
        *after, cache::CacheBlockState::FAILED, std::nullopt, 10);
    CO_ASSERT_OK(failed);
    CO_ASSERT_EQ(failed->records.size(), size_t{1});
    CO_ASSERT_EQ(failed->records.front().key, legacy.key);
    CO_ASSERT_EQ(failed->records.front().state, legacy.state);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
