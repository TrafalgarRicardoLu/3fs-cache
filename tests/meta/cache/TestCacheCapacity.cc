#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

class TestCacheCapacity : public ::testing::Test {
 protected:
  kv::MemKVEngine engine_;
};

cache::CacheBlockKey capacityKey(uint32_t block) { return cache::CacheBlockKey{100, cache::CacheBlockIndex{block}}; }

TEST_F(TestCacheCapacity, EnqueueCommitAndFinishCleanChargeExactlyOnce) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    CacheBlockRecord desiredReady;
    {
      auto txn = engine_.createReadWriteTransaction();
      CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1024));
      auto record = co_await CacheBlockStore::enqueue(*txn, capacityKey(1), flat::ChainId{7}, 256);
      CO_ASSERT_OK(record);
      CO_ASSERT_EQ(record->chargeKind, cache::ChargeKind::RESERVED);
      CO_ASSERT_OK(co_await txn->commit());
    }
    {
      auto txn = engine_.createReadWriteTransaction();
      auto record = co_await CacheBlockStore::load(*txn, capacityKey(1));
      CO_ASSERT_OK(record);
      CO_ASSERT_TRUE(record->has_value());
      (*record)->state = cache::CacheBlockState::READY;
      desiredReady = **record;
      CO_ASSERT_OK(co_await CacheBlockStore::commitCharge(*txn, **record));
      CO_ASSERT_OK(co_await txn->commit());
    }
    {
      auto txn = engine_.createReadWriteTransaction();
      CO_ASSERT_OK(co_await CacheBlockStore::commitCharge(*txn, desiredReady));
      CO_ASSERT_OK(co_await txn->commit());
    }
    {
      auto txn = engine_.createReadWriteTransaction();
      auto record = co_await CacheBlockStore::load(*txn, capacityKey(1));
      CO_ASSERT_OK(record);
      (*record)->state = cache::CacheBlockState::CLEANING;
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, **record));
      CO_ASSERT_OK(co_await CacheBlockStore::finishClean(*txn, capacityKey(1), cache::CleanupTerminalState::NONE));
      CO_ASSERT_OK(co_await txn->commit());
    }

    auto read = engine_.createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{0});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{0});
    CO_ASSERT_EQ(capacity->committedBytes, uint64_t{0});
    auto record = co_await CacheBlockStore::snapshotLoad(*read, capacityKey(1));
    CO_ASSERT_OK(record);
    CO_ASSERT_FALSE(record->has_value());
  }());
}

TEST_F(TestCacheCapacity, ReferenceCapacityDoesNotLimitReservationsOrReenqueueAccounting) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*txn, 100));
    CO_ASSERT_OK(co_await txn->commit());

    for (uint32_t block = 0; block < 100; ++block) {
      txn = engine_.createReadWriteTransaction();
      auto result = co_await CacheBlockStore::enqueue(*txn, capacityKey(block), flat::ChainId{3}, 2);
      CO_ASSERT_OK(result);
      CO_ASSERT_OK(co_await txn->commit());
    }

    txn = engine_.createReadWriteTransaction();
    auto record = co_await CacheBlockStore::load(*txn, capacityKey(0));
    CO_ASSERT_OK(record);
    (*record)->state = cache::CacheBlockState::CLEANING;
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, **record));
    CO_ASSERT_OK(co_await CacheBlockStore::finishClean(*txn, capacityKey(0), cache::CleanupTerminalState::REENQUEUE));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->logicalCapacity, uint64_t{100});
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{200});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{200});
    CO_ASSERT_EQ(capacity->committedBytes, uint64_t{0});

    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1));
    CO_ASSERT_OK(co_await txn->commit());
    read = engine_.createReadonlyTransaction();
    capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->logicalCapacity, uint64_t{1});
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{200});
  }());
}

TEST_F(TestCacheCapacity, ConcurrentReservationsPreserveExactCountersAboveReference) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto setup = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*setup, 100));
    CO_ASSERT_OK(co_await setup->commit());

    auto first = engine_.createReadWriteTransaction();
    auto second = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*first, capacityKey(200), flat::ChainId{3}, 60));
    CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*second, capacityKey(201), flat::ChainId{3}, 60));
    CO_ASSERT_OK(co_await first->commit());
    CO_ASSERT_ERROR(co_await second->commit(), TransactionCode::kConflict);

    auto retry = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*retry, capacityKey(201), flat::ChainId{3}, 60));
    CO_ASSERT_OK(co_await retry->commit());

    auto idempotent = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*idempotent, capacityKey(200), flat::ChainId{3}, 60));
    CO_ASSERT_OK(co_await idempotent->commit());

    auto read = engine_.createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->logicalCapacity, uint64_t{100});
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{120});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{120});
  }());
}

TEST_F(TestCacheCapacity, LogicalCounterOverflowStillFailsClosed) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1));
    CO_ASSERT_OK(co_await CacheCapacityStore::reserve(*txn, std::numeric_limits<uint64_t>::max()));
    CO_ASSERT_ERROR(co_await CacheCapacityStore::reserve(*txn, 1), CacheCode::kCapacityExceeded);
  }());
}

TEST_F(TestCacheCapacity, ReservedCleaningReleasesOnceIntoFailed) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*txn, 128));
    auto record = co_await CacheBlockStore::enqueue(*txn, capacityKey(300), flat::ChainId{4}, 32);
    CO_ASSERT_OK(record);
    record->state = cache::CacheBlockState::LOADING;
    record->loaderId = Uuid::random();
    record->loadEpoch = 1;
    record->cacheGeneration = cache::CacheGeneration{1};
    record->leaseExpiresAt = UtcClock::now() + (1_min).asUs();
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, *record));
    record->state = cache::CacheBlockState::CLEANING;
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, *record));
    CO_ASSERT_OK(co_await CacheBlockStore::finishClean(*txn, capacityKey(300), cache::CleanupTerminalState::FAILED));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::finishClean(*txn, capacityKey(300), cache::CleanupTerminalState::FAILED));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{0});
    auto failed = co_await CacheBlockStore::snapshotLoad(*read, capacityKey(300));
    CO_ASSERT_OK(failed);
    CO_ASSERT_TRUE(failed->has_value());
    CO_ASSERT_EQ((*failed)->state, cache::CacheBlockState::FAILED);
    CO_ASSERT_EQ((*failed)->chargeKind, cache::ChargeKind::NONE);
  }());
}

TEST_F(TestCacheCapacity, ListsBlocksWithStablePaginationAndOptionalInodeFilter) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto setup = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheCapacityStore::setLogicalCapacity(*setup, 1024));
    for (uint32_t block = 0; block < 3; ++block) {
      CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*setup,
                                                     cache::CacheBlockKey{100, cache::CacheBlockIndex{block}},
                                                     flat::ChainId{3},
                                                     16));
    }
    CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*setup,
                                                   cache::CacheBlockKey{101, cache::CacheBlockIndex{0}},
                                                   flat::ChainId{3},
                                                   16));
    CO_ASSERT_OK(co_await setup->commit());

    auto read = engine_.createReadonlyTransaction();
    auto first = co_await CacheBlockStore::snapshotList(*read, uint64_t{100}, cache::CacheBlockIndex{1}, 1);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->records.size(), size_t{1});
    CO_ASSERT_EQ(first->records.front().key.block, cache::CacheBlockIndex{1});
    CO_ASSERT_TRUE(first->more);

    auto second = co_await CacheBlockStore::snapshotList(*read, uint64_t{100}, cache::CacheBlockIndex{2}, 2);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->records.size(), size_t{1});
    CO_ASSERT_EQ(second->records.front().key.block, cache::CacheBlockIndex{2});
    CO_ASSERT_FALSE(second->more);

    auto filtered = co_await CacheBlockStore::snapshotListAll(*read, uint64_t{100});
    CO_ASSERT_OK(filtered);
    CO_ASSERT_EQ(filtered->size(), size_t{3});
    auto all = co_await CacheBlockStore::snapshotListAll(*read);
    CO_ASSERT_OK(all);
    CO_ASSERT_EQ(all->size(), size_t{4});
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
