#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/OrchestrationKey.h"
#include "meta/store/cache/PinStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

class TestPinStore : public ::testing::Test {
 protected:
  kv::MemKVEngine engine_;
};

cache::CacheBlockKey block(uint64_t inode, uint32_t index) {
  return cache::CacheBlockKey{inode, cache::CacheBlockIndex{index}};
}

cache::PinOwner owner(uint64_t id) {
  return cache::PinOwner{cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerId{Uuid::from(0, id)}};
}

cache::PinRecord pin(cache::CacheBlockKey key, cache::PinOwner pinOwner, uint64_t expiresAt = 200) {
  return cache::PinRecord{key, pinOwner, 100, expiresAt, cache::CacheGeneration{}};
}

TEST_F(TestPinStore, OverlappingOwnersAndTtlAreIndependent) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto key = block(1, 2);
    auto first = pin(key, owner(1), 150);
    auto second = pin(key, owner(2), 250);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, first));
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, second));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto atStart = co_await PinStore::snapshotQueryActive(*read, key, 125);
    CO_ASSERT_OK(atStart);
    CO_ASSERT_EQ(atStart->size(), size_t{2});
    auto later = co_await PinStore::snapshotQueryActive(*read, key, 175);
    CO_ASSERT_OK(later);
    CO_ASSERT_EQ(later->size(), size_t{1});
    CO_ASSERT_EQ(later->front(), second);
    auto expired = co_await PinStore::snapshotQueryActive(*read, key, 250);
    CO_ASSERT_OK(expired);
    CO_ASSERT_TRUE(expired->empty());
  }());
}

TEST_F(TestPinStore, RenewalAndRemovalAreIdempotent) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto initial = pin(block(2, 3), owner(3));
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, initial));
    CO_ASSERT_OK(co_await txn->commit());

    auto renewed = initial;
    renewed.expiresAtMs = 300;
    renewed.cacheGeneration = cache::CacheGeneration{4};
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, renewed));
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, renewed));
    CO_ASSERT_OK(co_await txn->commit());

    auto backwards = renewed;
    backwards.expiresAtMs = 250;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PinStore::upsert(*txn, backwards), CacheCode::kStateConflict);

    txn = engine_.createReadWriteTransaction();
    auto removed = co_await PinStore::remove(*txn, renewed.key, renewed.owner);
    CO_ASSERT_OK(removed);
    CO_ASSERT_TRUE(*removed);
    CO_ASSERT_OK(co_await txn->commit());
    txn = engine_.createReadWriteTransaction();
    removed = co_await PinStore::remove(*txn, renewed.key, renewed.owner);
    CO_ASSERT_OK(removed);
    CO_ASSERT_FALSE(*removed);
  }());
}

TEST_F(TestPinStore, OwnerLeaseRenewsAllPinsWithoutRewritingThem) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto pinOwner = owner(30);
    auto record = pin(block(30, 0), pinOwner, 150);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, record));
    auto created = co_await PinStore::renewOwnerLease(*txn, {pinOwner, 100, 300});
    CO_ASSERT_OK(created);
    CO_ASSERT_TRUE(created->created);
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto active = co_await PinStore::snapshotQueryActive(*read, record.key, 250);
    CO_ASSERT_OK(active);
    CO_ASSERT_EQ(active->size(), size_t{1});

    txn = engine_.createReadWriteTransaction();
    auto renewed = co_await PinStore::renewOwnerLease(*txn, {pinOwner, 100, 400});
    CO_ASSERT_OK(renewed);
    CO_ASSERT_FALSE(renewed->created);
    CO_ASSERT_OK(co_await txn->commit());
    read = engine_.createReadonlyTransaction();
    active = co_await PinStore::snapshotQueryActive(*read, record.key, 350);
    CO_ASSERT_OK(active);
    CO_ASSERT_EQ(active->size(), size_t{1});

    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::removeByOwner(*txn, pinOwner));
    CO_ASSERT_OK(co_await txn->commit());
    read = engine_.createReadonlyTransaction();
    active = co_await PinStore::snapshotQueryActive(*read, record.key, 350);
    CO_ASSERT_OK(active);
    CO_ASSERT_TRUE(active->empty());
  }());
}

TEST_F(TestPinStore, OwnerPaginationAndRemoveAllUseStableBlockOrder) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto pinOwner = owner(4);
    std::vector records{pin(block(9, 2), pinOwner), pin(block(9, 0), pinOwner), pin(block(9, 1), pinOwner)};
    auto txn = engine_.createReadWriteTransaction();
    for (const auto &record : records) CO_ASSERT_OK(co_await PinStore::upsert(*txn, record));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto first = co_await PinStore::snapshotListByOwner(*read, pinOwner, std::nullopt, 2);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->pins.size(), size_t{2});
    CO_ASSERT_TRUE(first->more);
    CO_ASSERT_EQ(first->pins[0].key.block, cache::CacheBlockIndex{0});
    CO_ASSERT_EQ(first->pins[1].key.block, cache::CacheBlockIndex{1});
    auto second = co_await PinStore::snapshotListByOwner(*read, pinOwner, first->pins.back().key, 2);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->pins.size(), size_t{1});
    CO_ASSERT_FALSE(second->more);

    txn = engine_.createReadWriteTransaction();
    auto removed = co_await PinStore::removeByOwner(*txn, pinOwner);
    CO_ASSERT_OK(removed);
    CO_ASSERT_EQ(*removed, uint64_t{3});
    CO_ASSERT_OK(co_await txn->commit());
  }());
}

TEST_F(TestPinStore, OwnershipLimitsFenceNewIdentities) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    PinStoreLimits limits{1, 1};
    auto first = pin(block(5, 0), owner(5));
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, first, limits));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PinStore::upsert(*txn, pin(first.key, owner(6)), limits), CacheCode::kRequestTooLarge);
    CO_ASSERT_ERROR(co_await PinStore::upsert(*txn, pin(block(5, 1), first.owner), limits),
                    CacheCode::kRequestTooLarge);
    auto renewal = first;
    renewal.expiresAtMs = 300;
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, renewal, limits));
  }());
}

TEST_F(TestPinStore, ExpiredPinsDoNotConsumeLimitsAndCanBeRecreated) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    PinStoreLimits limits{1, 1};
    auto expired = pin(block(7, 0), owner(8), 150);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, expired, limits));
    CO_ASSERT_OK(co_await txn->commit());

    auto replacement = pin(block(7, 1), expired.owner, 300);
    replacement.createdAtMs = 200;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, replacement, limits));
    CO_ASSERT_OK(co_await txn->commit());

    auto recreated = expired;
    recreated.createdAtMs = 200;
    recreated.expiresAtMs = 300;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PinStore::upsert(*txn, recreated, limits), CacheCode::kRequestTooLarge);
    CO_ASSERT_OK(co_await PinStore::remove(*txn, replacement.key, replacement.owner));
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, recreated, limits));
    CO_ASSERT_OK(co_await txn->commit());
  }());
}

TEST_F(TestPinStore, MissingOrDifferentCounterpartFailsClosed) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto record = pin(block(6, 0), owner(7));
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, record));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await txn->clear(OrchestrationKey::pinByOwner(record.owner, record.key)));
    CO_ASSERT_OK(co_await txn->commit());
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PinStore::upsert(*txn, record), StatusCode::kDataCorruption);
    CO_ASSERT_ERROR(co_await PinStore::remove(*txn, record.key, record.owner), StatusCode::kDataCorruption);

    auto read = engine_.createReadonlyTransaction();
    CO_ASSERT_ERROR(co_await PinStore::snapshotQueryActive(*read, record.key, 150), StatusCode::kDataCorruption);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
