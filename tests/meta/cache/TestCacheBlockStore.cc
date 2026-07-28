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

}  // namespace
}  // namespace hf3fs::meta::server
