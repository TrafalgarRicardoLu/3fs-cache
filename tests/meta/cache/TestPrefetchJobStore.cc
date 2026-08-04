#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/OrchestrationKey.h"
#include "meta/store/cache/PrefetchJobStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

class TestPrefetchJobStore : public ::testing::Test {
 protected:
  kv::MemKVEngine engine_;
};

cache::PrefetchJobRecord job(uint64_t id, uint32_t owner = 1) {
  cache::PrefetchJobRecord record;
  record.spec.jobId = cache::PrefetchJobId{Uuid::from(0, id)};
  record.spec.ownerUid = flat::Uid{owner};
  record.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  record.state = cache::PrefetchJobState::PENDING;
  record.stateVersion = 1;
  record.createdAtMs = 100;
  record.updatedAtMs = 100;
  return record;
}

TEST_F(TestPrefetchJobStore, CreateIsIdempotentAfterAmbiguousRetryAndRejectsSpecConflict) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = job(1);
    auto txn = engine_.createReadWriteTransaction();
    auto created = co_await PrefetchJobStore::create(*txn, desired);
    CO_ASSERT_OK(created);
    CO_ASSERT_TRUE(created->created);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    auto retry = co_await PrefetchJobStore::create(*txn, desired);
    CO_ASSERT_OK(retry);
    CO_ASSERT_FALSE(retry->created);
    CO_ASSERT_EQ(retry->job, desired);

    desired.spec.priority = 10;
    CO_ASSERT_ERROR(co_await PrefetchJobStore::create(*txn, desired), CacheCode::kStateConflict);
  }());
}

TEST_F(TestPrefetchJobStore, StateVersionProvidesCasAndTransactionConflictFence) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto setup = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*setup, job(2)));
    CO_ASSERT_OK(co_await setup->commit());

    auto first = engine_.createReadWriteTransaction();
    auto second = engine_.createReadWriteTransaction();
    auto desired = job(2);
    desired.state = cache::PrefetchJobState::PLANNING;
    desired.stateVersion = 2;
    desired.updatedAtMs = 101;
    CO_ASSERT_OK(co_await PrefetchJobStore::update(*first, 1, desired));
    CO_ASSERT_OK(co_await PrefetchJobStore::update(*second, 1, desired));
    CO_ASSERT_OK(co_await first->commit());
    CO_ASSERT_ERROR(co_await second->commit(), TransactionCode::kConflict);

    auto stale = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PrefetchJobStore::update(*stale, 1, desired), CacheCode::kStateConflict);
  }());
}

TEST_F(TestPrefetchJobStore, ListsWithStableCursorAndOwnerFilter) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    std::vector<cache::PrefetchJobRecord> records{job(11, 1), job(12, 2), job(13, 1)};
    std::sort(records.begin(), records.end(), [](const auto &left, const auto &right) {
      return OrchestrationKey::job(left.spec.jobId) < OrchestrationKey::job(right.spec.jobId);
    });
    auto txn = engine_.createReadWriteTransaction();
    for (const auto &record : records) CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, record));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto first = co_await PrefetchJobStore::snapshotList(*read, std::nullopt, std::nullopt, 2);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->jobs.size(), size_t{2});
    CO_ASSERT_TRUE(first->more);
    auto second = co_await PrefetchJobStore::snapshotList(*read, std::nullopt, first->jobs.back().spec.jobId, 2);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->jobs.size(), size_t{1});
    CO_ASSERT_FALSE(second->more);

    auto owned = co_await PrefetchJobStore::snapshotList(*read, flat::Uid{1}, std::nullopt, 2);
    CO_ASSERT_OK(owned);
    CO_ASSERT_EQ(owned->jobs.size(), size_t{2});
    CO_ASSERT_FALSE(owned->more);
    for (const auto &record : owned->jobs) CO_ASSERT_EQ(record.spec.ownerUid, flat::Uid{1});
  }());
}

TEST_F(TestPrefetchJobStore, TerminalStateCannotBeReopened) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, job(3)));
    CO_ASSERT_OK(co_await txn->commit());

    auto terminal = job(3);
    terminal.state = cache::PrefetchJobState::READY;
    terminal.stateVersion = 2;
    terminal.updatedAtMs = 101;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::update(*txn, 1, terminal));
    CO_ASSERT_OK(co_await txn->commit());

    auto reopened = terminal;
    reopened.state = cache::PrefetchJobState::LOADING;
    reopened.stateVersion = 3;
    reopened.updatedAtMs = 102;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PrefetchJobStore::update(*txn, 2, reopened), CacheCode::kStateConflict);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
