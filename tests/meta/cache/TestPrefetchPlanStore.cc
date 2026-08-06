#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/PinStore.h"
#include "meta/store/cache/PrefetchJobStore.h"
#include "meta/store/cache/PrefetchPlanStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

class TestPrefetchPlanStore : public ::testing::Test {
 protected:
  kv::MemKVEngine engine_;
};

cache::PrefetchJobRecord planJob(uint64_t id) {
  cache::PrefetchJobRecord record;
  record.spec.jobId = cache::PrefetchJobId{Uuid::from(0, id)};
  record.spec.ownerUid = flat::Uid{1};
  record.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  record.state = cache::PrefetchJobState::PENDING;
  record.stateVersion = 1;
  record.createdAtMs = 100;
  record.updatedAtMs = 100;
  return record;
}

cache::PrefetchPlanEntry entry(cache::PrefetchJobId jobId, uint32_t block, uint64_t length = 4096) {
  return cache::PrefetchPlanEntry{jobId,
                                  cache::CacheBlockKey{10, cache::CacheBlockIndex{block}},
                                  length,
                                  1,
                                  cache::PrefetchPlanEntryState::PLANNED,
                                  Uuid::zero()};
}

TEST_F(TestPrefetchPlanStore, RepeatedAndOverlappingPagesCountEachBlockOnce) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(1);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    CO_ASSERT_OK(co_await txn->commit());

    std::vector first{entry(desired.spec.jobId, 0), entry(desired.spec.jobId, 1)};
    txn = engine_.createReadWriteTransaction();
    auto appended = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, first, 0, "page-1", false);
    CO_ASSERT_OK(appended);
    CO_ASSERT_EQ(appended->insertedBlocks, uint64_t{2});
    CO_ASSERT_EQ(appended->insertedBytes, uint64_t{8192});
    CO_ASSERT_OK(co_await txn->commit());

    std::vector overlap{first.back(), entry(desired.spec.jobId, 2, 123)};
    txn = engine_.createReadWriteTransaction();
    appended = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, overlap, 1, "done", true);
    CO_ASSERT_OK(appended);
    CO_ASSERT_EQ(appended->insertedBlocks, uint64_t{1});
    CO_ASSERT_EQ(appended->insertedBytes, uint64_t{123});
    CO_ASSERT_EQ(appended->job.plannedBlocks, uint64_t{3});
    CO_ASSERT_EQ(appended->job.plannedBytes, uint64_t{8315});
    CO_ASSERT_TRUE(appended->job.planningComplete);
    CO_ASSERT_EQ(appended->job.plannerSourceIndex, uint32_t{1});
    CO_ASSERT_EQ(appended->job.plannerCursor, "done");
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    auto retry = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, overlap, 1, "done", true);
    CO_ASSERT_OK(retry);
    CO_ASSERT_EQ(retry->insertedBlocks, uint64_t{0});
    CO_ASSERT_EQ(retry->job.stateVersion, appended->job.stateVersion);
  }());
}

TEST_F(TestPrefetchPlanStore, ExactRetryDoesNotAdvanceCountersOrVersion) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(2);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    CO_ASSERT_OK(co_await txn->commit());
    std::vector page{entry(desired.spec.jobId, 0)};

    txn = engine_.createReadWriteTransaction();
    auto first = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, page, 0, "next", false);
    CO_ASSERT_OK(first);
    CO_ASSERT_OK(co_await txn->commit());
    txn = engine_.createReadWriteTransaction();
    auto retry = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, page, 0, "next", false);
    CO_ASSERT_OK(retry);
    CO_ASSERT_EQ(retry->insertedBlocks, uint64_t{0});
    CO_ASSERT_EQ(retry->job.stateVersion, first->job.stateVersion);
  }());
}

TEST_F(TestPrefetchPlanStore, ListsInStableBlockOrderAndRejectsChangedDuplicate) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(3);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    std::vector entries{entry(desired.spec.jobId, 0), entry(desired.spec.jobId, 1), entry(desired.spec.jobId, 2)};
    CO_ASSERT_OK(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, entries, 0, "next", false));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto first = co_await PrefetchPlanStore::snapshotList(*read, desired.spec.jobId, std::nullopt, 2);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->entries.size(), size_t{2});
    CO_ASSERT_TRUE(first->more);
    auto second = co_await PrefetchPlanStore::snapshotList(*read, desired.spec.jobId, first->entries.back().key, 2);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->entries.size(), size_t{1});
    CO_ASSERT_FALSE(second->more);

    entries.front().blockLength = 1;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, entries, 0, "next", false),
                    CacheCode::kStateConflict);
  }());
}

TEST_F(TestPrefetchPlanStore, CounterOverflowFailsBeforeCommit) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(4);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    CO_ASSERT_OK(co_await txn->commit());

    desired.state = cache::PrefetchJobState::PLANNING;
    desired.stateVersion = 2;
    desired.updatedAtMs = 101;
    desired.plannedBytes = std::numeric_limits<uint64_t>::max();
    desired.plannedBlocks = 1;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::update(*txn, 1, desired));
    CO_ASSERT_OK(co_await txn->commit());

    std::vector page{entry(desired.spec.jobId, 0)};
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, page, 0, "next", false),
                    CacheCode::kStateConflict);
  }());
}

TEST_F(TestPrefetchPlanStore, EmptyFinalPagePersistsCompletionCursor) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(5);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    std::vector<cache::PrefetchPlanEntry> empty;
    auto completed = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, empty, 1, "empty", true);
    CO_ASSERT_OK(completed);
    CO_ASSERT_EQ(completed->insertedBlocks, uint64_t{0});
    CO_ASSERT_TRUE(completed->job.planningComplete);
    CO_ASSERT_EQ(completed->job.plannedBlocks, uint64_t{0});
    CO_ASSERT_OK(co_await txn->commit());
  }());
}

TEST_F(TestPrefetchPlanStore, EmptySourceCanAdvanceToNextSource) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(7);
    desired.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/second", true}});
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    std::vector<cache::PrefetchPlanEntry> empty;
    auto advanced = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, empty, 1, "", false);
    CO_ASSERT_OK(advanced);
    CO_ASSERT_EQ(advanced->job.plannerSourceIndex, 1u);
    CO_ASSERT_FALSE(advanced->job.planningComplete);
    CO_ASSERT_OK(co_await txn->commit());
  }());
}

TEST_F(TestPrefetchPlanStore, CreatesActiveJobPinsAtomicallyWithPlanEntries) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(8);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    std::vector page{entry(desired.spec.jobId, 0)};
    CO_ASSERT_OK(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, page, 0, "next", false, 500));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    cache::PinOwner owner{cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerId{desired.spec.jobId.toUnderType()}};
    auto pins = co_await PinStore::snapshotListByOwner(*read, owner, std::nullopt, 10);
    CO_ASSERT_OK(pins);
    CO_ASSERT_EQ(pins->pins.size(), size_t{1});
    CO_ASSERT_EQ(pins->pins.front().createdAtMs, desired.createdAtMs);
    CO_ASSERT_EQ(pins->pins.front().expiresAtMs, uint64_t{500});
  }());
}

TEST_F(TestPrefetchPlanStore, CasUpdatesAdmissionIdentityAndRejectsStaleOrIllegalTransitions) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = planJob(6);
    auto planned = entry(desired.spec.jobId, 0);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    std::vector plan{planned};
    CO_ASSERT_OK(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, plan, 1, "done", true));
    CO_ASSERT_OK(co_await txn->commit());

    auto admitted = planned;
    admitted.state = cache::PrefetchPlanEntryState::ADMITTED;
    admitted.admissionAttemptId = Uuid::from(1, 2);
    txn = engine_.createReadWriteTransaction();
    auto updated = co_await PrefetchPlanStore::update(*txn, planned, admitted);
    CO_ASSERT_OK(updated);
    CO_ASSERT_EQ(*updated, admitted);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    updated = co_await PrefetchPlanStore::update(*txn, planned, admitted);
    CO_ASSERT_OK(updated);
    CO_ASSERT_EQ(*updated, admitted);
    auto stale = admitted;
    stale.state = cache::PrefetchPlanEntryState::ATTACHED;
    CO_ASSERT_ERROR(co_await PrefetchPlanStore::update(*txn, planned, stale), CacheCode::kStateConflict);

    auto changedIdentity = admitted;
    changedIdentity.blockLength++;
    CO_ASSERT_ERROR(co_await PrefetchPlanStore::update(*txn, admitted, changedIdentity), StatusCode::kInvalidArg);
    auto backwards = admitted;
    backwards.state = cache::PrefetchPlanEntryState::PLANNED;
    backwards.admissionAttemptId = Uuid::zero();
    CO_ASSERT_ERROR(co_await PrefetchPlanStore::update(*txn, admitted, backwards), StatusCode::kInvalidArg);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
