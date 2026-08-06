#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/PrefetchJobStateMachine.h"
#include "meta/store/cache/PrefetchPlanStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

cache::PrefetchJobRecord job(uint64_t id, uint32_t requiredReadyBps) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(2, id)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.spec.requiredReadyBps = requiredReadyBps;
  result.state = cache::PrefetchJobState::PENDING;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 1;
  return result;
}

CoTryTask<cache::PrefetchJobRecord> persist(kv::MemKVEngine &engine,
                                            cache::PrefetchJobRecord desired,
                                            uint64_t plannedBytes,
                                            uint64_t readyBytes) {
  auto txn = engine.createReadWriteTransaction();
  CO_RETURN_ON_ERROR(co_await PrefetchJobStore::create(*txn, desired));
  std::vector<cache::PrefetchPlanEntry> entries;
  if (plannedBytes != 0) {
    entries.push_back({desired.spec.jobId,
                       {10, cache::CacheBlockIndex{0}},
                       plannedBytes,
                       1,
                       cache::PrefetchPlanEntryState::PLANNED,
                       Uuid::zero()});
  }
  auto appended = co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, entries, 1, "", true);
  CO_RETURN_ON_ERROR(appended);
  CO_RETURN_ON_ERROR(co_await txn->commit());
  auto current = appended->job;
  if (readyBytes == 0) co_return current;
  current.readyBytes = readyBytes;
  current.readyBlocks = 1;
  ++current.stateVersion;
  ++current.updatedAtMs;
  txn = engine.createReadWriteTransaction();
  auto updated = co_await PrefetchJobStore::update(*txn, current.stateVersion - 1, current);
  CO_RETURN_ON_ERROR(updated);
  CO_RETURN_ON_ERROR(co_await txn->commit());
  co_return std::move(*updated);
}

TEST(TestPrefetchJobStateMachine, UsesExactIntegerBasisPoints) {
  EXPECT_EQ(prefetchReadyRatioBps(0, 1), 0u);
  EXPECT_EQ(prefetchReadyRatioBps(1, 10'000), 1u);
  EXPECT_EQ(prefetchReadyRatioBps(9'999, 10'000), 9'999u);
  EXPECT_EQ(prefetchReadyRatioBps(10'000, 10'000), 10'000u);
  EXPECT_TRUE(meetsPrefetchReadyRequirement(1, 1, 10'000));
  EXPECT_FALSE(meetsPrefetchReadyRequirement(9'999, 10'000, 10'000));
  EXPECT_TRUE(meetsPrefetchReadyRequirement(9'999, 10'000, 9'999));
  EXPECT_FALSE(meetsPrefetchReadyRequirement(0, 0, 1));
}

TEST(TestPrefetchJobStateMachine, AdvancesLoadingPartialAndReadyAtByteBoundaries) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    for (const auto &[id, planned, ready, required, expected] :
         std::vector<std::tuple<uint64_t, uint64_t, uint64_t, uint32_t, cache::PrefetchJobState>>{
             {1, 10'000, 0, 10'000, cache::PrefetchJobState::LOADING},
             {2, 10'000, 1, 10'000, cache::PrefetchJobState::PARTIAL_READY},
             {3, 10'000, 9'999, 9'999, cache::PrefetchJobState::READY},
             {4, 1, 1, 10'000, cache::PrefetchJobState::READY}}) {
      kv::MemKVEngine engine;
      auto current = co_await persist(engine, job(id, required), planned, ready);
      CO_ASSERT_OK(current);
      auto txn = engine.createReadWriteTransaction();
      auto advanced = co_await PrefetchJobStateMachine::advance(*txn, current->spec.jobId, current->stateVersion);
      CO_ASSERT_OK(advanced);
      CO_ASSERT_EQ(advanced->state, expected);
      CO_ASSERT_OK(co_await txn->commit());
    }
  }());
}

TEST(TestPrefetchJobStateMachine, EmptyPlanFailsAndReadyTransitionIsSingleShot) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    kv::MemKVEngine emptyEngine;
    auto empty = co_await persist(emptyEngine, job(5, 10'000), 0, 0);
    CO_ASSERT_OK(empty);
    auto txn = emptyEngine.createReadWriteTransaction();
    auto failed = co_await PrefetchJobStateMachine::advance(*txn, empty->spec.jobId, empty->stateVersion);
    CO_ASSERT_OK(failed);
    CO_ASSERT_EQ(failed->state, cache::PrefetchJobState::FAILED);
    CO_ASSERT_EQ(failed->error, "EMPTY_PLAN");
    CO_ASSERT_OK(co_await txn->commit());

    kv::MemKVEngine readyEngine;
    auto current = co_await persist(readyEngine, job(6, 10'000), 1, 1);
    CO_ASSERT_OK(current);
    txn = readyEngine.createReadWriteTransaction();
    auto first = co_await PrefetchJobStateMachine::advance(*txn, current->spec.jobId, current->stateVersion);
    CO_ASSERT_OK(first);
    CO_ASSERT_OK(co_await txn->commit());
    txn = readyEngine.createReadWriteTransaction();
    auto concurrentRetry = co_await PrefetchJobStateMachine::advance(*txn, current->spec.jobId, current->stateVersion);
    CO_ASSERT_OK(concurrentRetry);
    CO_ASSERT_EQ(concurrentRetry->state, cache::PrefetchJobState::READY);
    CO_ASSERT_EQ(concurrentRetry->stateVersion, first->stateVersion);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
