#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/PrefetchCancellationStore.h"
#include "meta/store/cache/PrefetchPlanStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

cache::PrefetchJobRecord job(uint64_t id) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(3, id)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.state = cache::PrefetchJobState::PENDING;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 1;
  return result;
}

cache::PrefetchPlanEntry entry(cache::PrefetchJobId jobId) {
  return {jobId, {10, cache::CacheBlockIndex{0}}, 4096, 1, cache::PrefetchPlanEntryState::PLANNED, Uuid::zero()};
}

TEST(TestPrefetchCancellationStore, PersistsCancellationFirstAndIsIdempotent) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    kv::MemKVEngine engine;
    auto desired = job(1);
    auto planned = entry(desired.spec.jobId);
    auto txn = engine.createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
    std::vector entries{planned};
    CO_ASSERT_OK(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, entries, 1, "", true));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine.createReadWriteTransaction();
    auto cancelled = co_await PrefetchCancellationStore::cancel(*txn, desired.spec.jobId);
    CO_ASSERT_OK(cancelled);
    CO_ASSERT_EQ(cancelled->state, cache::PrefetchJobState::CANCELLED);
    CO_ASSERT_EQ(cancelled->cancelEpoch, 1u);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine.createReadWriteTransaction();
    auto repeated = co_await PrefetchCancellationStore::cancel(*txn, desired.spec.jobId);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_EQ(repeated->stateVersion, cancelled->stateVersion);
    CO_ASSERT_EQ(repeated->cancelEpoch, cancelled->cancelEpoch);

    auto admitted = planned;
    admitted.state = cache::PrefetchPlanEntryState::ADMITTED;
    admitted.admissionAttemptId = Uuid::from(4, 5);
    CO_ASSERT_ERROR(co_await PrefetchPlanStore::update(*txn, planned, admitted), CacheCode::kStateConflict);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
