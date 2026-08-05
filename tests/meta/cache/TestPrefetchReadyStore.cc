#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "common/kv/mem/MemKVEngine.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/PrefetchJobStore.h"
#include "meta/store/cache/PrefetchPlanStore.h"
#include "meta/store/cache/PrefetchReadyStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

cache::PrefetchJobRecord job(uint64_t id) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(1, id)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.state = cache::PrefetchJobState::PENDING;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 1;
  return result;
}

cache::PrefetchPlanEntry entry(cache::PrefetchJobId jobId, uint32_t block, uint64_t length) {
  return {jobId, {10, cache::CacheBlockIndex{block}}, length, 1, cache::PrefetchPlanEntryState::PLANNED, Uuid::zero()};
}

CacheBlockRecord ready(const cache::PrefetchPlanEntry &entry, uint64_t generation) {
  CacheBlockRecord record;
  record.key = entry.key;
  record.state = cache::CacheBlockState::READY;
  record.chainId = flat::ChainId{1};
  record.blockLength = entry.blockLength;
  record.cacheGeneration = cache::CacheGeneration{generation};
  record.ready = cache::ReadyIdentity{generation, record.cacheGeneration, 1, 1234, entry.blockLength};
  record.chargeKind = cache::ChargeKind::COMMITTED;
  record.chargedBytes = entry.blockLength;
  return record;
}

CoTask<void> prepare(kv::MemKVEngine &engine,
                     cache::PrefetchJobRecord desired,
                     const std::vector<cache::PrefetchPlanEntry> &entries) {
  auto txn = engine.createReadWriteTransaction();
  CO_ASSERT_OK(co_await PrefetchJobStore::create(*txn, desired));
  CO_ASSERT_OK(co_await PrefetchPlanStore::append(*txn, desired.spec.jobId, entries, 1, "", true));
  for (const auto &planned : entries) {
    auto admitted = planned;
    admitted.state = cache::PrefetchPlanEntryState::ADMITTED;
    admitted.admissionAttemptId = Uuid::random();
    CO_ASSERT_OK(co_await PrefetchPlanStore::update(*txn, planned, admitted));
  }
  CO_ASSERT_OK(co_await txn->commit());
}

TEST(TestPrefetchReadyStore, TracksTailBlocksAtomicallyAndIsIdempotentAcrossPages) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    kv::MemKVEngine engine;
    auto desired = job(1);
    std::vector entries{entry(desired.spec.jobId, 0, 4096), entry(desired.spec.jobId, 1, 1)};
    co_await prepare(engine, desired, entries);
    auto txn = engine.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, ready(entries[0], 1)));
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, ready(entries[1], 2)));
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine.createReadWriteTransaction();
    auto first = co_await PrefetchReadyStore::track(*txn, desired.spec.jobId, std::nullopt, 1);
    CO_ASSERT_OK(first);
    CO_ASSERT_TRUE(first->more);
    CO_ASSERT_EQ(first->currentReadyBytes, 4096u);
    CO_ASSERT_EQ(first->job.readyBytes, 4096u);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine.createReadWriteTransaction();
    auto second = co_await PrefetchReadyStore::track(*txn, desired.spec.jobId, first->nextAfter, 1);
    CO_ASSERT_OK(second);
    CO_ASSERT_FALSE(second->more);
    CO_ASSERT_EQ(second->currentReadyBytes, 1u);
    CO_ASSERT_EQ(second->job.readyBytes, 4097u);
    CO_ASSERT_EQ(second->job.readyBlocks, 2u);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine.createReadWriteTransaction();
    auto repeated = co_await PrefetchReadyStore::track(*txn, desired.spec.jobId, std::nullopt, 2);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_EQ(repeated->currentReadyBytes, 4097u);
    CO_ASSERT_EQ(repeated->job.readyBytes, 4097u);
    CO_ASSERT_EQ(repeated->job.stateVersion, second->job.stateVersion);
  }());
}

TEST(TestPrefetchReadyStore, GenerationChangeDropsCurrentButKeepsAchievedReady) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    kv::MemKVEngine engine;
    auto desired = job(2);
    std::vector entries{entry(desired.spec.jobId, 0, 4096)};
    co_await prepare(engine, desired, entries);
    auto txn = engine.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, ready(entries[0], 1)));
    CO_ASSERT_OK(co_await txn->commit());
    txn = engine.createReadWriteTransaction();
    auto tracked = co_await PrefetchReadyStore::track(*txn, desired.spec.jobId, std::nullopt, 10);
    CO_ASSERT_OK(tracked);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine.createReadWriteTransaction();
    CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, ready(entries[0], 2)));
    CO_ASSERT_OK(co_await txn->commit());
    txn = engine.createReadWriteTransaction();
    auto changed = co_await PrefetchReadyStore::track(*txn, desired.spec.jobId, std::nullopt, 10);
    CO_ASSERT_OK(changed);
    CO_ASSERT_EQ(changed->currentReadyBytes, 0u);
    CO_ASSERT_EQ(changed->job.readyBytes, 4096u);
    CO_ASSERT_EQ(changed->job.stateVersion, tracked->job.stateVersion);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
