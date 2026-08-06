#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/job/ActiveJobPinManager.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::PrefetchJobRecord job(uint64_t id, cache::PrefetchJobState state, bool pinAfterReady = false) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(1, id)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.spec.pinAfterReady = pinAfterReady;
  result.spec.pinTtlMs = pinAfterReady ? 60000 : 0;
  result.state = state;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 100;
  if (state != cache::PrefetchJobState::PENDING) {
    result.planningComplete = true;
    result.plannerSourceIndex = 1;
    result.plannedBlocks = 1;
    result.plannedBytes = 4096;
  }
  if (state == cache::PrefetchJobState::READY) {
    result.readyBlocks = result.plannedBlocks;
    result.readyBytes = result.plannedBytes;
  }
  if (state == cache::PrefetchJobState::CANCELLED) result.cancelEpoch = 1;
  return result;
}

cache::PrefetchPlanEntry entry(const cache::PrefetchJobRecord &job, uint32_t block = 0) {
  return {job.spec.jobId,
          {10, cache::CacheBlockIndex{block}},
          4096,
          1,
          cache::PrefetchPlanEntryState::PLANNED,
          Uuid::zero()};
}

class FakeBackend : public ActiveJobPinBackend {
 public:
  std::vector<cache::PrefetchJobRecord> jobs;
  std::map<Uuid, std::vector<cache::PrefetchPlanEntry>> plans;
  std::vector<cache::PinRecord> pins;
  std::vector<cache::PinOwner> removed;
  std::vector<std::pair<cache::PrefetchJobId, std::vector<cache::CacheBlockKey>>> converted;
  bool failUpsert = false;

  CoTryTask<meta::ListPrefetchJobsRsp> listJobs(std::optional<cache::PrefetchJobId> after, uint32_t limit) override {
    meta::ListPrefetchJobsRsp response;
    for (const auto &item : jobs) {
      if (after && item.spec.jobId.toUnderType() <= after->toUnderType()) continue;
      if (response.jobs.size() == limit) {
        response.more = true;
        break;
      }
      response.jobs.push_back(item);
    }
    co_return response;
  }

  CoTryTask<meta::ListPrefetchPlanRsp> listPlan(cache::PrefetchJobId jobId,
                                                std::optional<cache::CacheBlockKey> after,
                                                uint32_t limit) override {
    meta::ListPrefetchPlanRsp response;
    for (const auto &item : plans[jobId.toUnderType()]) {
      if (after && std::tie(item.key.inode, item.key.block) <= std::tie(after->inode, after->block)) continue;
      if (response.entries.size() == limit) {
        response.more = true;
        break;
      }
      response.entries.push_back(item);
    }
    co_return response;
  }

  CoTryTask<void> upsert(std::vector<cache::PinRecord> records) override {
    if (failUpsert) co_return makeError(StatusCode::kIOError, "injected renewal failure");
    pins.insert(pins.end(), records.begin(), records.end());
    co_return Void{};
  }

  CoTryTask<void> remove(cache::PinOwner owner) override {
    removed.push_back(owner);
    co_return Void{};
  }
  CoTryTask<void> convert(cache::PrefetchJobId jobId, std::vector<cache::CacheBlockKey> keys) override {
    converted.emplace_back(jobId, std::move(keys));
    co_return Void{};
  }
};

TEST(TestActiveJobPinManager, RecoversRenewsOverlappingJobsAndRetriesFailures) {
  auto backend = std::make_shared<FakeBackend>();
  auto first = job(1, cache::PrefetchJobState::LOADING);
  auto second = job(2, cache::PrefetchJobState::PARTIAL_READY);
  backend->jobs = {first, second};
  backend->plans[first.spec.jobId.toUnderType()] = {entry(first)};
  backend->plans[second.spec.jobId.toUnderType()] = {entry(second)};
  ActiveJobPinManager manager(backend, 1, 1, 500, [] { return 1000; });

  backend->failUpsert = true;
  ASSERT_ERROR(folly::coro::blockingWait(manager.runOnce()), StatusCode::kIOError);
  backend->failUpsert = false;
  ASSERT_OK(folly::coro::blockingWait(manager.runOnce()));
  ASSERT_EQ(backend->pins.size(), 2u);
  EXPECT_EQ(backend->pins[0].key, backend->pins[1].key);
  EXPECT_NE(backend->pins[0].owner, backend->pins[1].owner);
  EXPECT_EQ(backend->pins[0].expiresAtMs, 1500u);
}

TEST(TestActiveJobPinManager, RemovesTerminalOwnersAndRetainsReadyPinByPolicy) {
  auto backend = std::make_shared<FakeBackend>();
  auto failed = job(1, cache::PrefetchJobState::FAILED);
  auto cancelled = job(2, cache::PrefetchJobState::CANCELLED);
  auto ready = job(3, cache::PrefetchJobState::READY);
  auto retained = job(4, cache::PrefetchJobState::READY, true);
  backend->jobs = {failed, cancelled, ready, retained};
  backend->plans[retained.spec.jobId.toUnderType()] = {entry(retained)};
  ActiveJobPinManager manager(backend, 10, 10, 500, [] { return 1000; });

  ASSERT_OK(folly::coro::blockingWait(manager.runOnce()));
  EXPECT_TRUE(backend->pins.empty());
  ASSERT_EQ(backend->converted.size(), 1u);
  EXPECT_EQ(backend->converted.front().first, retained.spec.jobId);
  EXPECT_EQ(backend->removed.size(), 4u);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
