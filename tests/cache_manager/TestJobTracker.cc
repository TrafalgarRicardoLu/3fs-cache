#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "cache_manager/job/JobTracker.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::PrefetchJobRecord job() {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(1, 1)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.state = cache::PrefetchJobState::LOADING;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 1;
  result.plannerSourceIndex = 1;
  result.planningComplete = true;
  result.plannedBytes = std::numeric_limits<uint64_t>::max();
  result.plannedBlocks = 2;
  return result;
}

class FakeTrackerBackend : public JobTrackerBackend {
 public:
  std::vector<meta::TrackPrefetchReadyRsp> pages;
  size_t calls{0};
  size_t advanceCalls{0};

  CoTryTask<meta::TrackPrefetchReadyRsp> track(cache::PrefetchJobId,
                                               std::optional<cache::CacheBlockKey>,
                                               uint32_t) override {
    co_return pages.at(calls++);
  }

  CoTryTask<meta::AdvancePrefetchJobStateRsp> advance(cache::PrefetchJobId, uint64_t) override {
    ++advanceCalls;
    meta::AdvancePrefetchJobStateRsp response;
    response.job = pages.back().job;
    response.achievedReadyBps = 1;
    co_return response;
  }
};

TEST(TestJobTracker, RecalculatesCurrentReadinessAcrossPages) {
  auto backend = std::make_shared<FakeTrackerBackend>();
  auto record = job();
  auto firstJob = record;
  firstJob.readyBytes = 4096;
  firstJob.readyBlocks = 1;
  ++firstJob.stateVersion;
  ++firstJob.updatedAtMs;
  auto secondJob = firstJob;
  secondJob.readyBytes = 4097;
  secondJob.readyBlocks = 2;
  ++secondJob.stateVersion;
  ++secondJob.updatedAtMs;
  meta::TrackPrefetchReadyRsp first;
  first.job = firstJob;
  first.currentReadyBytes = 4096;
  first.currentReadyBlocks = 1;
  first.more = true;
  first.nextAfter = cache::CacheBlockKey{10, cache::CacheBlockIndex{0}};
  meta::TrackPrefetchReadyRsp second;
  second.job = secondJob;
  second.currentReadyBytes = 1;
  second.currentReadyBlocks = 1;
  backend->pages = {first, second};

  JobTracker tracker(backend, 1);
  auto result = folly::coro::blockingWait(tracker.run(record));
  ASSERT_OK(result);
  EXPECT_EQ(result->currentReadyBytes, 4097u);
  EXPECT_EQ(result->currentReadyBlocks, 2u);
  EXPECT_EQ(result->job.readyBytes, 4097u);
  EXPECT_EQ(backend->calls, 2u);
  EXPECT_EQ(backend->advanceCalls, 1u);
  EXPECT_EQ(result->achievedReadyBps, 1u);
}

TEST(TestJobTracker, RejectsCurrentCounterOverflowAndNonAdvancingPages) {
  auto backend = std::make_shared<FakeTrackerBackend>();
  auto record = job();
  meta::TrackPrefetchReadyRsp first;
  first.job = record;
  first.currentReadyBytes = std::numeric_limits<uint64_t>::max();
  first.more = true;
  first.nextAfter = cache::CacheBlockKey{10, cache::CacheBlockIndex{0}};
  auto second = first;
  second.currentReadyBytes = 1;
  second.more = false;
  backend->pages = {first, second};
  JobTracker tracker(backend, 1);
  ASSERT_ERROR(folly::coro::blockingWait(tracker.run(record)), CacheCode::kStateConflict);

  backend->calls = 0;
  first.nextAfter.reset();
  backend->pages = {first};
  ASSERT_ERROR(folly::coro::blockingWait(tracker.run(record)), CacheCode::kInvalidResponse);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
