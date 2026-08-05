#include <gtest/gtest.h>

#include "cache_manager/scheduler/JobQuota.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

TEST(TestJobQuota, EnforcesParallelLoadsAndReleasesWithPermitLifetime) {
  SteadyTime now{};
  JobQuota quota([&] { return now; });
  auto job = cache::PrefetchJobId{Uuid::from(1, 1)};
  ASSERT_OK(quota.registerJob(job, {2, 0}));
  auto first = quota.tryAcquire(job, 10);
  auto second = quota.tryAcquire(job, 10);
  ASSERT_OK(first);
  ASSERT_OK(second);
  ASSERT_ERROR(quota.tryAcquire(job, 1), CacheCode::kThrottled);
  EXPECT_EQ(quota.inflight(job), uint32_t{2});
  first = JobQuota::Permit{};
  EXPECT_EQ(quota.inflight(job), uint32_t{1});
  ASSERT_OK(quota.tryAcquire(job, 1));
}

TEST(TestJobQuota, RefillsIntegerTokenBucketWithInjectedClock) {
  SteadyTime now{};
  JobQuota quota([&] { return now; });
  auto job = cache::PrefetchJobId{Uuid::from(1, 1)};
  ASSERT_OK(quota.registerJob(job, {4, 100}));
  auto initial = quota.tryAcquire(job, 100);
  ASSERT_OK(initial);
  initial = JobQuota::Permit{};
  ASSERT_ERROR(quota.tryAcquire(job, 1), CacheCode::kThrottled);
  now += std::chrono::milliseconds(250);
  auto refilled = quota.tryAcquire(job, 25);
  ASSERT_OK(refilled);
  ASSERT_ERROR(quota.tryAcquire(job, 1), CacheCode::kThrottled);
}

TEST(TestJobQuota, IsolatesJobsAndHonorsCancellationBeforeWaiting) {
  SteadyTime now{};
  JobQuota quota([&] { return now; });
  auto first = cache::PrefetchJobId{Uuid::from(1, 1)};
  auto second = cache::PrefetchJobId{Uuid::from(1, 2)};
  ASSERT_OK(quota.registerJob(first, {1, 10}));
  ASSERT_OK(quota.registerJob(second, {1, 10}));
  auto firstPermit = quota.tryAcquire(first, 10);
  auto secondPermit = quota.tryAcquire(second, 10);
  ASSERT_OK(firstPermit);
  ASSERT_OK(secondPermit);

  CancellationSource cancellation;
  cancellation.requestCancellation();
  EXPECT_THROW(quota.tryAcquire(first, 1, cancellation.getToken()), OperationCancelled);
  ASSERT_ERROR(quota.removeJob(first), CacheCode::kStateConflict);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
