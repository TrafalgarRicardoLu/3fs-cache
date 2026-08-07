#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <tuple>

#include "cache_manager/job/JobRunner.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

class FakeJobRunnerBackend : public JobRunnerBackend {
 public:
  std::vector<cache::PrefetchPlanEntry> entries;
  std::map<uint32_t, JobAdmissionDisposition> dispositions;
  std::map<uint32_t, Status> errors;
  std::map<uint32_t, Completion> completions;
  std::vector<Uuid> seenAttempts;
  bool completeDuringAttach{false};

  CoTryTask<meta::ListPrefetchPlanRsp> list(cache::PrefetchJobId jobId,
                                            std::optional<cache::CacheBlockKey> after,
                                            uint32_t limit) override {
    meta::ListPrefetchPlanRsp response;
    for (const auto &entry : entries) {
      if (entry.jobId != jobId || (after && !less(*after, entry.key))) continue;
      if (response.entries.size() == limit) {
        response.more = true;
        break;
      }
      response.entries.push_back(entry);
    }
    co_return response;
  }

  CoTryTask<cache::PrefetchPlanEntry> update(const cache::PrefetchPlanEntry &expected,
                                             const cache::PrefetchPlanEntry &desired) override {
    auto found = std::find(entries.begin(), entries.end(), expected);
    if (found == entries.end()) {
      found = std::find(entries.begin(), entries.end(), desired);
      if (found != entries.end()) co_return desired;
      co_return makeError(CacheCode::kStateConflict, "fake CAS mismatch");
    }
    *found = desired;
    co_return desired;
  }

  CoTryTask<JobAdmissionResult> admit(const cache::PrefetchPlanEntry &entry, Completion completion) override {
    auto block = entry.key.block.toUnderType();
    seenAttempts.push_back(entry.admissionAttemptId);
    auto error = errors.find(block);
    if (error != errors.end()) {
      auto status = error->second;
      errors.erase(error);
      co_return makeError(std::move(status));
    }
    if (completeDuringAttach)
      completion(Status::OK);
    else
      completions[block] = std::move(completion);
    auto disposition = dispositions.at(block);
    std::optional<cache::ReadyIdentity> ready;
    if (disposition == JobAdmissionDisposition::READY) {
      ready = cache::ReadyIdentity{1, cache::CacheGeneration{block + 1}, 1, block + 10, entry.blockLength};
    }
    co_return JobAdmissionResult{disposition, ready};
  }

 private:
  static bool less(const cache::CacheBlockKey &lhs, const cache::CacheBlockKey &rhs) {
    return std::tie(lhs.inode, lhs.block) < std::tie(rhs.inode, rhs.block);
  }
};

cache::PrefetchJobRecord job(uint32_t parallel = 4) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(1, 1)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.spec.priority = 7;
  result.spec.maxParallelLoads = parallel;
  result.state = cache::PrefetchJobState::LOADING;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 1;
  return result;
}

cache::PrefetchPlanEntry entry(uint32_t block) {
  return {cache::PrefetchJobId{Uuid::from(1, 1)},
          {10, cache::CacheBlockIndex{block}},
          4096,
          7,
          cache::PrefetchPlanEntryState::PLANNED,
          Uuid::zero()};
}

TEST(TestJobRunner, PersistsReadyQueuedAndLoadingOutcomes) {
  auto backend = std::make_shared<FakeJobRunnerBackend>();
  backend->entries = {entry(0), entry(1), entry(2)};
  backend->dispositions = {{0, JobAdmissionDisposition::READY},
                           {1, JobAdmissionDisposition::QUEUED},
                           {2, JobAdmissionDisposition::LOADING}};
  JobQuota quota;
  uint64_t attempt = 0;
  JobRunner runner(backend, quota, 10, [&] { return Uuid::from(2, ++attempt); });

  auto result = folly::coro::blockingWait(runner.runNextPage(job()));
  ASSERT_OK(result);
  EXPECT_EQ(result->ready, 1u);
  EXPECT_EQ(result->attached, 2u);
  EXPECT_EQ(backend->entries[0].state, cache::PrefetchPlanEntryState::READY);
  EXPECT_EQ(backend->entries[1].state, cache::PrefetchPlanEntryState::ATTACHED);
  EXPECT_EQ(backend->entries[2].state, cache::PrefetchPlanEntryState::ATTACHED);
  EXPECT_EQ(quota.inflight(job().spec.jobId), 2u);
  backend->completions.at(1)(Status::OK);
  backend->completions.at(2)(Status::OK);
  EXPECT_EQ(quota.inflight(job().spec.jobId), 0u);
}

TEST(TestJobRunner, LeavesCapacityFailuresRetryableAndDoesNotAdvancePastThrottle) {
  auto backend = std::make_shared<FakeJobRunnerBackend>();
  backend->entries = {entry(0), entry(1), entry(2)};
  backend->dispositions = {{0, JobAdmissionDisposition::CAPACITY_WAIT},
                           {1, JobAdmissionDisposition::QUEUED},
                           {2, JobAdmissionDisposition::QUEUED}};
  JobQuota quota;
  JobRunner runner(backend, quota, 10, [] { return Uuid::random(); });

  auto result = folly::coro::blockingWait(runner.runNextPage(job(1)));
  ASSERT_OK(result);
  EXPECT_EQ(result->retryable, 1u);
  EXPECT_TRUE(result->throttled);
  ASSERT_TRUE(result->nextAfter.has_value());
  EXPECT_EQ(result->nextAfter->block, cache::CacheBlockIndex{1});
  EXPECT_EQ(backend->entries[0].state, cache::PrefetchPlanEntryState::ADMITTED);
  EXPECT_EQ(backend->entries[1].state, cache::PrefetchPlanEntryState::ATTACHED);
  EXPECT_EQ(backend->entries[2].state, cache::PrefetchPlanEntryState::PLANNED);
}

TEST(TestJobRunner, ReusesDurableAttemptAfterAmbiguousTimeoutAndRestart) {
  auto backend = std::make_shared<FakeJobRunnerBackend>();
  backend->entries = {entry(0)};
  backend->dispositions = {{0, JobAdmissionDisposition::READY}};
  backend->errors.emplace(0, Status(RPCCode::kTimeout));
  JobQuota firstQuota;
  JobRunner first(backend, firstQuota, 10, [] { return Uuid::from(9, 9); });
  auto timedOut = folly::coro::blockingWait(first.runNextPage(job()));
  ASSERT_OK(timedOut);
  EXPECT_EQ(timedOut->retryable, 1u);
  EXPECT_EQ(backend->entries[0].state, cache::PrefetchPlanEntryState::ADMITTED);

  JobQuota restartedQuota;
  JobRunner restarted(backend, restartedQuota, 10, [] { return Uuid::from(8, 8); });
  auto recovered = folly::coro::blockingWait(restarted.runNextPage(job()));
  ASSERT_OK(recovered);
  EXPECT_EQ(recovered->ready, 1u);
  ASSERT_EQ(backend->seenAttempts.size(), 2u);
  EXPECT_EQ(backend->seenAttempts[0], Uuid::from(9, 9));
  EXPECT_EQ(backend->seenAttempts[1], Uuid::from(9, 9));
}

TEST(TestJobRunner, RecoveryReattachesDurableOwnershipWithoutNewAdmission) {
  auto backend = std::make_shared<FakeJobRunnerBackend>();
  auto planned = entry(0);
  auto admitted = entry(1);
  admitted.state = cache::PrefetchPlanEntryState::ADMITTED;
  admitted.admissionAttemptId = Uuid::from(7, 7);
  backend->entries = {planned, admitted};
  backend->dispositions = {{1, JobAdmissionDisposition::READY}};
  JobQuota quota;
  uint64_t newAttempts = 0;
  JobRunner restarted(backend, quota, 10, [&] {
    ++newAttempts;
    return Uuid::from(8, newAttempts);
  });

  auto recovered = folly::coro::blockingWait(restarted.runNextPage(job(), std::nullopt, {}, true));
  ASSERT_OK(recovered);
  EXPECT_EQ(recovered->visited, 2u);
  EXPECT_EQ(recovered->ready, 1u);
  EXPECT_EQ(newAttempts, 0u);
  ASSERT_EQ(backend->seenAttempts.size(), 1u);
  EXPECT_EQ(backend->seenAttempts.front(), Uuid::from(7, 7));
  EXPECT_EQ(backend->entries.front().state, cache::PrefetchPlanEntryState::PLANNED);
}

TEST(TestJobRunner, HandlesCompletionRacingWithAttach) {
  auto backend = std::make_shared<FakeJobRunnerBackend>();
  backend->entries = {entry(0)};
  backend->dispositions = {{0, JobAdmissionDisposition::LOADING}};
  backend->completeDuringAttach = true;
  JobQuota quota;
  JobRunner runner(backend, quota);
  auto result = folly::coro::blockingWait(runner.runNextPage(job(1)));
  ASSERT_OK(result);
  EXPECT_EQ(result->attached, 1u);
  EXPECT_EQ(quota.inflight(job().spec.jobId), 0u);
}

TEST(TestJobRunner, ObserveOnlyPinDoesNotAdmitMissingBlocks) {
  auto backend = std::make_shared<FakeJobRunnerBackend>();
  backend->entries = {entry(0)};
  JobQuota quota;
  JobRunner runner(backend, quota);
  auto observeOnly = job();
  observeOnly.spec.loadMissing = false;
  auto result = folly::coro::blockingWait(runner.runNextPage(observeOnly));
  ASSERT_OK(result);
  EXPECT_EQ(result->visited, 0u);
  EXPECT_TRUE(backend->seenAttempts.empty());
  EXPECT_EQ(backend->entries.front().state, cache::PrefetchPlanEntryState::PLANNED);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
