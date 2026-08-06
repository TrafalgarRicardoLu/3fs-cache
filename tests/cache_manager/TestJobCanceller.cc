#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/job/JobCanceller.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PermitIdentity permit(Uuid attempt) {
  storage::PlacementIdentity placement{{flat::ChainId{1}, flat::ChainVersion{1}},
                                       {flat::TargetId{1}},
                                       flat::TargetId{1},
                                       attempt};
  return {Uuid::from(7, 7), placement, 1, {{flat::TargetId{1}, 4096}}};
}

cache::PrefetchJobRecord cancelledJob(cache::PrefetchJobId jobId) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = jobId;
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.state = cache::PrefetchJobState::CANCELLED;
  result.stateVersion = 2;
  result.createdAtMs = 1;
  result.updatedAtMs = 2;
  result.cancelEpoch = 1;
  result.plannerSourceIndex = 1;
  result.planningComplete = true;
  result.plannedBlocks = 4;
  result.plannedBytes = 4 * 4096;
  return result;
}

cache::PrefetchPlanEntry entry(cache::PrefetchJobId jobId, uint32_t block, cache::PrefetchPlanEntryState state) {
  return {jobId,
          {10, cache::CacheBlockIndex{block}},
          4096,
          1,
          state,
          state == cache::PrefetchPlanEntryState::PLANNED ? Uuid::zero() : Uuid::from(9, block + 1)};
}

class FakeCancellerBackend : public JobCancellerBackend {
 public:
  cache::PrefetchJobRecord job;
  std::vector<cache::PrefetchPlanEntry> entries;
  std::vector<std::string> events;
  uint32_t queuedCalls{0};
  uint32_t timeouts{0};

  CoTryTask<cache::PrefetchJobRecord> cancelJob(cache::PrefetchJobId) override {
    events.push_back("job");
    co_return job;
  }

  CoTryTask<meta::ListPrefetchPlanRsp> list(cache::PrefetchJobId,
                                            std::optional<cache::CacheBlockKey> after,
                                            uint32_t limit) override {
    meta::ListPrefetchPlanRsp response;
    for (const auto &current : entries) {
      if (after && current.key.block.toUnderType() <= after->block.toUnderType()) continue;
      if (response.entries.size() == limit) {
        response.more = true;
        break;
      }
      response.entries.push_back(current);
    }
    co_return response;
  }

  CoTryTask<cache::PrefetchPlanEntry> update(const cache::PrefetchPlanEntry &expected,
                                             const cache::PrefetchPlanEntry &desired) override {
    events.push_back("entry");
    auto found = std::find(entries.begin(), entries.end(), expected);
    if (found == entries.end()) co_return makeError(CacheCode::kStateConflict, "fake cancellation CAS mismatch");
    *found = desired;
    co_return desired;
  }

  CoTryTask<void> cancelQueued(const cache::CacheBlockKey &, const storage::PermitIdentity &) override {
    events.push_back("queued");
    ++queuedCalls;
    if (timeouts != 0) {
      --timeouts;
      co_return makeError(RPCCode::kTimeout);
    }
    co_return Void{};
  }
};

TEST(TestJobCanceller, ConvergesPlannedSharedQueuedAndLoadingWithTimeoutRetry) {
  auto jobId = cache::PrefetchJobId{Uuid::from(1, 1)};
  auto otherJob = cache::PrefetchJobId{Uuid::from(1, 2)};
  auto backend = std::make_shared<FakeCancellerBackend>();
  backend->job = cancelledJob(jobId);
  backend->entries = {entry(jobId, 0, cache::PrefetchPlanEntryState::PLANNED),
                      entry(jobId, 1, cache::PrefetchPlanEntryState::ATTACHED),
                      entry(jobId, 2, cache::PrefetchPlanEntryState::ATTACHED),
                      entry(jobId, 3, cache::PrefetchPlanEntryState::ATTACHED)};
  backend->timeouts = 1;
  HintCoalescer hints;
  auto sharedPermit = permit(backend->entries[1].admissionAttemptId);
  ASSERT_OK(hints.attach({meta::InodeId{10},
                          cache::CacheBlockIndex{1},
                          4096,
                          EnsureReason::PREFETCH,
                          1,
                          {{jobId, 1, {}}, {otherJob, 1, {}}},
                          sharedPermit}));
  bool released = false;
  auto exclusivePermit = permit(backend->entries[2].admissionAttemptId);
  ASSERT_OK(hints.attach({meta::InodeId{10},
                          cache::CacheBlockIndex{2},
                          4096,
                          EnsureReason::PREFETCH,
                          1,
                          {{jobId, 1, [&](const Status &) { released = true; }}},
                          exclusivePermit}));

  JobCanceller canceller(backend, hints, 10);
  auto timedOut = folly::coro::blockingWait(canceller.cancel(jobId));
  ASSERT_ERROR(timedOut, RPCCode::kTimeout);
  ASSERT_FALSE(backend->events.empty());
  EXPECT_EQ(backend->events.front(), "job");
  EXPECT_TRUE(released);
  EXPECT_EQ(backend->queuedCalls, 1u);
  EXPECT_EQ(backend->entries[0].state, cache::PrefetchPlanEntryState::CANCELLED);
  EXPECT_EQ(backend->entries[1].state, cache::PrefetchPlanEntryState::CANCELLED);
  EXPECT_EQ(backend->entries[2].state, cache::PrefetchPlanEntryState::ATTACHED);

  auto retried = folly::coro::blockingWait(canceller.cancel(jobId));
  ASSERT_OK(retried);
  EXPECT_EQ(retried->cancelledQueuedLoads, 1u);
  EXPECT_EQ(backend->queuedCalls, 2u);
  for (const auto &current : backend->entries) EXPECT_EQ(current.state, cache::PrefetchPlanEntryState::CANCELLED);
  EXPECT_EQ(hints.size(), 1u);

  auto repeated = folly::coro::blockingWait(canceller.cancel(jobId));
  ASSERT_OK(repeated);
  EXPECT_EQ(repeated->cancelledEntries, 0u);
  EXPECT_EQ(backend->queuedCalls, 2u);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
