#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/job/JobPlanner.h"
#include "cache_manager/job/OrchestrationCoordinator.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::PrefetchPlanEntry planEntry(cache::PrefetchJobId jobId) {
  return {jobId, {10, cache::CacheBlockIndex{0}}, 4096, 3, cache::PrefetchPlanEntryState::PLANNED, Uuid::zero()};
}

cache::PrefetchJobRecord job(uint64_t id, cache::PrefetchJobState state = cache::PrefetchJobState::PENDING) {
  cache::PrefetchJobRecord result;
  result.spec.jobId = cache::PrefetchJobId{Uuid::from(1, id)};
  result.spec.ownerUid = flat::Uid{1};
  result.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  result.spec.priority = 3;
  result.state = state;
  result.stateVersion = 1;
  result.createdAtMs = result.updatedAtMs = 1;
  return result;
}

class FixedPlanner : public SourcePlanner {
 public:
  FixedPlanner(PlannerContext context, uint32_t maxCursorBytes, PlannerPage page)
      : SourcePlanner(std::move(context), maxCursorBytes),
        page_(std::move(page)) {}

 protected:
  CoTryTask<PlannerPage> plan(std::string_view) override { co_return page_; }

 private:
  PlannerPage page_;
};

class FakePlannerBackend : public JobPlannerBackend {
 public:
  cache::PrefetchJobRecord record;
  std::vector<cache::PrefetchPlanEntry> appended;
  uint64_t activePinExpiresAtMs = 0;

  CoTryTask<void> append(cache::PrefetchJobId,
                         std::vector<cache::PrefetchPlanEntry> entries,
                         uint32_t sourceIndex,
                         std::string cursor,
                         bool complete,
                         uint64_t pinExpiresAtMs) override {
    activePinExpiresAtMs = pinExpiresAtMs;
    appended = std::move(entries);
    record.plannerSourceIndex = sourceIndex;
    record.plannerCursor = std::move(cursor);
    record.planningComplete = complete;
    record.state = cache::PrefetchJobState::PLANNING;
    record.plannedBlocks += appended.size();
    for (const auto &entry : appended) record.plannedBytes += entry.blockLength;
    ++record.stateVersion;
    ++record.updatedAtMs;
    co_return Void{};
  }

  CoTryTask<cache::PrefetchJobRecord> get(cache::PrefetchJobId) override { co_return record; }
};

std::shared_ptr<SourcePlannerFactory> factory(PlannerPage page) {
  SourcePlannerFactory::Builders builders;
  for (auto type : {cache::DatasetSourceType::NAMESPACE_PATH,
                    cache::DatasetSourceType::PATH_LIST,
                    cache::DatasetSourceType::MANIFEST_PATH,
                    cache::DatasetSourceType::S3_PREFIX}) {
    builders.emplace(type, [page](const cache::DatasetSource &, PlannerContext context, uint32_t maxCursorBytes) {
      std::unique_ptr<SourcePlanner> result = std::make_unique<FixedPlanner>(std::move(context), maxCursorBytes, page);
      return Result<std::unique_ptr<SourcePlanner>>(std::move(result));
    });
  }
  auto created = SourcePlannerFactory::create({}, std::move(builders));
  EXPECT_TRUE(created.hasValue());
  return std::shared_ptr<SourcePlannerFactory>(std::move(*created));
}

TEST(TestJobPlanner, PersistsSourceProgressIncludingEmptyFinalPage) {
  auto backend = std::make_shared<FakePlannerBackend>();
  backend->record = job(1);
  JobPlanner planner(backend, factory({{}, {}, true}), 100);
  auto result = folly::coro::blockingWait(planner.runNextPage(backend->record));
  ASSERT_OK(result);
  EXPECT_TRUE(result->planningComplete);
  EXPECT_EQ(result->plannerSourceIndex, 1u);
  EXPECT_TRUE(backend->appended.empty());

  backend->record = job(2);
  auto expected = planEntry(backend->record.spec.jobId);
  JobPlanner populated(backend, factory({{expected}, {}, true}), 100);
  result = folly::coro::blockingWait(populated.runNextPage(backend->record));
  ASSERT_OK(result);
  EXPECT_EQ(result->plannedBlocks, 1u);
  ASSERT_EQ(backend->appended.size(), 1u);
  EXPECT_EQ(backend->appended.front(), expected);

  backend->record = job(3);
  JobPlanner pinned(backend, factory({{planEntry(backend->record.spec.jobId)}, {}, true}), 100, 500, [] {
    return 1000;
  });
  ASSERT_OK(folly::coro::blockingWait(pinned.runNextPage(backend->record)));
  EXPECT_EQ(backend->activePinExpiresAtMs, 1500u);
}

class FakeCoordinatorBackend : public OrchestrationCoordinatorBackend {
 public:
  std::vector<cache::PrefetchJobRecord> jobs;

  CoTryTask<meta::ListPrefetchJobsRsp> list(std::optional<cache::PrefetchJobId> after, uint32_t limit) override {
    meta::ListPrefetchJobsRsp response;
    for (const auto &current : jobs) {
      if (after && current.spec.jobId.toUnderType() <= after->toUnderType()) continue;
      if (response.jobs.size() == limit) {
        response.more = true;
        break;
      }
      response.jobs.push_back(current);
    }
    co_return response;
  }
};

TEST(TestOrchestrationCoordinator, RecoversNonTerminalJobsAndRunsSeparatedTicks) {
  auto backend = std::make_shared<FakeCoordinatorBackend>();
  auto pending = job(1);
  auto planning = job(2, cache::PrefetchJobState::PLANNING);
  auto loading = job(3, cache::PrefetchJobState::LOADING);
  loading.plannerSourceIndex = 1;
  loading.planningComplete = true;
  loading.plannedBlocks = 1;
  loading.plannedBytes = 4096;
  auto ready = loading;
  ready.spec.jobId = cache::PrefetchJobId{Uuid::from(1, 4)};
  ready.state = cache::PrefetchJobState::READY;
  backend->jobs = {pending, planning, loading, ready};
  uint32_t plannerCalls = 0;
  uint32_t runnerCalls = 0;
  uint32_t trackerCalls = 0;
  uint32_t recoveryCalls = 0;
  OrchestrationCoordinator coordinator(
      backend,
      2,
      [&](const cache::PrefetchJobRecord &current, const CancellationToken &) -> CoTryTask<cache::PrefetchJobRecord> {
        ++plannerCalls;
        auto updated = current;
        updated.plannerSourceIndex = 1;
        updated.plannerCursor.clear();
        updated.planningComplete = true;
        updated.state = cache::PrefetchJobState::PLANNING;
        ++updated.stateVersion;
        ++updated.updatedAtMs;
        co_return updated;
      },
      [&](const cache::PrefetchJobRecord &,
          std::optional<cache::CacheBlockKey>,
          const CancellationToken &) -> CoTryTask<JobRunnerPageResult> {
        ++runnerCalls;
        co_return JobRunnerPageResult{};
      },
      [&](const cache::PrefetchJobRecord &, const CancellationToken &) -> CoTryTask<void> {
        ++trackerCalls;
        co_return Void{};
      },
      [&](const cache::PrefetchJobRecord &current,
          std::optional<cache::CacheBlockKey>,
          const CancellationToken &) -> CoTryTask<JobRunnerPageResult> {
        ++recoveryCalls;
        EXPECT_EQ(current.spec.jobId, loading.spec.jobId);
        co_return JobRunnerPageResult{};
      });

  auto recovered = folly::coro::blockingWait(coordinator.recover());
  ASSERT_OK(recovered);
  EXPECT_EQ(*recovered, 3u);
  EXPECT_EQ(recoveryCalls, 1u);
  ASSERT_OK(folly::coro::blockingWait(coordinator.recover()));
  EXPECT_EQ(recoveryCalls, 2u);
  ASSERT_OK(folly::coro::blockingWait(coordinator.runPlannerOnce()));
  EXPECT_EQ(plannerCalls, 2u);
  ASSERT_OK(folly::coro::blockingWait(coordinator.runRunnerOnce()));
  EXPECT_EQ(runnerCalls, 1u);
  ASSERT_OK(folly::coro::blockingWait(coordinator.runTrackerOnce()));
  EXPECT_EQ(trackerCalls, 3u);
}

TEST(TestOrchestrationCoordinator, StopPreventsNewSchedulingAndIsIdempotent) {
  auto backend = std::make_shared<FakeCoordinatorBackend>();
  backend->jobs = {job(1)};
  OrchestrationCoordinator coordinator(backend, 10, {}, {});
  ASSERT_OK(folly::coro::blockingWait(coordinator.recover()));
  coordinator.stop();
  coordinator.stop();
  EXPECT_THROW(folly::coro::blockingWait(coordinator.runPlannerOnce()), OperationCancelled);
}

TEST(TestOrchestrationCoordinator, SchedulesJobsByGlobalPriorityThenCreationOrder) {
  auto backend = std::make_shared<FakeCoordinatorBackend>();
  auto low = job(1, cache::PrefetchJobState::LOADING);
  low.spec.priority = 1;
  low.plannedBlocks = 1;
  auto high = job(2, cache::PrefetchJobState::LOADING);
  high.spec.priority = 10;
  high.plannedBlocks = 1;
  backend->jobs = {low, high};
  std::vector<cache::PrefetchJobId> order;
  OrchestrationCoordinator coordinator(
      backend,
      10,
      {},
      [&](const cache::PrefetchJobRecord &current,
          std::optional<cache::CacheBlockKey>,
          const CancellationToken &) -> CoTryTask<JobRunnerPageResult> {
        order.push_back(current.spec.jobId);
        co_return JobRunnerPageResult{};
      });
  ASSERT_OK(folly::coro::blockingWait(coordinator.runRunnerOnce()));
  ASSERT_EQ(order, (std::vector<cache::PrefetchJobId>{high.spec.jobId, low.spec.jobId}));
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
