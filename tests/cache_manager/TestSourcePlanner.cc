#include <cstdlib>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/planner/SourcePlanner.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

class FakePlanner : public SourcePlanner {
 public:
  FakePlanner(PlannerContext context, uint32_t maxCursorBytes, PlannerPage page)
      : SourcePlanner(std::move(context), maxCursorBytes),
        page_(std::move(page)) {}

  std::string seenCursor;

 protected:
  CoTryTask<PlannerPage> plan(std::string_view cursor) override {
    seenCursor = cursor;
    co_return page_;
  }

 private:
  PlannerPage page_;
};

PlannerContext context(CancellationToken cancellation = {}) {
  return {cache::PrefetchJobId{Uuid::from(1, 2)}, 3, 7, 2, std::move(cancellation)};
}

cache::PrefetchPlanEntry entry(uint32_t block = 0) {
  return {cache::PrefetchJobId{Uuid::from(1, 2)},
          {10, cache::CacheBlockIndex{block}},
          4096,
          7,
          cache::PrefetchPlanEntryState::PLANNED,
          Uuid::zero()};
}

cache::DatasetSource source(cache::DatasetSourceType type) {
  switch (type) {
    case cache::DatasetSourceType::NAMESPACE_PATH:
      return cache::DatasetSource{cache::NamespacePathSource{"/data", true}};
    case cache::DatasetSourceType::PATH_LIST:
      return cache::DatasetSource{cache::PathListSource{{"/data"}}};
    case cache::DatasetSourceType::MANIFEST_PATH:
      return cache::DatasetSource{cache::ManifestPathSource{"/manifest"}};
    case cache::DatasetSourceType::S3_PREFIX:
      return cache::DatasetSource{cache::S3PrefixSource{cache::OriginId{1}, "bucket", "prefix", "/dest"}};
  }
  std::abort();
}

SourcePlannerFactory::Builders builders(PlannerPage page = {{entry()}, "next", false}) {
  SourcePlannerFactory::Builders result;
  for (auto type : {cache::DatasetSourceType::NAMESPACE_PATH,
                    cache::DatasetSourceType::PATH_LIST,
                    cache::DatasetSourceType::MANIFEST_PATH,
                    cache::DatasetSourceType::S3_PREFIX}) {
    result.emplace(type, [page](const cache::DatasetSource &, PlannerContext plannerContext, uint32_t maxCursorBytes) {
      std::unique_ptr<SourcePlanner> planner =
          std::make_unique<FakePlanner>(std::move(plannerContext), maxCursorBytes, page);
      return Result<std::unique_ptr<SourcePlanner>>(std::move(planner));
    });
  }
  return result;
}

TEST(TestSourcePlanner, FactoryRequiresCompleteKnownBuildersAndValidLimits) {
  auto missing = builders();
  missing.erase(cache::DatasetSourceType::S3_PREFIX);
  ASSERT_ERROR(SourcePlannerFactory::create({}, std::move(missing)), StatusCode::kInvalidConfig);

  auto unknown = builders();
  unknown.erase(cache::DatasetSourceType::S3_PREFIX);
  unknown.emplace(static_cast<cache::DatasetSourceType>(255), unknown.begin()->second);
  ASSERT_ERROR(SourcePlannerFactory::create({}, std::move(unknown)), StatusCode::kInvalidConfig);

  ASSERT_ERROR(SourcePlannerFactory::create({0, 10}, builders()), StatusCode::kInvalidConfig);
  ASSERT_ERROR(SourcePlannerFactory::create({10, 0}, builders()), StatusCode::kInvalidConfig);

  auto factory = SourcePlannerFactory::create({}, builders());
  ASSERT_OK(factory);
  for (auto type : {cache::DatasetSourceType::NAMESPACE_PATH,
                    cache::DatasetSourceType::PATH_LIST,
                    cache::DatasetSourceType::MANIFEST_PATH,
                    cache::DatasetSourceType::S3_PREFIX}) {
    ASSERT_OK((*factory)->make(source(type), context()));
  }
}

TEST(TestSourcePlanner, PreservesOpaqueCursorAndValidatesPageBounds) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto factory = SourcePlannerFactory::create({}, builders());
    CO_ASSERT_OK(factory);
    auto planner = (*factory)->make(source(cache::DatasetSourceType::NAMESPACE_PATH), context());
    CO_ASSERT_OK(planner);
    auto page = co_await (*planner)->nextPage("opaque:cursor");
    CO_ASSERT_OK(page);
    CO_ASSERT_EQ(page->nextCursor, "next");
    auto *fake = dynamic_cast<FakePlanner *>(planner->get());
    CO_ASSERT_NE(fake, nullptr);
    CO_ASSERT_EQ(fake->seenCursor, "opaque:cursor");

    PlannerPage oversized{{entry(0), entry(1), entry(2)}, "next", false};
    factory = SourcePlannerFactory::create({}, builders(std::move(oversized)));
    CO_ASSERT_OK(factory);
    planner = (*factory)->make(source(cache::DatasetSourceType::NAMESPACE_PATH), context());
    CO_ASSERT_OK(planner);
    CO_ASSERT_ERROR(co_await (*planner)->nextPage(""), CacheCode::kRequestTooLarge);
  }());
}

TEST(TestSourcePlanner, RejectsInvalidOutputAndHonorsCancellation) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto wrong = entry();
    wrong.priority = 8;
    auto factory = SourcePlannerFactory::create({}, builders({{wrong}, "next", false}));
    CO_ASSERT_OK(factory);
    auto planner = (*factory)->make(source(cache::DatasetSourceType::NAMESPACE_PATH), context());
    CO_ASSERT_OK(planner);
    CO_ASSERT_ERROR(co_await (*planner)->nextPage(""), StatusCode::kInvalidArg);

    CancellationSource cancellation;
    cancellation.requestCancellation();
    factory = SourcePlannerFactory::create({}, builders());
    CO_ASSERT_OK(factory);
    planner = (*factory)->make(source(cache::DatasetSourceType::NAMESPACE_PATH), context(cancellation.getToken()));
    CO_ASSERT_OK(planner);
    EXPECT_THROW(co_await (*planner)->nextPage(""), OperationCancelled);
  }());
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
