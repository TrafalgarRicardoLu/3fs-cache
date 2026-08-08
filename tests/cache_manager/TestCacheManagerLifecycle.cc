#include <array>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <vector>

#include "cache_manager/config/Config.h"
#include "cache_manager/recovery/StartupRecoveryCoordinator.h"
#include "cache_manager/service/CacheManagerOperator.h"
#include "common/utils/Toml.hpp"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

Config makeConfig() {
  Config config;
  config.set_service_token("test-token");
  return config;
}

TEST(TestCacheManagerConfig, SerdeAndOriginMapping) {
  auto config = makeConfig();
  ASSERT_TRUE(config.update(toml::parse(R"(
    global_concurrency = 8
    max_inflight_bytes = 1048576
    range_size = 65536
    load_lease = "20s"
    hint_timeout = "250ms"

    [[origins]]
    origin_id = 7
    endpoint = "http://minio:9000"
    region = "test-region"
    use_tls = false
    path_style = true
    max_concurrent_requests = 3
    max_inflight_bytes = 524288
  )"),
                            false));
  ASSERT_OK(config.validateRuntime());
  ASSERT_EQ(config.global_concurrency(), uint32_t{8});
  ASSERT_EQ(config.findOrigin(7)->region(), "test-region");
  ASSERT_EQ(config.findOrigin(8), nullptr);

  Config decoded;
  ASSERT_TRUE(decoded.update(toml::parse(config.toString()), false));
  ASSERT_EQ(decoded.findOrigin(7)->endpoint(), "http://minio:9000");
}

TEST(TestCacheManagerConfig, RejectsInvalidAndDuplicateOrigins) {
  auto missingToken = Config{};
  ASSERT_ERROR(missingToken.validateRuntime(), StatusCode::kInvalidConfig);

  auto duplicate = makeConfig();
  duplicate.set_origins_length(2);
  duplicate.origins(0).set_origin_id(1);
  duplicate.origins(0).set_endpoint("a");
  duplicate.origins(1).set_origin_id(1);
  duplicate.origins(1).set_endpoint("b");
  ASSERT_ERROR(duplicate.validateRuntime(), StatusCode::kInvalidConfig);
}

TEST(TestCacheManagerConfig, PhaseThreeRequiresPhaseTwoAndDefaultsOff) {
  auto config = makeConfig();
  EXPECT_FALSE(config.enable_phase3());
  config.set_enable_phase3(true);
  ASSERT_ERROR(config.validateRuntime(), StatusCode::kInvalidConfig);
  config.set_enable_phase2(true);
  ASSERT_OK(config.validateRuntime());
}

TEST(TestCacheManagerConfig, PhaseFourRequiresEarlierPhasesAndDefaultsOff) {
  auto config = makeConfig();
  EXPECT_FALSE(config.enable_phase4());
  EXPECT_EQ(config.phase4_publish_prefetch_priority(), static_cast<uint32_t>(std::numeric_limits<int32_t>::max()));
  config.set_enable_phase4(true);
  ASSERT_ERROR(config.validateRuntime(), StatusCode::kInvalidConfig);
  config.set_enable_phase2(true);
  config.set_enable_phase3(true);
  ASSERT_OK(config.validateRuntime());
  config.set_phase4_publish_prefetch_priority(std::numeric_limits<uint32_t>::max());
  EXPECT_EQ(config.phase4_publish_prefetch_priority(), static_cast<uint32_t>(std::numeric_limits<int32_t>::max()));

  config.set_upload_part_size(5_MB - 1);
  ASSERT_ERROR(config.validateRuntime(), StatusCode::kInvalidConfig);
  config.set_upload_part_size(5_MB);
  config.set_phase4_upload_global_concurrency(1);
  config.set_phase4_upload_per_owner_concurrency(2);
  ASSERT_ERROR(config.validateRuntime(), StatusCode::kInvalidConfig);
}

TEST(TestCacheManagerLifecycle, StartStopAndRepeatedStop) {
  auto config = makeConfig();
  CacheManagerOperator operator_(config, nullptr, nullptr);
  int starts = 0;
  int stops = 0;
  ASSERT_OK(operator_.startForTest(
      [&]() -> Result<Void> {
        EXPECT_FALSE(operator_.admissionReady());
        ++starts;
        return Void{};
      },
      [&] { ++stops; }));
  ASSERT_TRUE(operator_.running());
  ASSERT_TRUE(operator_.admissionReady());
  ASSERT_OK(operator_.startForTest([&]() -> Result<Void> {
    ++starts;
    return Void{};
  }));
  ASSERT_EQ(starts, 1);
  operator_.stop();
  operator_.stop();
  ASSERT_FALSE(operator_.running());
  ASSERT_FALSE(operator_.admissionReady());
  ASSERT_EQ(stops, 1);
}

TEST(TestCacheManagerLifecycle, FailedDependencyIsRolledBack) {
  auto config = makeConfig();
  CacheManagerOperator operator_(config, nullptr, nullptr);
  int rollbacks = 0;
  auto result = operator_.startForTest(
      []() -> Result<Void> { return makeError(StatusCode::kInvalidConfig, "dependency failed"); },
      [&] { ++rollbacks; });
  ASSERT_ERROR(result, StatusCode::kInvalidConfig);
  ASSERT_FALSE(operator_.running());
  ASSERT_FALSE(operator_.admissionReady());
  ASSERT_EQ(rollbacks, 1);
  operator_.stop();
  ASSERT_EQ(rollbacks, 1);
}

TEST(TestCacheManagerLifecycle, StartupRecoveryRollsBackEveryFailureBoundaryAndRetries) {
  constexpr std::array stages{StartupRecoveryStage::ROUTING,
                              StartupRecoveryStage::PERMIT_AND_LOADING,
                              StartupRecoveryStage::EVICTION_AND_CLEANUP,
                              StartupRecoveryStage::JOBS,
                              StartupRecoveryStage::RECONCILE};
  for (size_t failed = 0; failed < stages.size(); ++failed) {
    std::vector<std::string> events;
    bool injectFailure = true;
    std::array<StartupRecoveryCoordinator::Step, stages.size()> steps;
    for (size_t index = 0; index < stages.size(); ++index) {
      steps[index] = {
          stages[index],
          [&, index]() -> CoTryTask<void> {
            events.push_back("start:" + std::to_string(index));
            if (injectFailure && index == failed) {
              co_return makeError(CacheCode::kUnavailable, "injected startup failure");
            }
            co_return Void{};
          },
          [&, index] { events.push_back("stop:" + std::to_string(index)); },
      };
    }
    StartupRecoveryCoordinator coordinator(std::move(steps));
    ASSERT_ERROR(folly::coro::blockingWait(coordinator.run()), CacheCode::kUnavailable);
    std::vector<std::string> expected;
    for (size_t index = 0; index <= failed; ++index) expected.push_back("start:" + std::to_string(index));
    for (size_t count = failed + 1; count > 0; --count) expected.push_back("stop:" + std::to_string(count - 1));
    EXPECT_EQ(events, expected);
    EXPECT_FALSE(coordinator.running());

    events.clear();
    injectFailure = false;
    ASSERT_OK(folly::coro::blockingWait(coordinator.run()));
    expected.clear();
    for (size_t index = 0; index < stages.size(); ++index) expected.push_back("start:" + std::to_string(index));
    EXPECT_EQ(events, expected);
    EXPECT_FALSE(coordinator.running());
  }
}

TEST(TestCacheManagerLifecycle, StartupRecoveryRejectsOutOfOrderStepsWithoutStartingWorkers) {
  constexpr std::array stages{StartupRecoveryStage::PERMIT_AND_LOADING,
                              StartupRecoveryStage::ROUTING,
                              StartupRecoveryStage::EVICTION_AND_CLEANUP,
                              StartupRecoveryStage::JOBS,
                              StartupRecoveryStage::RECONCILE};
  uint64_t starts = 0;
  uint64_t stops = 0;
  std::array<StartupRecoveryCoordinator::Step, stages.size()> steps;
  for (size_t index = 0; index < stages.size(); ++index) {
    steps[index] = {stages[index],
                    [&]() -> CoTryTask<void> {
                      ++starts;
                      co_return Void{};
                    },
                    [&] { ++stops; }};
  }
  StartupRecoveryCoordinator coordinator(std::move(steps));
  ASSERT_ERROR(folly::coro::blockingWait(coordinator.run()), StatusCode::kInvalidConfig);
  EXPECT_EQ(starts, 0);
  EXPECT_EQ(stops, 0);
}

TEST(TestCacheManagerLifecycle, PhaseFourAdmissionRemainsClosedUntilStartupCompletes) {
  auto config = makeConfig();
  config.set_enable_phase2(true);
  config.set_enable_phase3(true);
  config.set_enable_phase4(true);
  CacheManagerOperator operator_(config, nullptr, nullptr);
  EnsureCachedReq request;
  request.service = {"cache-manager", "test-token"};
  request.inode = meta::InodeId{1};
  request.blockCount = 1;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;

  auto blocked = folly::coro::blockingWait(operator_.ensureCached(request));
  ASSERT_OK(blocked);
  EXPECT_EQ(blocked->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(blocked->bypassReason, BypassReason::UNAVAILABLE);

  ASSERT_OK(operator_.startForTest([] { return Result<Void>{Void{}}; }));
  auto enabled = folly::coro::blockingWait(operator_.ensureCached(request));
  ASSERT_OK(enabled);
  EXPECT_EQ(enabled->bypassReason, BypassReason::FEATURE_DISABLED);
}

TEST(TestCacheManagerService, ValidatesIdentityAndProtocol) {
  auto config = makeConfig();
  CacheManagerOperator operator_(config, nullptr, nullptr);
  EnsureCachedReq request;
  request.service = {"cache-manager", "test-token"};
  request.inode = meta::InodeId{1};
  request.blockCount = 1;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;

  auto accepted = folly::coro::blockingWait(operator_.ensureCached(request));
  ASSERT_OK(accepted);
  ASSERT_EQ(accepted->status, EnsureCachedStatus::BYPASSED);
  ASSERT_EQ(accepted->bypassReason, BypassReason::FEATURE_DISABLED);

  request.service.token = "wrong";
  ASSERT_ERROR(folly::coro::blockingWait(operator_.ensureCached(request)), StatusCode::kAuthenticationFail);
  request.service.token = "test-token";
  request.cacheProtocolVersion = 0;
  ASSERT_ERROR(folly::coro::blockingWait(operator_.ensureCached(request)), CacheCode::kUpgradeRequired);
}

TEST(TestCacheManagerService, RedactsServiceToken) {
  ServiceIdentity identity{"cache-manager", "sensitive"};
  auto readable = identity.serdeToReadable();
  ASSERT_EQ(readable.find("sensitive"), std::string::npos);
}

TEST(TestCacheManagerService, Phase3ContractsDefaultToDisabled) {
  auto config = makeConfig();
  CacheManagerOperator operator_(config, nullptr, nullptr);
  CreatePrefetchJobReq request;
  request.user.uid = flat::Uid{1000};
  request.spec.jobId = cache::PrefetchJobId{Uuid::from(1, 2)};
  request.spec.ownerUid = request.user.uid;
  request.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;

  ASSERT_ERROR(folly::coro::blockingWait(operator_.createPrefetchJob(request)), CacheCode::kFeatureDisabled);
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  ASSERT_ERROR(folly::coro::blockingWait(operator_.createPrefetchJob(request)), CacheCode::kUpgradeRequired);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
