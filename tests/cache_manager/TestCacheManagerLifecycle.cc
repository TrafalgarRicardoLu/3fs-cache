#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/config/Config.h"
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
  config.set_enable_phase4(true);
  ASSERT_ERROR(config.validateRuntime(), StatusCode::kInvalidConfig);
  config.set_enable_phase2(true);
  config.set_enable_phase3(true);
  ASSERT_OK(config.validateRuntime());
}

TEST(TestCacheManagerLifecycle, StartStopAndRepeatedStop) {
  auto config = makeConfig();
  CacheManagerOperator operator_(config, nullptr, nullptr);
  int starts = 0;
  int stops = 0;
  ASSERT_OK(operator_.startForTest(
      [&]() -> Result<Void> {
        ++starts;
        return Void{};
      },
      [&] { ++stops; }));
  ASSERT_TRUE(operator_.running());
  ASSERT_OK(operator_.startForTest([&]() -> Result<Void> {
    ++starts;
    return Void{};
  }));
  ASSERT_EQ(starts, 1);
  operator_.stop();
  operator_.stop();
  ASSERT_FALSE(operator_.running());
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
  ASSERT_EQ(rollbacks, 1);
  operator_.stop();
  ASSERT_EQ(rollbacks, 1);
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
