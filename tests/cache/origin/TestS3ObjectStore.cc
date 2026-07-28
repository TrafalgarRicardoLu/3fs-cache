#include <deque>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>
#include <memory>

#include "cache/origin/s3/S3ObjectStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::origin::s3 {
namespace {

class FakeExecutor : public S3RequestExecutor {
 public:
  S3Outcome<HeadResponse> head(const HeadRequest &) override {
    ++headCalls;
    auto outcome = std::move(headOutcomes.front());
    headOutcomes.pop_front();
    return outcome;
  }

  S3Outcome<GetRangeResponse> getRange(const GetRangeRequest &request) override {
    ++getCalls;
    lastRange = request.range;
    auto outcome = std::move(getOutcomes.front());
    getOutcomes.pop_front();
    return outcome;
  }

  uint32_t headCalls{0};
  uint32_t getCalls{0};
  ByteRange lastRange;
  std::deque<S3Outcome<HeadResponse>> headOutcomes;
  std::deque<S3Outcome<GetRangeResponse>> getOutcomes;
};

ObjectRef objectRef() { return ObjectRef{OriginId{1}, "bucket", "key"}; }

ImmutableObjectIdentity identity(VersionSelectorType type = VersionSelectorType::VERSION_ID) {
  return ImmutableObjectIdentity{OriginId{1}, "bucket", "key", VersionSelector{type, "version-1"}};
}

S3ObjectStoreConfig testConfig() {
  S3ObjectStoreConfig config;
  config.ioThreads = 1;
  config.maxRetries = 2;
  config.retryDelay = Duration::zero();
  config.totalTimeout = 1_s;
  return config;
}

TEST(S3ObjectStore, HeadRetriesAndPrefersVersionId) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  fake->headOutcomes.emplace_back(S3Failure{S3FailureKind::TIMEOUT, 0, "timeout"});
  fake->headOutcomes.emplace_back(HeadResponse{42, "version-1", "\"etag\""});
  S3ObjectStore store(testConfig(), std::move(executor));

  auto result = folly::coro::blockingWait(store.head(objectRef()));
  ASSERT_OK(result);
  EXPECT_EQ(result->size, uint64_t{42});
  EXPECT_EQ(result->identity.version.type, VersionSelectorType::VERSION_ID);
  EXPECT_EQ(result->identity.version.value, "version-1");
  EXPECT_EQ(fake->headCalls, uint32_t{2});
}

TEST(S3ObjectStore, HeadNormalizesStrongEtagAndRejectsWeakEtag) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  fake->headOutcomes.emplace_back(HeadResponse{10, std::nullopt, "\"strong\""});
  fake->headOutcomes.emplace_back(HeadResponse{10, std::nullopt, "W/\"weak\""});
  S3ObjectStore store(testConfig(), std::move(executor));

  auto strong = folly::coro::blockingWait(store.head(objectRef()));
  ASSERT_OK(strong);
  EXPECT_EQ(strong->identity.version.type, VersionSelectorType::STRONG_ETAG);
  EXPECT_EQ(strong->identity.version.value, "strong");
  auto weak = folly::coro::blockingWait(store.head(objectRef()));
  ASSERT_ERROR(weak, CacheCode::kVersionMismatch);
}

TEST(S3ObjectStore, ValidatesRangeBodyContentRangeAndVersion) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  fake->getOutcomes.emplace_back(GetRangeResponse{206, "bytes 5-7/100", "version-1", std::nullopt, {1, 2, 3}});
  fake->getOutcomes.emplace_back(GetRangeResponse{200, "", "version-1", std::nullopt, {1, 2, 3}});
  fake->getOutcomes.emplace_back(GetRangeResponse{206, "bytes 5-7/100", "version-1", std::nullopt, {1, 2}});
  fake->getOutcomes.emplace_back(GetRangeResponse{206, "bytes 5-7/invalid", "version-1", std::nullopt, {1, 2, 3}});
  fake->getOutcomes.emplace_back(GetRangeResponse{206, "bytes 5-7/100", "version-2", std::nullopt, {1, 2, 3}});
  S3ObjectStore store(testConfig(), std::move(executor));

  auto valid = folly::coro::blockingWait(store.getRange(identity(), ByteRange{5, 3}));
  ASSERT_OK(valid);
  EXPECT_EQ(*valid, (std::vector<uint8_t>{1, 2, 3}));
  auto wrongStatus = folly::coro::blockingWait(store.getRange(identity(), ByteRange{5, 3}));
  ASSERT_ERROR(wrongStatus, CacheCode::kInvalidResponse);
  auto wrongLength = folly::coro::blockingWait(store.getRange(identity(), ByteRange{5, 3}));
  ASSERT_ERROR(wrongLength, CacheCode::kInvalidResponse);
  auto wrongContentRange = folly::coro::blockingWait(store.getRange(identity(), ByteRange{5, 3}));
  ASSERT_ERROR(wrongContentRange, CacheCode::kInvalidResponse);
  auto wrongVersion = folly::coro::blockingWait(store.getRange(identity(), ByteRange{5, 3}));
  ASSERT_ERROR(wrongVersion, CacheCode::kVersionMismatch);
  EXPECT_EQ(fake->getCalls, uint32_t{5});
}

TEST(S3ObjectStore, RetriesOnlyTransientFailuresAndPreservesTerminalError) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  fake->headOutcomes.emplace_back(S3Failure{S3FailureKind::THROTTLED, 429, "slow down"});
  fake->headOutcomes.emplace_back(S3Failure{S3FailureKind::SERVER, 503, "unavailable"});
  fake->headOutcomes.emplace_back(HeadResponse{10, "version-1", std::nullopt});
  S3ObjectStore store(testConfig(), std::move(executor));

  auto recovered = folly::coro::blockingWait(store.head(objectRef()));
  ASSERT_OK(recovered);
  EXPECT_EQ(fake->headCalls, uint32_t{3});

  auto timeoutExecutor = std::make_unique<FakeExecutor>();
  timeoutExecutor->headOutcomes.emplace_back(S3Failure{S3FailureKind::TIMEOUT, 0, "timeout"});
  auto config = testConfig();
  config.maxRetries = 0;
  S3ObjectStore timeoutStore(config, std::move(timeoutExecutor));
  auto timeout = folly::coro::blockingWait(timeoutStore.head(objectRef()));
  ASSERT_ERROR(timeout, CacheCode::kTimeout);

  auto invalidExecutor = std::make_unique<FakeExecutor>();
  invalidExecutor->headOutcomes.emplace_back(S3Failure{S3FailureKind::INVALID_RESPONSE, 200, "invalid content length"});
  S3ObjectStore invalidStore(testConfig(), std::move(invalidExecutor));
  auto invalid = folly::coro::blockingWait(invalidStore.head(objectRef()));
  ASSERT_ERROR(invalid, CacheCode::kInvalidResponse);
}

TEST(S3ObjectStore, DoesNotRetryPermanentFailureAndBoundsInflightBytes) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  fake->getOutcomes.emplace_back(S3Failure{S3FailureKind::AUTHENTICATION, 403, "denied"});
  fake->getOutcomes.emplace_back(S3Failure{S3FailureKind::NOT_FOUND, 404, "missing"});
  auto config = testConfig();
  config.maxInflightBytes = 4;
  S3ObjectStore store(config, std::move(executor));

  auto denied = folly::coro::blockingWait(store.getRange(identity(), ByteRange{0, 1}));
  ASSERT_ERROR(denied, CacheCode::kAccessDenied);
  EXPECT_EQ(fake->getCalls, uint32_t{1});
  auto missing = folly::coro::blockingWait(store.getRange(identity(), ByteRange{0, 1}));
  ASSERT_ERROR(missing, CacheCode::kNotFound);
  EXPECT_EQ(fake->getCalls, uint32_t{2});
  auto tooLarge = folly::coro::blockingWait(store.getRange(identity(), ByteRange{0, 5}));
  ASSERT_ERROR(tooLarge, CacheCode::kRequestTooLarge);
  EXPECT_EQ(fake->getCalls, uint32_t{2});
}

TEST(S3ObjectStore, EmptyRangeSkipsBackendAndOverflowIsRejected) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  S3ObjectStore store(testConfig(), std::move(executor));

  auto empty = folly::coro::blockingWait(store.getRange(identity(), ByteRange{7, 0}));
  ASSERT_OK(empty);
  EXPECT_TRUE(empty->empty());
  auto overflow = folly::coro::blockingWait(
      store.getRange(identity(), ByteRange{std::numeric_limits<uint64_t>::max(), uint64_t{2}}));
  ASSERT_ERROR(overflow, StatusCode::kInvalidArg);
  EXPECT_EQ(fake->getCalls, uint32_t{0});
}

TEST(S3ObjectStore, RejectsZeroConcurrencyWithoutCallingBackend) {
  auto executor = std::make_unique<FakeExecutor>();
  auto *fake = executor.get();
  auto config = testConfig();
  config.maxConcurrentRequests = 0;
  S3ObjectStore store(config, std::move(executor));

  auto result = folly::coro::blockingWait(store.head(objectRef()));
  ASSERT_ERROR(result, StatusCode::kInvalidConfig);
  EXPECT_EQ(fake->headCalls, uint32_t{0});
}

#ifndef HF3FS_ENABLE_CACHE
TEST(S3ObjectStore, AwsFactoryIsFailClosedWhenBackendIsDisabled) {
  auto result = S3ObjectStore::createAws(testConfig(), AwsS3ClientConfig{});
  ASSERT_ERROR(result, CacheCode::kFeatureDisabled);
}
#endif

}  // namespace
}  // namespace hf3fs::cache::origin::s3
