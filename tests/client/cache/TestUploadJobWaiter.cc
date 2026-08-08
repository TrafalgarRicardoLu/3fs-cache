#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "client/cache/UploadJobWaiter.h"
#include "common/utils/StatusCode.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::client::cache {
namespace {

hf3fs::cache::UploadJobRecord job(hf3fs::cache::UploadJobState state) {
  hf3fs::cache::UploadJobRecord result;
  result.jobId = hf3fs::cache::UploadJobId{Uuid::from(1, 2)};
  result.ownerUid = flat::Uid{7};
  result.path = "/waited";
  result.stagingInode = 42;
  result.stagingLength = 4;
  result.destination = {hf3fs::cache::OriginId{1}, "bucket", "object"};
  result.state = state;
  result.stateVersion = 3;
  result.createdAtMs = 1;
  result.updatedAtMs = 2;
  if (state == hf3fs::cache::UploadJobState::UPLOADING || state == hf3fs::cache::UploadJobState::COMPLETING) {
    result.multipartId = "multipart";
  }
  if (state == hf3fs::cache::UploadJobState::PUBLISHING || state == hf3fs::cache::UploadJobState::PUBLISHED) {
    result.multipartId = "multipart";
    result.parts = {{1, 4, "etag", "checksum"}};
    result.nextPartNumber = 2;
    result.completedObject =
        hf3fs::cache::ImmutableObjectIdentity{result.destination.originId,
                                              result.destination.bucket,
                                              result.destination.key,
                                              {hf3fs::cache::VersionSelectorType::VERSION_ID, "version"}};
  }
  if (state == hf3fs::cache::UploadJobState::PUBLISHED) result.publishedInode = 99;
  if (state == hf3fs::cache::UploadJobState::FAILED) result.error = "credential text must not escape";
  return result;
}

class FakeBackend : public UploadJobWaiterBackend {
 public:
  CoTryTask<hf3fs::cache::UploadJobRecord> get(flat::UserInfo, hf3fs::cache::UploadJobId) final {
    ++gets;
    if (transientFailures != 0) {
      --transientFailures;
      co_return makeError(RPCCode::kConnectFailed, "reconnecting");
    }
    auto index = std::min<size_t>(gets - 1 - initialFailures, responses.size() - 1);
    co_return responses.at(index);
  }

  CoTryTask<Void> wait(Duration delay) final {
    nowValue += delay.asUs();
    ++waits;
    if (onWait) onWait();
    co_return Void{};
  }

  std::chrono::steady_clock::time_point now() const final { return nowValue; }

  void failTransiently(uint32_t count) {
    transientFailures = count;
    initialFailures = count;
  }

  std::vector<hf3fs::cache::UploadJobRecord> responses{job(hf3fs::cache::UploadJobState::SEALED)};
  std::function<void()> onWait;
  std::chrono::steady_clock::time_point nowValue{};
  uint32_t transientFailures{0};
  uint32_t initialFailures{0};
  uint32_t gets{0};
  uint32_t waits{0};
};

UploadJobWaiterConfig config() { return {.pollInterval = 1_ms}; }

TEST(TestUploadJobWaiter, WaitsUntilPublishedAndReturnsDurableIdentity) {
  auto backend = std::make_shared<FakeBackend>();
  backend->responses = {job(hf3fs::cache::UploadJobState::SEALED),
                        job(hf3fs::cache::UploadJobState::PUBLISHING),
                        job(hf3fs::cache::UploadJobState::PUBLISHED)};
  UploadJobWaiter waiter(backend, config());
  auto result = folly::coro::blockingWait(waiter.awaitTerminal(flat::UserInfo{flat::Uid{7}, flat::Gid{7}},
                                                               backend->responses[0].jobId,
                                                               backend->now() + 1_s));
  ASSERT_OK(result);
  EXPECT_EQ(result->state, hf3fs::cache::UploadJobState::PUBLISHED);
  EXPECT_EQ(result->publishedInode, 99);
  EXPECT_EQ(backend->waits, 2);
}

TEST(TestUploadJobWaiter, TimeoutStillAllowsLaterResultQuery) {
  auto backend = std::make_shared<FakeBackend>();
  UploadJobWaiter waiter(backend, config());
  auto id = backend->responses.front().jobId;
  auto timedOut = folly::coro::blockingWait(
      waiter.awaitTerminal(flat::UserInfo{flat::Uid{7}, flat::Gid{7}}, id, backend->now() + 2_ms));
  ASSERT_ERROR(timedOut, CacheCode::kTimeout);
  EXPECT_EQ(StatusCode::toErrno(timedOut.error().code()), ETIMEDOUT);

  backend->responses = {job(hf3fs::cache::UploadJobState::PUBLISHED)};
  backend->gets = 0;
  auto queried = folly::coro::blockingWait(waiter.query(flat::UserInfo{flat::Uid{7}, flat::Gid{7}}, id));
  ASSERT_OK(queried);
  EXPECT_EQ(queried->publishedInode, 99);
}

TEST(TestUploadJobWaiter, RetriesClientReconnectAndHonorsCancellation) {
  auto backend = std::make_shared<FakeBackend>();
  backend->responses = {job(hf3fs::cache::UploadJobState::PUBLISHED)};
  backend->failTransiently(1);
  UploadJobWaiter waiter(backend, config());
  auto id = backend->responses.front().jobId;
  auto reconnected = folly::coro::blockingWait(
      waiter.awaitTerminal(flat::UserInfo{flat::Uid{7}, flat::Gid{7}}, id, backend->now() + 1_s));
  ASSERT_OK(reconnected);
  EXPECT_EQ(backend->gets, 2);

  backend = std::make_shared<FakeBackend>();
  bool cancelled = false;
  backend->onWait = [&] { cancelled = true; };
  UploadJobWaiter cancelling(backend, config());
  auto stopped = folly::coro::blockingWait(
      cancelling.awaitTerminal(flat::UserInfo{flat::Uid{7}, flat::Gid{7}}, id, backend->now() + 1_s, [&] {
        return cancelled;
      }));
  ASSERT_ERROR(stopped, MetaCode::kRequestCanceled);
}

TEST(TestUploadJobWaiter, MapsFailedAndCancelledToStableErrnos) {
  auto failedBackend = std::make_shared<FakeBackend>();
  failedBackend->responses = {job(hf3fs::cache::UploadJobState::FAILED)};
  UploadJobWaiter failed(failedBackend, config());
  auto failedResult = folly::coro::blockingWait(
      failed.awaitTerminal(flat::UserInfo{}, failedBackend->responses.front().jobId, failedBackend->now() + 1_s));
  ASSERT_ERROR(failedResult, CacheCode::kUnavailable);
  EXPECT_EQ(StatusCode::toErrno(failedResult.error().code()), EIO);
  EXPECT_EQ(failedResult.error().message(), "write-through publish failed");

  auto cancelledBackend = std::make_shared<FakeBackend>();
  cancelledBackend->responses = {job(hf3fs::cache::UploadJobState::CANCELLED)};
  UploadJobWaiter cancelled(cancelledBackend, config());
  auto cancelledResult = folly::coro::blockingWait(cancelled.awaitTerminal(flat::UserInfo{},
                                                                           cancelledBackend->responses.front().jobId,
                                                                           cancelledBackend->now() + 1_s));
  ASSERT_ERROR(cancelledResult, MetaCode::kRequestCanceled);
  EXPECT_EQ(StatusCode::toErrno(cancelledResult.error().code()), EINTR);
}

}  // namespace
}  // namespace hf3fs::client::cache
