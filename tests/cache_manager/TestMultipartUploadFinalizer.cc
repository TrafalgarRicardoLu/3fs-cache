#include <deque>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/upload/MultipartUploadFinalizer.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::UploadJobRecord uploadingJob() {
  cache::UploadJobRecord job;
  job.jobId = cache::UploadJobId{Uuid::from(7, 8)};
  job.ownerUid = flat::Uid{1000};
  job.path = "/checkpoints/finalize";
  job.stagingInode = 19;
  job.stagingLength = 6;
  job.destination = {cache::OriginId{1}, "bucket", "job/final"};
  job.multipartId = "upload-1";
  job.parts = {{1, 4, "etag-1", "crc32c:1"}, {2, 2, "etag-2", "crc32c:2"}};
  job.nextPartNumber = 3;
  job.state = cache::UploadJobState::UPLOADING;
  job.stateVersion = 5;
  job.createdAtMs = 1;
  job.updatedAtMs = 5;
  return job;
}

cache::origin::ObjectMetadata completedObject(uint64_t size = 6) {
  return {{cache::OriginId{1}, "bucket", "job/final", {cache::VersionSelectorType::VERSION_ID, "version-1"}}, size};
}

class FakeFinalizerBackend : public MultipartUploadFinalizerBackend {
 public:
  explicit FakeFinalizerBackend(cache::UploadJobRecord job)
      : job(std::move(job)) {}

  CoTryTask<cache::origin::ObjectMetadata> complete(cache::origin::CompleteMultipartUploadRequest) final {
    ++completeCalls;
    if (!completeResults.empty()) {
      auto result = completeResults.front();
      completeResults.pop_front();
      co_return result;
    }
    co_return object;
  }

  CoTryTask<cache::origin::ObjectMetadata> headCompleted(cache::origin::HeadCompletedUploadRequest) final {
    ++headCalls;
    if (!headResults.empty()) {
      auto result = headResults.front();
      headResults.pop_front();
      co_return result;
    }
    co_return object;
  }

  CoTryTask<Void> abort(cache::origin::AbortMultipartUploadRequest) final {
    ++abortCalls;
    if (!abortResults.empty()) {
      auto result = abortResults.front();
      abortResults.pop_front();
      co_return result;
    }
    co_return Void{};
  }

  CoTryTask<cache::UploadJobRecord> mutate(cache::UploadJobRecord input,
                                           meta::MultipartUploadMutation mutation,
                                           std::optional<cache::ImmutableObjectIdentity> completed,
                                           std::string error) final {
    mutations.push_back(mutation);
    if (input.stateVersion != job.stateVersion) co_return makeError(CacheCode::kStateConflict);
    switch (mutation) {
      case meta::MultipartUploadMutation::PREPARE_COMPLETE:
        job.state = cache::UploadJobState::COMPLETING;
        break;
      case meta::MultipartUploadMutation::SAVE_COMPLETED:
        job.state = cache::UploadJobState::PUBLISHING;
        job.completedObject = std::move(completed);
        break;
      case meta::MultipartUploadMutation::BEGIN_ABORT:
        job.state = cache::UploadJobState::ABORTING;
        job.error = std::move(error);
        break;
      case meta::MultipartUploadMutation::FINISH_ABORT:
        job.state = cache::UploadJobState::CANCELLED;
        break;
      default:
        co_return makeError(StatusCode::kInvalidArg);
    }
    ++job.stateVersion;
    ++job.updatedAtMs;
    co_return job;
  }

  CoTryTask<Void> backoff(std::chrono::milliseconds delay) final {
    delays.push_back(delay);
    co_return Void{};
  }
  bool cancelled() const final { return stopped; }

  cache::UploadJobRecord job;
  cache::origin::ObjectMetadata object = completedObject();
  std::deque<Result<cache::origin::ObjectMetadata>> completeResults;
  std::deque<Result<cache::origin::ObjectMetadata>> headResults;
  std::deque<Result<Void>> abortResults;
  std::vector<meta::MultipartUploadMutation> mutations;
  std::vector<std::chrono::milliseconds> delays;
  bool stopped{false};
  int completeCalls{0};
  int headCalls{0};
  int abortCalls{0};
};

TEST(TestMultipartUploadFinalizer, CompleteTimeoutRecoversFromHead) {
  auto backend = std::make_shared<FakeFinalizerBackend>(uploadingJob());
  backend->completeResults.push_back(makeError(CacheCode::kTimeout, "ambiguous"));
  MultipartUploadFinalizer finalizer(backend);
  auto result = folly::coro::blockingWait(finalizer.complete(backend->job));
  ASSERT_OK(result);
  EXPECT_EQ(result->state, cache::UploadJobState::PUBLISHING);
  EXPECT_EQ(result->completedObject, backend->object.identity);
  EXPECT_EQ(backend->completeCalls, 1);
  EXPECT_EQ(backend->headCalls, 1);
}

TEST(TestMultipartUploadFinalizer, MissingHeadRetriesCompleteWithinBudget) {
  auto backend = std::make_shared<FakeFinalizerBackend>(uploadingJob());
  backend->completeResults.push_back(makeError(CacheCode::kTimeout, "ambiguous"));
  backend->headResults.push_back(makeError(CacheCode::kNotFound, "not visible"));
  MultipartUploadFinalizer finalizer(backend, {.maxRetries = 2});
  auto result = folly::coro::blockingWait(finalizer.complete(backend->job));
  ASSERT_OK(result);
  EXPECT_EQ(backend->completeCalls, 2);
  EXPECT_EQ(backend->headCalls, 1);
  EXPECT_EQ(backend->delays, std::vector<std::chrono::milliseconds>{std::chrono::milliseconds{100}});
}

TEST(TestMultipartUploadFinalizer, AuthenticationStopsWithoutRetryOrAbort) {
  auto backend = std::make_shared<FakeFinalizerBackend>(uploadingJob());
  backend->completeResults.push_back(makeError(CacheCode::kAccessDenied, "credentials"));
  MultipartUploadFinalizer finalizer(backend, {.maxRetries = 5});
  auto result = folly::coro::blockingWait(finalizer.complete(backend->job));
  ASSERT_ERROR(result, CacheCode::kAccessDenied);
  EXPECT_EQ(backend->completeCalls, 1);
  EXPECT_EQ(backend->headCalls, 0);
  EXPECT_EQ(backend->job.state, cache::UploadJobState::COMPLETING);
}

TEST(TestMultipartUploadFinalizer, MismatchedCompletedObjectPersistsAborting) {
  auto backend = std::make_shared<FakeFinalizerBackend>(uploadingJob());
  backend->object = completedObject(7);
  MultipartUploadFinalizer finalizer(backend);
  auto result = folly::coro::blockingWait(finalizer.complete(backend->job));
  ASSERT_ERROR(result, CacheCode::kInvalidResponse);
  EXPECT_EQ(backend->job.state, cache::UploadJobState::ABORTING);
  EXPECT_FALSE(backend->job.completedObject.has_value());
}

TEST(TestMultipartUploadFinalizer, AbortRetriesAndNoSuchUploadIsSuccess) {
  for (bool noSuchUpload : {false, true}) {
    auto job = uploadingJob();
    job.state = cache::UploadJobState::COMPLETING;
    auto backend = std::make_shared<FakeFinalizerBackend>(job);
    backend->abortResults.push_back(makeError(CacheCode::kTimeout, "ambiguous"));
    backend->abortResults.push_back(noSuchUpload ? Result<Void>(makeError(CacheCode::kNotFound, "gone"))
                                                 : Result<Void>(Void{}));
    MultipartUploadFinalizer finalizer(backend, {.maxRetries = 2});
    auto result = folly::coro::blockingWait(finalizer.cancel(backend->job, "cancelled by user"));
    ASSERT_OK(result);
    EXPECT_EQ(result->state, cache::UploadJobState::CANCELLED);
    EXPECT_EQ(backend->abortCalls, 2);
    EXPECT_EQ(backend->mutations,
              (std::vector<meta::MultipartUploadMutation>{meta::MultipartUploadMutation::BEGIN_ABORT,
                                                          meta::MultipartUploadMutation::FINISH_ABORT}));
  }
}

TEST(TestMultipartUploadFinalizer, CancellationDoesNotStartRemoteMutation) {
  auto backend = std::make_shared<FakeFinalizerBackend>(uploadingJob());
  backend->stopped = true;
  MultipartUploadFinalizer finalizer(backend);
  ASSERT_ERROR(folly::coro::blockingWait(finalizer.complete(backend->job)), MetaCode::kRequestCanceled);
  EXPECT_TRUE(backend->mutations.empty());
  EXPECT_EQ(backend->completeCalls, 0);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
