#include <deque>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>

#include "cache_manager/upload/MultipartUploader.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::UploadJobRecord sealedJob(uint64_t length = 10) {
  cache::UploadJobRecord job;
  job.jobId = cache::UploadJobId{Uuid::from(1, 2)};
  job.ownerUid = flat::Uid{1000};
  job.path = "/checkpoints/model";
  job.stagingInode = 17;
  job.stagingLength = length;
  job.destination = {cache::OriginId{1}, "bucket", "job/object"};
  job.state = cache::UploadJobState::SEALED;
  job.stateVersion = 2;
  job.createdAtMs = 1;
  job.updatedAtMs = 2;
  return job;
}

class FakeBackend : public MultipartUploaderBackend {
 public:
  explicit FakeBackend(cache::UploadJobRecord job)
      : job(std::move(job)),
        data(this->job.stagingLength) {
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<uint8_t>(i);
  }

  CoTryTask<std::vector<uint8_t>> readStaging(uint64_t inode, cache::ByteRange range) final {
    reads.push_back(range);
    if (inode != job.stagingInode || range.offset > data.size() || range.length > data.size() - range.offset) {
      co_return makeError(StatusCode::kInvalidArg);
    }
    co_return std::vector<uint8_t>(data.begin() + range.offset, data.begin() + range.offset + range.length);
  }

  CoTryTask<cache::origin::MultipartUpload> createMultipartUpload(const cache::ObjectRef &destination) final {
    ++creates;
    co_return cache::origin::MultipartUpload{destination, "upload-1"};
  }

  CoTryTask<cache::origin::UploadPartResult> uploadPart(cache::origin::UploadPartRequest request) final {
    uploadNumbers.push_back(request.partNumber);
    if (!uploadFailures.empty()) {
      auto failure = uploadFailures.front();
      uploadFailures.pop_front();
      co_return makeError(failure);
    }
    uploaded[request.partNumber] = request.body;
    cache::CompletedUploadPart part{request.partNumber,
                                    request.body.size(),
                                    "etag-" + std::to_string(request.partNumber),
                                    request.checksum};
    co_return cache::origin::UploadPartResult{std::move(part)};
  }

  CoTryTask<cache::UploadJobRecord> beginMultipartUpload(cache::UploadJobId jobId,
                                                         uint64_t expectedStateVersion,
                                                         std::string multipartId) final {
    ++begins;
    if (!beginFailures.empty()) {
      auto failure = beginFailures.front();
      beginFailures.pop_front();
      co_return makeError(failure);
    }
    if (job.jobId != jobId) co_return makeError(CacheCode::kStateConflict);
    if (job.state == cache::UploadJobState::UPLOADING && job.stateVersion == expectedStateVersion + 1 &&
        job.multipartId == multipartId) {
      co_return job;
    }
    if (job.state != cache::UploadJobState::SEALED || job.stateVersion != expectedStateVersion) {
      co_return makeError(CacheCode::kStateConflict);
    }
    job.state = cache::UploadJobState::UPLOADING;
    job.multipartId = std::move(multipartId);
    ++job.stateVersion;
    ++job.updatedAtMs;
    co_return job;
  }

  CoTryTask<cache::UploadJobRecord> checkpointUploadPart(cache::UploadJobId jobId,
                                                         uint64_t expectedStateVersion,
                                                         std::string multipartId,
                                                         cache::CompletedUploadPart part) final {
    ++checkpointCalls;
    if (!checkpointFailures.empty()) {
      auto failure = checkpointFailures.front();
      checkpointFailures.pop_front();
      co_return makeError(failure);
    }
    if (job.jobId != jobId || job.multipartId != multipartId) co_return makeError(CacheCode::kStateConflict);
    if (job.stateVersion == expectedStateVersion + 1 && part.partNumber <= job.parts.size() &&
        job.parts[part.partNumber - 1] == part) {
      co_return job;
    }
    if (job.stateVersion != expectedStateVersion || part.partNumber != job.nextPartNumber) {
      co_return makeError(CacheCode::kStateConflict);
    }
    job.parts.push_back(std::move(part));
    ++job.nextPartNumber;
    ++job.stateVersion;
    ++job.updatedAtMs;
    if (cancelAfterCheckpoints != 0 && job.parts.size() >= cancelAfterCheckpoints) stopped = true;
    co_return job;
  }

  CoTryTask<Void> backoff(std::chrono::milliseconds delay) final {
    delays.push_back(delay);
    co_return Void{};
  }

  bool cancelled() const final { return stopped; }

  cache::UploadJobRecord job;
  std::vector<uint8_t> data;
  std::map<uint32_t, std::vector<uint8_t>> uploaded;
  std::vector<cache::ByteRange> reads;
  std::vector<uint32_t> uploadNumbers;
  std::vector<std::chrono::milliseconds> delays;
  std::deque<Status> uploadFailures;
  std::deque<Status> beginFailures;
  std::deque<Status> checkpointFailures;
  size_t cancelAfterCheckpoints{0};
  bool stopped{false};
  int creates{0};
  int begins{0};
  int checkpointCalls{0};
};

TEST(TestMultipartUploader, CheckpointsEveryPartAndTail) {
  auto backend = std::make_shared<FakeBackend>(sealedJob());
  MultipartUploader uploader(backend, {.partSize = 4, .maxRetries = 2});
  auto result = folly::coro::blockingWait(uploader.upload(backend->job));
  ASSERT_OK(result);
  ASSERT_EQ(result->state, cache::UploadJobState::UPLOADING);
  ASSERT_EQ(result->nextPartNumber, 4);
  ASSERT_EQ(result->parts.size(), 3);
  EXPECT_EQ(result->parts[0].size, 4);
  EXPECT_EQ(result->parts[1].size, 4);
  EXPECT_EQ(result->parts[2].size, 2);
  EXPECT_EQ(backend->reads, (std::vector<cache::ByteRange>{{0, 4}, {4, 4}, {8, 2}}));
}

TEST(TestMultipartUploader, RestartContinuesAfterLastCheckpoint) {
  auto backend = std::make_shared<FakeBackend>(sealedJob());
  backend->cancelAfterCheckpoints = 1;
  MultipartUploader first(backend, {.partSize = 4});
  auto interrupted = folly::coro::blockingWait(first.upload(backend->job));
  ASSERT_ERROR(interrupted, MetaCode::kRequestCanceled);
  ASSERT_EQ(backend->job.parts.size(), 1);

  backend->stopped = false;
  backend->cancelAfterCheckpoints = 0;
  backend->reads.clear();
  MultipartUploader resumed(backend, {.partSize = 4});
  auto result = folly::coro::blockingWait(resumed.upload(backend->job));
  ASSERT_OK(result);
  EXPECT_EQ(backend->creates, 1);
  EXPECT_EQ(backend->reads, (std::vector<cache::ByteRange>{{4, 4}, {8, 2}}));
}

TEST(TestMultipartUploader, ReuploadsSamePartAfterCrashBeforeCheckpoint) {
  auto backend = std::make_shared<FakeBackend>(sealedJob(4));
  backend->checkpointFailures.push_back(Status(CacheCode::kInvalidResponse, "crash boundary"));
  MultipartUploader first(backend, {.partSize = 4});
  auto interrupted = folly::coro::blockingWait(first.upload(backend->job));
  ASSERT_ERROR(interrupted, CacheCode::kInvalidResponse);
  ASSERT_TRUE(backend->job.parts.empty());

  MultipartUploader resumed(backend, {.partSize = 4});
  auto result = folly::coro::blockingWait(resumed.upload(backend->job));
  ASSERT_OK(result);
  EXPECT_EQ(backend->uploadNumbers, (std::vector<uint32_t>{1, 1}));
  EXPECT_EQ(backend->creates, 1);
}

TEST(TestMultipartUploader, RetryUsesSamePartAndHonorsBudget) {
  auto backend = std::make_shared<FakeBackend>(sealedJob(4));
  backend->uploadFailures = {Status(CacheCode::kThrottled, "slow"),
                             Status(CacheCode::kTimeout, "timeout"),
                             Status(CacheCode::kUnavailable, "down")};
  MultipartUploader uploader(backend, {.partSize = 4, .maxRetries = 2});
  auto result = folly::coro::blockingWait(uploader.upload(backend->job));
  ASSERT_ERROR(result, CacheCode::kUnavailable);
  EXPECT_EQ(backend->uploadNumbers, (std::vector<uint32_t>{1, 1, 1}));
  EXPECT_EQ(backend->delays,
            (std::vector<std::chrono::milliseconds>{std::chrono::milliseconds{100}, std::chrono::milliseconds{200}}));
  EXPECT_TRUE(backend->job.parts.empty());
}

TEST(TestMultipartUploader, DoesNotRetryAuthenticationOrInvalidResponse) {
  for (auto code : {CacheCode::kAccessDenied, CacheCode::kInvalidResponse}) {
    auto backend = std::make_shared<FakeBackend>(sealedJob(4));
    backend->uploadFailures.push_back(Status(code, "permanent"));
    MultipartUploader uploader(backend, {.partSize = 4, .maxRetries = 5});
    auto result = folly::coro::blockingWait(uploader.upload(backend->job));
    ASSERT_ERROR(result, code);
    EXPECT_EQ(backend->uploadNumbers.size(), 1);
    EXPECT_TRUE(backend->delays.empty());
  }
}

TEST(TestMultipartUploader, CancellationInterruptsRetryBackoffBoundary) {
  auto backend = std::make_shared<FakeBackend>(sealedJob(4));
  backend->uploadFailures.push_back(Status(CacheCode::kTimeout, "timeout"));
  backend->stopped = true;
  MultipartUploader uploader(backend, {.partSize = 4});
  auto result = folly::coro::blockingWait(uploader.upload(backend->job));
  ASSERT_ERROR(result, MetaCode::kRequestCanceled);
  EXPECT_TRUE(backend->uploadNumbers.empty());
}

TEST(TestMultipartUploader, RejectsChangedCheckpointShapeAndSupportsEmptyTail) {
  auto invalid = sealedJob(5);
  invalid.state = cache::UploadJobState::UPLOADING;
  invalid.multipartId = "upload-1";
  invalid.parts.push_back({1, 3, "etag", "crc32c:1"});
  invalid.nextPartNumber = 2;
  ASSERT_ERROR(MultipartUploader::validateProgress(invalid, 4), CacheCode::kStateConflict);

  auto backend = std::make_shared<FakeBackend>(sealedJob(0));
  MultipartUploader uploader(backend, {.partSize = 4});
  auto result = folly::coro::blockingWait(uploader.upload(backend->job));
  ASSERT_OK(result);
  ASSERT_EQ(result->parts.size(), 1);
  EXPECT_EQ(result->parts.front().size, 0);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
