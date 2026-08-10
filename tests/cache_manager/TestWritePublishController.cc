#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <functional>
#include <gtest/gtest.h>
#include <set>

#include "cache/metrics/CacheMetrics.h"
#include "cache_manager/upload/WritePublishController.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager {
namespace {

cache::UploadJobRecord job(uint64_t id, cache::UploadJobState state, uint32_t owner = 1, uint32_t origin = 1) {
  cache::UploadJobRecord result;
  result.jobId = cache::UploadJobId{Uuid::from(1, id)};
  result.ownerUid = flat::Uid{owner};
  result.path = "/upload-" + std::to_string(id);
  result.stagingInode = 100 + id;
  result.stagingLength = 4;
  result.destination = {cache::OriginId{origin}, "bucket", "object-" + std::to_string(id)};
  result.state = state;
  result.stateVersion = id + 1;
  result.createdAtMs = 1;
  result.updatedAtMs = 1;
  if (state == cache::UploadJobState::OPEN) {
    result.writerLeaseId = Uuid::from(9, id);
    result.writerLeaseExpiresAtMs = 2;
  }
  if (state == cache::UploadJobState::UPLOADING || state == cache::UploadJobState::COMPLETING ||
      state == cache::UploadJobState::ABORTING) {
    result.multipartId = "multipart-" + std::to_string(id);
  }
  if (state == cache::UploadJobState::COMPLETING || state == cache::UploadJobState::PUBLISHING ||
      state == cache::UploadJobState::PUBLISHED) {
    result.multipartId = "multipart-" + std::to_string(id);
    result.parts = {{1, 4, "etag", "checksum"}};
    result.nextPartNumber = 2;
  }
  if (state == cache::UploadJobState::PUBLISHING || state == cache::UploadJobState::PUBLISHED) {
    result.completedObject = cache::ImmutableObjectIdentity{result.destination.originId,
                                                            result.destination.bucket,
                                                            result.destination.key,
                                                            {cache::VersionSelectorType::VERSION_ID, "version"}};
  }
  if (state == cache::UploadJobState::PUBLISHED) result.publishedInode = 1000 + id;
  return result;
}

WritePublishControllerConfig config() {
  WritePublishControllerConfig result;
  result.pageSize = 2;
  result.globalConcurrency = 8;
  result.perOwnerConcurrency = 8;
  result.perOriginConcurrency = 8;
  result.cacheTableId = flat::ChainTableId{2};
  result.cacheBlockSize = 4096;
  result.cacheStripeSize = 1;
  result.uploader.partSize = 4;
  return result;
}

class FakeBackend : public WritePublishControllerBackend {
 public:
  CoTryTask<UploadJobPage> list(std::optional<cache::UploadJobId> after,
                                uint32_t limit,
                                bool includeTerminal) override {
    ++listCalls;
    includeTerminalRequests.push_back(includeTerminal);
    std::vector<cache::UploadJobRecord> visible;
    for (const auto &value : jobs) {
      auto terminal = value.state == cache::UploadJobState::FAILED || value.state == cache::UploadJobState::CANCELLED ||
                      std::find(warmedSubmitted.begin(), warmedSubmitted.end(), value.jobId) != warmedSubmitted.end();
      if (includeTerminal || !terminal) visible.push_back(value);
    }
    size_t begin = 0;
    if (after) {
      while (begin < visible.size() && visible[begin].jobId != *after) ++begin;
      if (begin < visible.size()) ++begin;
    }
    auto end = std::min(visible.size(), begin + limit);
    UploadJobPage page;
    page.jobs.insert(page.jobs.end(), visible.begin() + begin, visible.begin() + end);
    page.more = end < visible.size();
    co_return page;
  }

  CoTryTask<UploadJobPage> listExpiredOpen(std::optional<meta::UploadOpenLeaseCursor> after,
                                           uint64_t expiresBeforeMs,
                                           uint32_t limit) override {
    std::vector<cache::UploadJobRecord> visible;
    for (const auto &value : jobs) {
      if (value.state == cache::UploadJobState::OPEN && value.writerLeaseExpiresAtMs <= expiresBeforeMs) {
        visible.push_back(value);
      }
    }
    size_t begin = 0;
    if (after) {
      while (begin < visible.size() && visible[begin].jobId != after->jobId) ++begin;
      if (begin < visible.size()) ++begin;
    }
    auto end = std::min(visible.size(), begin + limit);
    UploadJobPage page;
    page.jobs.insert(page.jobs.end(), visible.begin() + begin, visible.begin() + end);
    page.more = end < visible.size();
    co_return page;
  }

  CoTryTask<cache::UploadJobRecord> recoverOpen(cache::UploadJobRecord value) override {
    recoveredOpen.push_back(value.jobId);
    value.state = cache::UploadJobState::SEALED;
    value.stagingLength = 4;
    value.writerLeaseId = Uuid::zero();
    value.writerLeaseExpiresAtMs = 0;
    ++value.stateVersion;
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> finalizeCancelled(cache::UploadJobRecord value) override {
    finalizedCancelled.push_back(value.jobId);
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> upload(cache::UploadJobRecord value) final {
    uploaded.push_back(value.jobId);
    if (failUpload && value.jobId == *failUpload)
      co_return makeError(CacheCode::kUnavailable, "injected upload failure");
    value.state = cache::UploadJobState::UPLOADING;
    value.multipartId = value.multipartId.empty() ? "multipart" : value.multipartId;
    if (value.parts.empty()) {
      value.parts = {{1, value.stagingLength, "etag", "checksum"}};
      value.nextPartNumber = 2;
    }
    ++value.stateVersion;
    if (onUpload) onUpload();
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> complete(cache::UploadJobRecord value) final {
    completed.push_back(value.jobId);
    value.state = cache::UploadJobState::PUBLISHING;
    value.completedObject = cache::ImmutableObjectIdentity{value.destination.originId,
                                                           value.destination.bucket,
                                                           value.destination.key,
                                                           {cache::VersionSelectorType::VERSION_ID, "version"}};
    ++value.stateVersion;
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> publish(cache::UploadJobRecord value) final {
    published.push_back(value.jobId);
    if (failPublishPermanent && value.jobId == *failPublishPermanent) {
      co_return makeError(CacheCode::kAccessDenied, "injected permanent publish failure");
    }
    value.state = cache::UploadJobState::PUBLISHED;
    value.publishedInode = value.stagingInode + 1000;
    ++value.stateVersion;
    if (failPublishAfterCommit && value.jobId == *failPublishAfterCommit) {
      auto found = std::find_if(jobs.begin(), jobs.end(), [&](const auto &job) { return job.jobId == value.jobId; });
      if (found != jobs.end()) *found = value;
      failPublishAfterCommit.reset();
      co_return makeError(CacheCode::kTimeout, "injected lost publish response");
    }
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> failPublish(cache::UploadJobRecord value, const Status &) final {
    value.state = cache::UploadJobState::FAILED;
    value.error = "publish failed";
    value.orphanCleanupState = cache::OrphanCleanupState::PENDING;
    value.orphanCleanupEligibleAtMs = 1;
    ++value.stateVersion;
    auto found = std::find_if(jobs.begin(), jobs.end(), [&](const auto &job) { return job.jobId == value.jobId; });
    if (found != jobs.end()) *found = value;
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> cleanupOrphan(cache::UploadJobRecord value) final {
    cleanedOrphans.push_back(value.jobId);
    value.orphanCleanupState = cache::OrphanCleanupState::COMPLETE;
    value.orphanCleanupOperationId = Uuid::from(1, 1);
    value.orphanCleanupEligibleAtMs = value.orphanCleanupEligibleAtMs == 0 ? 1 : value.orphanCleanupEligibleAtMs;
    ++value.orphanCleanupAttempts;
    ++value.stateVersion;
    auto found = std::find_if(jobs.begin(), jobs.end(), [&](const auto &job) { return job.jobId == value.jobId; });
    if (found != jobs.end()) *found = value;
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> warm(cache::UploadJobRecord value) final {
    warmed.push_back(value.jobId);
    if (failWarm && value.jobId == *failWarm) co_return makeError(CacheCode::kUnavailable, "injected prefetch failure");
    ++value.stateVersion;
    warmedSubmitted.push_back(value.jobId);
    auto found = std::find_if(jobs.begin(), jobs.end(), [&](const auto &job) { return job.jobId == value.jobId; });
    if (found != jobs.end()) *found = value;
    co_return value;
  }

  CoTryTask<cache::UploadJobRecord> abort(cache::UploadJobRecord value) final {
    aborted.push_back(value.jobId);
    value.state = cache::UploadJobState::CANCELLED;
    ++value.stateVersion;
    co_return value;
  }

  void stop() final {
    stopped = true;
    ++stopCalls;
  }

  std::vector<cache::UploadJobRecord> jobs;
  std::vector<cache::UploadJobId> uploaded;
  std::vector<cache::UploadJobId> completed;
  std::vector<cache::UploadJobId> published;
  std::vector<cache::UploadJobId> warmed;
  std::vector<cache::UploadJobId> aborted;
  std::vector<cache::UploadJobId> recoveredOpen;
  std::vector<cache::UploadJobId> finalizedCancelled;
  std::vector<cache::UploadJobId> cleanedOrphans;
  std::vector<cache::UploadJobId> warmedSubmitted;
  std::function<void()> onUpload;
  std::optional<cache::UploadJobId> failUpload;
  std::optional<cache::UploadJobId> failWarm;
  std::optional<cache::UploadJobId> failPublishAfterCommit;
  std::optional<cache::UploadJobId> failPublishPermanent;
  uint32_t listCalls{0};
  uint32_t stopCalls{0};
  std::vector<bool> includeTerminalRequests;
  bool stopped{false};
};

TEST(TestWritePublishController, RecoversEveryDurableStateAcrossPages) {
  cache::metrics::resetForTest();
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(1, cache::UploadJobState::SEALED),
                   job(2, cache::UploadJobState::UPLOADING),
                   job(3, cache::UploadJobState::COMPLETING),
                   job(4, cache::UploadJobState::PUBLISHING),
                   job(5, cache::UploadJobState::ABORTING),
                   job(6, cache::UploadJobState::OPEN),
                   job(7, cache::UploadJobState::PUBLISHED)};
  WritePublishController controller(backend, config());
  auto result = folly::coro::blockingWait(controller.recover());
  ASSERT_OK(result);
  EXPECT_EQ(result->scanned, 7);
  EXPECT_EQ(result->scheduled, 7);
  EXPECT_EQ(result->completed, 7);
  EXPECT_EQ(result->failed, 0);
  EXPECT_EQ(backend->listCalls, 8);
  EXPECT_EQ(std::count(backend->includeTerminalRequests.begin(), backend->includeTerminalRequests.end(), true), 4);
  EXPECT_EQ(backend->recoveredOpen.size(), 1);
  EXPECT_EQ(backend->uploaded.size(), 3);
  EXPECT_EQ(backend->completed.size(), 4);
  EXPECT_EQ(backend->published.size(), 5);
  EXPECT_EQ(backend->warmed.size(), 6);
  EXPECT_EQ(backend->aborted.size(), 1);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_UPLOAD_RUN), 1);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_UPLOAD_SCANNED), 7);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_UPLOAD_SCHEDULED), 7);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_UPLOAD_COMPLETED), 7);
  EXPECT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_UPLOAD_FAILED), 0);
  EXPECT_EQ(cache::metrics::lastTagsForTest(cache::metrics::Event::MANAGER_UPLOAD_RUN).reason, "complete");
}

TEST(TestWritePublishController, AppliesGlobalOwnerAndOriginLimitsFairly) {
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(1, cache::UploadJobState::SEALED, 1, 1),
                   job(2, cache::UploadJobState::SEALED, 1, 2),
                   job(3, cache::UploadJobState::SEALED, 2, 1),
                   job(4, cache::UploadJobState::SEALED, 2, 2),
                   job(5, cache::UploadJobState::SEALED, 3, 3)};
  auto limits = config();
  limits.globalConcurrency = 3;
  limits.perOwnerConcurrency = 1;
  limits.perOriginConcurrency = 1;
  WritePublishController controller(backend, limits);
  auto result = folly::coro::blockingWait(controller.runOnce());
  ASSERT_OK(result);
  EXPECT_EQ(result->scheduled, 3);
  ASSERT_EQ(backend->uploaded.size(), 3);
  std::set<flat::Uid> owners;
  std::set<cache::OriginId> origins;
  for (auto id : backend->uploaded) {
    auto found =
        std::find_if(backend->jobs.begin(), backend->jobs.end(), [&](const auto &value) { return value.jobId == id; });
    ASSERT_NE(found, backend->jobs.end());
    owners.insert(found->ownerUid);
    origins.insert(found->destination.originId);
  }
  EXPECT_EQ(owners.size(), 3);
  EXPECT_EQ(origins.size(), 3);
}

TEST(TestWritePublishController, StopDoesNotStartTheNextStageAndRestartResumesCheckpoint) {
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(1, cache::UploadJobState::SEALED)};
  auto first = std::make_unique<WritePublishController>(backend, config());
  backend->onUpload = [&] { first->stop(); };
  auto stopped = folly::coro::blockingWait(first->runOnce());
  ASSERT_OK(stopped);
  EXPECT_TRUE(stopped->stopped);
  EXPECT_EQ(backend->uploaded.size(), 1);
  EXPECT_TRUE(backend->completed.empty());
  EXPECT_EQ(backend->stopCalls, 1);

  backend->onUpload = {};
  backend->jobs = {job(1, cache::UploadJobState::UPLOADING)};
  WritePublishController restarted(backend, config());
  auto recovered = folly::coro::blockingWait(restarted.recover());
  ASSERT_OK(recovered);
  EXPECT_EQ(recovered->completed, 1);
  EXPECT_EQ(backend->completed.size(), 1);
  EXPECT_EQ(backend->published.size(), 1);
}

TEST(TestWritePublishController, IsolatesJobFailureAndContinuesOtherOwners) {
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(1, cache::UploadJobState::SEALED, 1), job(2, cache::UploadJobState::SEALED, 2)};
  backend->failUpload = backend->jobs.front().jobId;
  WritePublishController controller(backend, config());
  auto result = folly::coro::blockingWait(controller.runOnce());
  ASSERT_OK(result);
  EXPECT_EQ(result->scheduled, 2);
  EXPECT_EQ(result->failed, 1);
  EXPECT_EQ(result->completed, 1);
  EXPECT_EQ(backend->published, std::vector<cache::UploadJobId>{backend->jobs.back().jobId});
}

TEST(TestWritePublishController, PublishedPrefetchFailureDoesNotRepublishAndRestartRetriesWarmCheckpoint) {
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(8, cache::UploadJobState::PUBLISHED)};
  backend->failWarm = backend->jobs.front().jobId;
  WritePublishController first(backend, config());
  auto failed = folly::coro::blockingWait(first.runOnce());
  ASSERT_OK(failed);
  EXPECT_EQ(failed->failed, 1);
  EXPECT_TRUE(backend->published.empty());
  EXPECT_EQ(backend->warmed.size(), 1);

  backend->failWarm.reset();
  WritePublishController restarted(backend, config());
  auto recovered = folly::coro::blockingWait(restarted.recover());
  ASSERT_OK(recovered);
  EXPECT_EQ(recovered->completed, 1);
  EXPECT_TRUE(backend->published.empty());
  EXPECT_EQ(backend->warmed.size(), 2);

  auto steady = folly::coro::blockingWait(restarted.runOnce());
  ASSERT_OK(steady);
  EXPECT_EQ(steady->scheduled, 0);
  EXPECT_EQ(backend->warmed.size(), 2);
}

TEST(TestWritePublishController, LostPublishResponseRecoversWithoutRepublishing) {
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(9, cache::UploadJobState::PUBLISHING)};
  backend->failPublishAfterCommit = backend->jobs.front().jobId;
  WritePublishController first(backend, config());
  auto ambiguous = folly::coro::blockingWait(first.runOnce());
  ASSERT_OK(ambiguous);
  EXPECT_EQ(ambiguous->failed, 1);
  ASSERT_EQ(backend->jobs.front().state, cache::UploadJobState::PUBLISHED);
  EXPECT_EQ(backend->published.size(), 1);
  EXPECT_TRUE(backend->warmed.empty());

  WritePublishController restarted(backend, config());
  auto recovered = folly::coro::blockingWait(restarted.recover());
  ASSERT_OK(recovered);
  EXPECT_EQ(recovered->completed, 1);
  EXPECT_EQ(backend->published.size(), 1);
  EXPECT_EQ(backend->warmed, std::vector<cache::UploadJobId>{backend->jobs.front().jobId});
}

TEST(TestWritePublishController, PermanentPublishFailureCheckpointsAndDeletesOrphan) {
  auto backend = std::make_shared<FakeBackend>();
  backend->jobs = {job(10, cache::UploadJobState::PUBLISHING)};
  backend->failPublishPermanent = backend->jobs.front().jobId;
  WritePublishController controller(backend, config());

  auto result = folly::coro::blockingWait(controller.runOnce());
  ASSERT_OK(result);
  ASSERT_EQ(backend->jobs.front().state, cache::UploadJobState::FAILED);
  ASSERT_EQ(backend->jobs.front().orphanCleanupState, cache::OrphanCleanupState::COMPLETE);
  ASSERT_EQ(backend->cleanedOrphans, std::vector<cache::UploadJobId>{backend->jobs.front().jobId});
}

TEST(TestWritePublishController, ValidatesLimitsAndRejectsBrokenPagination) {
  auto invalid = config();
  invalid.globalConcurrency = 0;
  ASSERT_ERROR(invalid.valid(), StatusCode::kInvalidConfig);

  class BrokenBackend final : public FakeBackend {
   public:
    CoTryTask<UploadJobPage> list(std::optional<cache::UploadJobId>, uint32_t, bool) final {
      co_return UploadJobPage{{}, true};
    }
  };
  auto backend = std::make_shared<BrokenBackend>();
  WritePublishController controller(backend, config());
  ASSERT_ERROR(folly::coro::blockingWait(controller.runOnce()), CacheCode::kInvalidResponse);
}

}  // namespace
}  // namespace hf3fs::cache_manager
