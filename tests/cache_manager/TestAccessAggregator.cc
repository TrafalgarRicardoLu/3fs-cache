#include <condition_variable>
#include <gtest/gtest.h>
#include <mutex>

#include "cache_manager/access/AccessFlushWorker.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager {
namespace {

CacheAccessReportItem access(uint32_t block, uint64_t generation = 1) {
  return {{7, cache::CacheBlockIndex{block}}, cache::CacheGeneration{generation}, 1};
}

class AccessBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final { co_return makeError(StatusCode::kNotImplemented); }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &,
                                                       cache::CacheBlockIndex,
                                                       const meta::CacheBlockLease &,
                                                       std::vector<uint8_t>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &,
                         const meta::CacheBlockLease &,
                         const storage::CacheChunkGenerationInfo &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::UpdateCacheBlockAccessRsp> updateAccess(std::vector<meta::UpdateCacheBlockAccessItem> items) final {
    {
      auto lock = std::unique_lock(mutex);
      batches.push_back(items);
      entered.notify_all();
    }
    if (error) co_return makeError(*error);
    meta::UpdateCacheBlockAccessRsp response;
    for (size_t i = 0; i < items.size(); ++i) {
      response.results.push_back(meta::UpdateCacheBlockAccessResult{items[i].key, i >= staleResults});
    }
    co_return response;
  }

  bool waitForBatches(size_t count) {
    auto lock = std::unique_lock(mutex);
    return entered.wait_for(lock, std::chrono::seconds(2), [&] { return batches.size() >= count; });
  }

  std::mutex mutex;
  std::condition_variable entered;
  std::vector<std::vector<meta::UpdateCacheBlockAccessItem>> batches;
  std::optional<Status> error;
  size_t staleResults{0};
};

AccessFlushWorker::Config workerConfig() {
  AccessFlushWorker::Config config;
  config.flushThreshold = 2;
  config.batchSize = 8;
  config.maxEntries = 8;
  config.flushInterval = 5_s;
  return config;
}

TEST(TestAccessAggregator, MergesByBlockGenerationAndKeepsMaximumReceiveTime) {
  AccessAggregator aggregator(8);
  ASSERT_TRUE(aggregator.record(access(0, 1), 10));
  ASSERT_TRUE(aggregator.record(access(0, 1), 30));
  ASSERT_TRUE(aggregator.record(access(0, 1), 20));
  ASSERT_TRUE(aggregator.record(access(0, 2), 25));
  auto batch = aggregator.take(8);
  ASSERT_EQ(batch.size(), size_t{2});
  EXPECT_EQ(batch[0].generation, cache::CacheGeneration{1});
  EXPECT_EQ(batch[0].managerReceiveTimeNs, uint64_t{30});
  EXPECT_EQ(batch[1].generation, cache::CacheGeneration{2});
  EXPECT_EQ(batch[1].managerReceiveTimeNs, uint64_t{25});
  EXPECT_EQ(aggregator.stats().merged, uint64_t{2});
}

TEST(TestAccessAggregator, EvictsOldestApproximationAtCapacity) {
  AccessAggregator aggregator(2);
  ASSERT_TRUE(aggregator.record(access(0), 10));
  ASSERT_TRUE(aggregator.record(access(1), 20));
  ASSERT_TRUE(aggregator.record(access(2), 30));
  auto batch = aggregator.take(8);
  ASSERT_EQ(batch.size(), size_t{2});
  EXPECT_EQ(batch[0].key.block, cache::CacheBlockIndex{1});
  EXPECT_EQ(batch[1].key.block, cache::CacheBlockIndex{2});
  EXPECT_EQ(aggregator.stats().capacityDropped, uint64_t{1});
}

TEST(TestAccessFlushWorker, FlushesAtThresholdAndCountsStaleResults) {
  auto backend = std::make_shared<AccessBackend>();
  backend->staleResults = 1;
  AccessFlushWorker worker(backend, workerConfig(), [] { return uint64_t{1000}; });
  ASSERT_OK(worker.start());
  auto statuses = worker.submit(std::array{access(0), access(1)});
  EXPECT_EQ(statuses, (std::vector{AccessReportStatus::ACCEPTED, AccessReportStatus::ACCEPTED}));
  ASSERT_TRUE(backend->waitForBatches(1));
  worker.stop();
  ASSERT_EQ(backend->batches[0].size(), size_t{2});
  EXPECT_EQ(backend->batches[0][0].managerReceiveTimeNs, uint64_t{1000});
  EXPECT_EQ(worker.stats().stale, uint64_t{1});
  EXPECT_EQ(worker.stats().flushed, uint64_t{1});
}

TEST(TestAccessFlushWorker, DropsTimedOutMetadataBatchWithoutRetry) {
  auto backend = std::make_shared<AccessBackend>();
  backend->error = Status(RPCCode::kTimeout);
  auto config = workerConfig();
  config.flushThreshold = 1;
  AccessFlushWorker worker(backend, config, [] { return uint64_t{1000}; });
  ASSERT_OK(worker.start());
  worker.submit(std::array{access(0)});
  ASSERT_TRUE(backend->waitForBatches(1));
  worker.stop();
  EXPECT_EQ(backend->batches.size(), size_t{1});
  EXPECT_EQ(worker.stats().failedBatches, uint64_t{1});
  EXPECT_EQ(worker.stats().dropped, uint64_t{1});
}

TEST(TestAccessFlushWorker, StopDrainsPendingAggregation) {
  auto backend = std::make_shared<AccessBackend>();
  AccessFlushWorker worker(backend, workerConfig(), [] { return uint64_t{1000}; });
  ASSERT_OK(worker.start());
  worker.submit(std::array{access(0)});
  worker.stop();
  ASSERT_EQ(backend->batches.size(), size_t{1});
  EXPECT_EQ(worker.pending(), size_t{0});
  EXPECT_EQ(worker.stats().flushed, uint64_t{1});
}

}  // namespace
}  // namespace hf3fs::cache_manager
