#include <condition_variable>
#include <gtest/gtest.h>
#include <mutex>

#include "client/cache/CacheAccessReporter.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::client::cache {
namespace {

class ReportSink {
 public:
  CacheAccessReporter::SendFn sender() {
    return [this](const cache_manager::ReportCacheAccessReq &request,
                  const net::UserRequestOptions &options) -> CoTryTask<cache_manager::ReportCacheAccessRsp> {
      {
        auto lock = std::unique_lock(mutex);
        requests.push_back(request);
        timeouts.push_back(options.timeout);
        entered.notify_all();
        blocked.wait(lock, [&] { return !block; });
      }
      if (error) co_return makeError(*error);
      cache_manager::ReportCacheAccessRsp response;
      for (const auto &item : request.items) {
        response.results.push_back(
            cache_manager::CacheAccessReportResult{item.key, cache_manager::AccessReportStatus::ACCEPTED});
      }
      co_return response;
    };
  }

  bool waitForRequests(size_t count, std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    auto lock = std::unique_lock(mutex);
    return entered.wait_for(lock, timeout, [&] { return requests.size() >= count; });
  }

  void unblock() {
    {
      auto lock = std::unique_lock(mutex);
      block = false;
    }
    blocked.notify_all();
  }

  std::mutex mutex;
  std::condition_variable entered;
  std::condition_variable blocked;
  std::vector<cache_manager::ReportCacheAccessReq> requests;
  std::vector<std::optional<Duration>> timeouts;
  std::optional<Status> error;
  bool block{false};
};

flat::UserInfo user(uint32_t id = 1) { return {flat::Uid{id}, flat::Gid{id}, "token"}; }

hf3fs::cache::CacheBlockKey key(uint32_t block) { return {7, hf3fs::cache::CacheBlockIndex{block}}; }

CacheAccessReporter::Config config() {
  CacheAccessReporter::Config value;
  value.flushThreshold = 2;
  value.maxBatchSize = 8;
  value.maxBufferedItems = 8;
  value.flushInterval = 5_s;
  value.requestTimeout = 50_ms;
  return value;
}

TEST(TestCacheAccessReporter, FlushesAtThresholdWithBoundedRpcOptions) {
  ReportSink sink;
  CacheAccessReporter reporter(sink.sender(), config());
  ASSERT_OK(reporter.start());
  ASSERT_TRUE(reporter.record(user(), key(0), hf3fs::cache::CacheGeneration{1}, 10));
  ASSERT_TRUE(reporter.record(user(), key(1), hf3fs::cache::CacheGeneration{1}, 20));
  ASSERT_TRUE(sink.waitForRequests(1));
  reporter.stop();
  ASSERT_EQ(sink.requests[0].items.size(), size_t{2});
  EXPECT_EQ(sink.timeouts[0], std::optional<Duration>{50_ms});
  EXPECT_EQ(reporter.stats().sent, uint64_t{2});
}

TEST(TestCacheAccessReporter, PeriodicallyFlushesBelowThreshold) {
  ReportSink sink;
  auto cfg = config();
  cfg.flushThreshold = 8;
  cfg.flushInterval = 10_ms;
  CacheAccessReporter reporter(sink.sender(), cfg);
  ASSERT_OK(reporter.start());
  ASSERT_TRUE(reporter.record(user(), key(0), hf3fs::cache::CacheGeneration{1}, 10));
  ASSERT_TRUE(sink.waitForRequests(1));
  reporter.stop();
  ASSERT_EQ(sink.requests[0].items.size(), size_t{1});
}

TEST(TestCacheAccessReporter, PreservesOutOfOrderDiagnosticTimes) {
  ReportSink sink;
  auto cfg = config();
  cfg.flushThreshold = 3;
  CacheAccessReporter reporter(sink.sender(), cfg);
  ASSERT_OK(reporter.start());
  ASSERT_TRUE(reporter.record(user(), key(0), hf3fs::cache::CacheGeneration{2}, 30));
  ASSERT_TRUE(reporter.record(user(), key(1), hf3fs::cache::CacheGeneration{2}, 10));
  ASSERT_TRUE(reporter.record(user(), key(2), hf3fs::cache::CacheGeneration{2}, 20));
  ASSERT_TRUE(sink.waitForRequests(1));
  reporter.stop();
  ASSERT_EQ(sink.requests[0].items.size(), size_t{3});
  EXPECT_EQ(sink.requests[0].items[0].clientObservedTimeNs, uint64_t{30});
  EXPECT_EQ(sink.requests[0].items[1].clientObservedTimeNs, uint64_t{10});
  EXPECT_EQ(sink.requests[0].items[2].clientObservedTimeNs, uint64_t{20});
}

TEST(TestCacheAccessReporter, DropsOverflowWithoutBlockingProducer) {
  ReportSink sink;
  sink.block = true;
  auto cfg = config();
  cfg.maxBufferedItems = 2;
  CacheAccessReporter reporter(sink.sender(), cfg);
  ASSERT_OK(reporter.start());
  ASSERT_TRUE(reporter.record(user(), key(0), hf3fs::cache::CacheGeneration{1}));
  ASSERT_TRUE(reporter.record(user(), key(1), hf3fs::cache::CacheGeneration{1}));
  ASSERT_TRUE(sink.waitForRequests(1));
  ASSERT_TRUE(reporter.record(user(), key(2), hf3fs::cache::CacheGeneration{1}));
  ASSERT_TRUE(reporter.record(user(), key(3), hf3fs::cache::CacheGeneration{1}));
  auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(reporter.record(user(), key(4), hf3fs::cache::CacheGeneration{1}));
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(50));
  sink.unblock();
  reporter.stop();
  EXPECT_EQ(reporter.stats().dropped, uint64_t{1});
}

TEST(TestCacheAccessReporter, DropsBatchWhenManagerIsDown) {
  ReportSink sink;
  sink.error = Status(CacheCode::kUnavailable);
  CacheAccessReporter reporter(sink.sender(), config());
  ASSERT_OK(reporter.start());
  ASSERT_TRUE(reporter.record(user(), key(0), hf3fs::cache::CacheGeneration{1}));
  ASSERT_TRUE(reporter.record(user(), key(1), hf3fs::cache::CacheGeneration{1}));
  ASSERT_TRUE(sink.waitForRequests(1));
  reporter.stop();
  EXPECT_EQ(reporter.stats().failedBatches, uint64_t{1});
  EXPECT_EQ(reporter.stats().dropped, uint64_t{2});
}

TEST(TestCacheAccessReporter, StopDrainsPendingBatch) {
  ReportSink sink;
  CacheAccessReporter reporter(sink.sender(), config());
  ASSERT_OK(reporter.start());
  ASSERT_TRUE(reporter.record(user(), key(0), hf3fs::cache::CacheGeneration{1}));
  auto start = std::chrono::steady_clock::now();
  reporter.stop();
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
  ASSERT_EQ(sink.requests.size(), size_t{1});
  EXPECT_EQ(reporter.stats().sent, uint64_t{1});
}

}  // namespace
}  // namespace hf3fs::client::cache
