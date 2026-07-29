#include <gtest/gtest.h>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache::metrics {
namespace {

class TestCacheMetrics : public ::testing::Test {
 protected:
  void SetUp() override { resetForTest(); }
};

TEST_F(TestCacheMetrics, RecordsCountsAndIdentitySafeTags) {
  Tags tags;
  tags.inode = 101;
  tags.block = 7;
  tags.originId = 3;
  tags.reason = "capacity";

  recordCount(Event::MANAGER_ADMISSION_RESULT, 5, tags);

  ASSERT_EQ(countForTest(Event::MANAGER_ADMISSION_RESULT), uint64_t{5});
  auto recorded = lastTagsForTest(Event::MANAGER_ADMISSION_RESULT);
  ASSERT_EQ(recorded.inode, tags.inode);
  ASSERT_EQ(recorded.block, tags.block);
  ASSERT_EQ(recorded.originId, tags.originId);
  ASSERT_EQ(recorded.reason, tags.reason);
}

TEST_F(TestCacheMetrics, RecordsLatencyAndGauge) {
  recordLatency(Event::CLIENT_READ_PLAN, std::chrono::microseconds{25}, {.reason = "success"});
  setGauge(Event::MANAGER_QUEUE, 17, {.reason = "pending"});

  ASSERT_EQ(countForTest(Event::CLIENT_READ_PLAN), uint64_t{1});
  ASSERT_EQ(lastTagsForTest(Event::CLIENT_READ_PLAN).reason, "success");
  ASSERT_EQ(countForTest(Event::MANAGER_QUEUE), uint64_t{17});
  ASSERT_EQ(lastTagsForTest(Event::MANAGER_QUEUE).reason, "pending");
}

TEST_F(TestCacheMetrics, IgnoresSentinelEvent) {
  recordCount(Event::COUNT, 1, {.reason = "invalid"});
  recordLatency(Event::COUNT, std::chrono::nanoseconds{1});
  setGauge(Event::COUNT, 1);

  ASSERT_EQ(countForTest(Event::COUNT), uint64_t{0});
  ASSERT_TRUE(lastTagsForTest(Event::COUNT).reason.empty());
}

}  // namespace
}  // namespace hf3fs::cache::metrics
