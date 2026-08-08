#include <gtest/gtest.h>
#include <set>

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
  tags.diskId = "disk-identity";
  tags.generation = "generation-identity";
  tags.epoch = "epoch-identity";
  tags.operationId = "operation-identity";
  tags.sourceId = "source-identity";
  tags.sequence = 9;
  tags.reason = "capacity";

  recordCount(Event::MANAGER_ADMISSION_RESULT, 5, tags);
  recordCount(Event::STORAGE_PERMIT_RESULT, 2, tags);

  ASSERT_EQ(countForTest(Event::MANAGER_ADMISSION_RESULT), uint64_t{5});
  ASSERT_EQ(countForTest(Event::STORAGE_PERMIT_RESULT), uint64_t{2});
  auto recorded = lastTagsForTest(Event::MANAGER_ADMISSION_RESULT);
  ASSERT_EQ(recorded.inode, tags.inode);
  ASSERT_EQ(recorded.block, tags.block);
  ASSERT_EQ(recorded.originId, tags.originId);
  ASSERT_EQ(recorded.diskId, tags.diskId);
  ASSERT_EQ(recorded.generation, tags.generation);
  ASSERT_EQ(recorded.epoch, tags.epoch);
  ASSERT_EQ(recorded.operationId, tags.operationId);
  ASSERT_EQ(recorded.sourceId, tags.sourceId);
  ASSERT_EQ(recorded.sequence, tags.sequence);
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

TEST_F(TestCacheMetrics, BoundsOrchestrationLabels) {
  recordCount(Event::MANAGER_ORCHESTRATION_TICK,
              1,
              {.sourceId = std::string(100, 'j'), .reason = std::string(100, 'r')});
  auto tags = lastTagsForTest(Event::MANAGER_ORCHESTRATION_TICK);
  EXPECT_EQ(tags.sourceId.size(), 64u);
  EXPECT_EQ(tags.reason.size(), 64u);
}

TEST_F(TestCacheMetrics, RegistersEveryMetricNameExactlyOnce) {
  std::set<std::string> names;
  for (uint8_t value = 0; value < static_cast<uint8_t>(Event::COUNT); ++value) {
    auto name = metricNameForTest(static_cast<Event>(value));
    ASSERT_FALSE(name.empty()) << static_cast<unsigned>(value);
    EXPECT_TRUE(names.emplace(name).second) << name;
  }
}

}  // namespace
}  // namespace hf3fs::cache::metrics
