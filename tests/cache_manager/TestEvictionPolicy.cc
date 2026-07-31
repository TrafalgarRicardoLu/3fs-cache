#include "cache_manager/config/Config.h"
#include "cache_manager/eviction/EvictionPolicy.h"
#include "cache_manager/eviction/LRUEvictionPolicy.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PhysicalDiskId disk(uint64_t id) { return storage::PhysicalDiskId{Uuid::from(0, id)}; }

EvictionCandidate candidate(uint64_t inode, int64_t accessUs, std::map<storage::PhysicalDiskId, uint64_t> footprint) {
  EvictionCandidate value;
  value.key = cache::CacheBlockKey{inode, cache::CacheBlockIndex{0}};
  value.readyAt = UtcTime::fromMicroseconds(accessUs);
  value.lastAccessAt = value.readyAt;
  value.physicalFootprintByDisk = std::move(footprint);
  return value;
}

EvictionContext context(std::map<storage::PhysicalDiskId, uint64_t> deficits,
                        int64_t nowUs = 1000,
                        Duration protection = 0_ns) {
  return {std::move(deficits), UtcTime::fromMicroseconds(nowUs), protection};
}

class FakeEvictionPolicy final : public EvictionPolicy {
 public:
  explicit FakeEvictionPolicy(std::vector<size_t> selected)
      : selected_(std::move(selected)) {}

  Result<std::vector<size_t>> select(std::span<const EvictionCandidate>, const EvictionContext &) override {
    return selected_;
  }

 private:
  std::vector<size_t> selected_;
};

TEST(TestEvictionPolicy, LruSatisfiesMultipleDiskDeficits) {
  auto first = disk(1);
  auto second = disk(2);
  std::vector<EvictionCandidate> candidates{
      candidate(1, 100, {{first, 60}}),
      candidate(2, 200, {{second, 70}}),
      candidate(3, 300, {{first, 50}, {second, 40}}),
  };
  LRUEvictionPolicy policy;
  auto selected = policy.select(candidates, context({{first, 100}, {second, 100}}));
  ASSERT_OK(selected);
  ASSERT_EQ(*selected, (std::vector<size_t>{0, 1, 2}));
  auto released = validateEvictionSelection(candidates, context({{first, 100}, {second, 100}}), *selected, 3);
  ASSERT_OK(released);
  ASSERT_EQ(released->at(first), 110);
  ASSERT_EQ(released->at(second), 110);
}

TEST(TestEvictionPolicy, LruIsStableAndHonorsProtectionPeriod) {
  auto pressured = disk(1);
  std::vector<EvictionCandidate> candidates{
      candidate(1, 500, {{pressured, 10}}),
      candidate(2, 500, {{pressured, 10}}),
      candidate(3, 600, {{pressured, 10}}),
  };
  LRUEvictionPolicy policy;
  auto selected = policy.select(candidates, context({{pressured, 30}}, 1000, 500_us));
  ASSERT_OK(selected);
  ASSERT_EQ(*selected, (std::vector<size_t>{0, 1}));
}

TEST(TestEvictionPolicy, ValidatorRejectsMaliciousIndexes) {
  auto pressured = disk(1);
  auto other = disk(2);
  std::vector<EvictionCandidate> candidates{
      candidate(1, 100, {{pressured, 10}}),
      candidate(2, 900, {{pressured, 10}}),
      candidate(3, 100, {{other, 10}}),
  };
  auto evictionContext = context({{pressured, 100}}, 1000, 200_us);

  for (auto output :
       {std::vector<size_t>{0, 0}, std::vector<size_t>{3}, std::vector<size_t>{1}, std::vector<size_t>{2}}) {
    FakeEvictionPolicy policy(std::move(output));
    auto selected = policy.select(candidates, evictionContext);
    ASSERT_OK(selected);
    ASSERT_ERROR(validateEvictionSelection(candidates, evictionContext, *selected, 2), StatusCode::kInvalidArg);
  }
  ASSERT_ERROR(validateEvictionSelection(candidates, evictionContext, std::vector<size_t>{0, 1}, 1),
               StatusCode::kInvalidArg);
}

TEST(TestEvictionPolicy, ValidatorReportsPartialProgress) {
  auto pressured = disk(1);
  std::vector<EvictionCandidate> candidates{candidate(1, 100, {{pressured, 10}})};
  auto released = validateEvictionSelection(candidates, context({{pressured, 100}}), std::vector<size_t>{0}, 1);
  ASSERT_OK(released);
  ASSERT_EQ(released->at(pressured), 10);
}

TEST(TestEvictionPolicy, FactoryRejectsUnknownPolicy) {
  ASSERT_OK(createEvictionPolicy("lru"));
  ASSERT_ERROR(createEvictionPolicy("clock"), StatusCode::kInvalidConfig);

  Config config;
  config.set_service_token("test");
  config.set_eviction_policy("clock");
  ASSERT_ERROR(config.validateRuntime(), StatusCode::kInvalidConfig);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
