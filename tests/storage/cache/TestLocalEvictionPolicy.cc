#include "storage/store/cache/LocalEvictionPolicy.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

LocalEvictionCandidate candidate(uint8_t id, uint64_t accessAtNs, uint64_t footprint) {
  LocalEvictionCandidate value;
  value.chunkId = ChunkId{0xED, id};
  value.descriptor.createdAtNs = accessAtNs;
  value.descriptor.lastAccessAtNs = accessAtNs;
  value.footprintBytes = footprint;
  return value;
}

TEST(TestLocalEvictionPolicy, LruIsStableAndHonorsProtection) {
  auto policy = createLocalEvictionPolicy("lru");
  ASSERT_OK(policy);
  std::vector<LocalEvictionCandidate> candidates{
      candidate(1, 100, 10),
      candidate(2, 100, 20),
      candidate(3, 900, 30),
  };
  auto selected = (*policy)->select(candidates, LocalEvictionContext{25, 1000, 200});
  ASSERT_OK(selected);
  ASSERT_EQ(*selected, (std::vector<size_t>{0, 1}));
}

TEST(TestLocalEvictionPolicy, SelectsOnlyEnoughPhysicalBytes) {
  auto policy = createLocalEvictionPolicy("lru");
  ASSERT_OK(policy);
  std::vector<LocalEvictionCandidate> candidates{
      candidate(1, 100, 0),
      candidate(2, 200, 40),
      candidate(3, 300, 40),
  };
  auto selected = (*policy)->select(candidates, LocalEvictionContext{30, 1000, 0});
  ASSERT_OK(selected);
  ASSERT_EQ(*selected, (std::vector<size_t>{1}));
}

TEST(TestLocalEvictionPolicy, FactoryRejectsUnknownPolicy) {
  ASSERT_ERROR(createLocalEvictionPolicy("clock"), StatusCode::kInvalidConfig);
}

}  // namespace
}  // namespace hf3fs::storage::test
