#include <gtest/gtest.h>

#include "storage/cache/eviction/LocalSafetyEvictor.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

PhysicalDiskId disk(uint64_t id) { return PhysicalDiskId{Uuid::from(0, id)}; }

CacheChunkDescriptor descriptor(uint64_t inode, uint64_t access) {
  auto placement =
      *PlacementIdentity::create({ChainId{1}, ChainVer{1}}, {TargetId{1}}, TargetId{1}, Uuid::from(1, inode));
  return {{inode, cache::CacheBlockIndex{0}}, cache::CacheGeneration{1}, placement, TargetId{1}, 1, access};
}

class Backend : public LocalSafetyBackend {
 public:
  Result<std::vector<LocalSafetyDisk>> diskSpace() final { return spaces; }
  Result<std::vector<LocalEvictionCandidate>> candidates(const PhysicalDiskId &) final { return chunks; }
  Result<bool> retire(const PhysicalDiskId &, const LocalEvictionCandidate &candidate, Uuid) final {
    retired.push_back(candidate.chunkId);
    return true;
  }

  std::vector<LocalSafetyDisk> spaces;
  std::vector<LocalEvictionCandidate> chunks;
  std::vector<ChunkId> retired;
};

TEST(TestLocalSafetyEvictor, HysteresisAndLruToLowWatermark) {
  auto backend = std::make_shared<Backend>();
  backend->spaces = {{disk(1), 1000, 960, 0}};
  backend->chunks = {{ChunkId{1, 1}, descriptor(1, 100), 80},
                     {ChunkId{1, 2}, descriptor(2, 200), 80},
                     {ChunkId{1, 3}, descriptor(3, 300), 80}};
  LocalSafetyEvictor evictor(backend, *createLocalEvictionPolicy("lru"), 0.95, 0.80, 0);
  auto first = evictor.runOnce(1000);
  ASSERT_OK(first);
  EXPECT_EQ(first->retired, 3);
  EXPECT_EQ(backend->retired, (std::vector<ChunkId>{ChunkId{1, 1}, ChunkId{1, 2}, ChunkId{1, 3}}));

  backend->retired.clear();
  backend->spaces.front().physicalUsedBytes = 900;
  ASSERT_OK(evictor.runOnce(1000));
  EXPECT_TRUE(backend->retired.empty());
}

TEST(TestLocalSafetyEvictor, RejectsMaliciousPolicyOutput) {
  class BadPolicy final : public LocalEvictionPolicy {
   public:
    Result<std::vector<size_t>> select(std::span<const LocalEvictionCandidate>, const LocalEvictionContext &) final {
      return std::vector<size_t>{1};
    }
  };
  auto backend = std::make_shared<Backend>();
  backend->spaces = {{disk(1), 1000, 960, 0}};
  backend->chunks = {{ChunkId{1, 1}, descriptor(1, 100), 200}};
  LocalSafetyEvictor evictor(backend, std::make_unique<BadPolicy>(), 0.95, 0.80, 0);
  ASSERT_ERROR(evictor.runOnce(1000), StatusCode::kDataCorruption);
}

}  // namespace
}  // namespace hf3fs::storage::test
