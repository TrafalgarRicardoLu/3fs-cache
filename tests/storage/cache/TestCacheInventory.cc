#include <gtest/gtest.h>

#include "storage/cache/reconcile/CacheInventory.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

CacheInventoryEntry entry(uint64_t chunk, uint64_t generation, uint32_t block = 0, TargetId target = TargetId{1}) {
  auto placement = *PlacementIdentity::create({ChainId{1}, ChainVer{1}}, {target}, target, Uuid::from(70, generation));
  CacheChunkDescriptor descriptor{{7, cache::CacheBlockIndex{block}},
                                  cache::CacheGeneration{generation},
                                  placement,
                                  target,
                                  1000,
                                  1000};
  return {target,
          PhysicalDiskId{Uuid::from(71, 1)},
          {placement.versionedChain, ChunkId{0, chunk}},
          descriptor,
          {descriptor.generation, false, 4096, ChecksumInfo{ChecksumType::CRC32C, static_cast<uint32_t>(chunk)}}};
}

ListCacheInventoryReq request(std::string cursor = {}, uint32_t limit = 2) {
  return {TargetId{1}, std::move(cursor), limit, cache::kCachePhase4ProtocolVersion};
}

TEST(TestCacheInventory, PagesStablePhysicalEntriesAndKeepsGenerations) {
  std::vector entries{entry(3, 2, 2), entry(1, 1, 0), entry(2, 3, 1)};
  auto first = pageCacheInventory(entries, request(), Uuid::from(72, 1));
  ASSERT_OK(first);
  ASSERT_EQ(first->entries.size(), 2);
  EXPECT_FALSE(first->done);
  EXPECT_FALSE(first->nextCursor.empty());
  EXPECT_EQ(first->entries[0].generation.cacheGeneration, cache::CacheGeneration{1});
  EXPECT_EQ(first->entries[1].generation.cacheGeneration, cache::CacheGeneration{3});

  auto second = pageCacheInventory(entries, request(first->nextCursor), Uuid::from(72, 1));
  ASSERT_OK(second);
  ASSERT_EQ(second->entries.size(), 1);
  EXPECT_TRUE(second->done);
  EXPECT_TRUE(second->nextCursor.empty());
  EXPECT_EQ(second->entries[0].generation.cacheGeneration, cache::CacheGeneration{2});
  EXPECT_EQ(second->inventoryEpoch, first->inventoryEpoch);
}

TEST(TestCacheInventory, EmptyTargetCompletesWithEpoch) {
  auto page = pageCacheInventory({}, request(), Uuid::from(72, 2));
  ASSERT_OK(page);
  EXPECT_TRUE(page->entries.empty());
  EXPECT_TRUE(page->done);
  EXPECT_NE(page->inventoryEpoch, Uuid::zero());
}

TEST(TestCacheInventory, RejectsCursorAfterMutationOrRestart) {
  std::vector entries{entry(1, 1), entry(2, 1, 1)};
  auto first = pageCacheInventory(entries, request({}, 1), Uuid::from(72, 3));
  ASSERT_OK(first);

  entries.push_back(entry(3, 2, 2));
  ASSERT_ERROR(pageCacheInventory(entries, request(first->nextCursor, 1), Uuid::from(72, 3)),
               CacheCode::kStateConflict);
  entries.pop_back();
  ASSERT_ERROR(pageCacheInventory(entries, request(first->nextCursor, 1), Uuid::from(72, 4)),
               CacheCode::kStateConflict);
}

TEST(TestCacheInventory, RejectsEntriesFromAnotherTarget) {
  auto foreign = entry(1, 1, 0, TargetId{2});
  ASSERT_ERROR(pageCacheInventory({foreign}, request(), Uuid::from(72, 5)), CacheCode::kRoleMismatch);
}

}  // namespace
}  // namespace hf3fs::storage::test
