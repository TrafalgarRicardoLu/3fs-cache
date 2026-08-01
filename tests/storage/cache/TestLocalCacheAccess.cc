#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/experimental/coro/BlockingWait.h>

#include "common/utils/Size.h"
#include "storage/service/TargetMap.h"
#include "storage/store/StorageTarget.h"
#include "storage/store/StorageTargets.h"
#include "storage/store/cache/LocalCacheAccess.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

CacheChunkDescriptor descriptor(uint64_t generation, uint64_t accessAtNs) {
  auto placement =
      *PlacementIdentity::create({ChainId{1}, ChainVer{1}}, {TargetId{1}}, TargetId{1}, Uuid::from(9, generation));
  return {{7, cache::CacheBlockIndex{0}},
          cache::CacheGeneration{generation},
          std::move(placement),
          TargetId{1},
          1000,
          accessAtNs};
}

ReplaceCacheChunkItem replaceItem(ChunkId chunkId, ChainId chainId, uint64_t generation) {
  ReplaceCacheChunkItem item;
  item.key.chunkId = chunkId;
  item.key.vChainId = VersionedChainId{chainId, ChainVer{1}};
  item.cacheGeneration = cache::CacheGeneration{generation};
  item.operationId = Uuid::from(8, generation);
  item.data = {'c', 'a', 'c', 'h', 'e'};
  item.chunkSize = 128_KB;
  item.checksumType = ChecksumType::CRC32C;
  item.logicalKey = cache::CacheBlockKey{7, cache::CacheBlockIndex{0}};
  item.permit = PermitIdentity{Uuid::from(7, generation),
                               descriptor(generation, 1000).placement,
                               1,
                               {{TargetId{1}, item.chunkSize}}};
  item.descriptor = descriptor(generation, 1000);
  return item;
}

StorageTargets::Config targetConfig(const Path &path) {
  StorageTargets::Config config;
  config.set_target_num_per_path(1);
  config.set_target_paths({path});
  config.storage_target().file_store().set_preopen_chunk_size_list({128_KB});
  config.set_allow_disk_without_uuid(true);
  return config;
}

StorageTargets::CreateConfig createConfig(bool chunkEngine) {
  StorageTargets::CreateConfig config;
  config.set_chunk_size_list({128_KB});
  config.set_physical_file_count(2);
  config.set_allow_disk_without_uuid(true);
  config.set_target_ids({TargetId{1}});
  config.set_only_chunk_engine(chunkEngine);
  return config;
}

TEST(TestLocalCacheAccess, CoalescesAndKeepsMonotonicTime) {
  LocalCacheAccess access;
  const ChunkId chunkId{0xAC, 1};
  std::vector<uint64_t> persisted;
  auto persist = [&](cache::CacheGeneration, uint64_t observedAtNs) -> Result<bool> {
    persisted.push_back(observedAtNs);
    return true;
  };

  ASSERT_EQ(*access.record(chunkId, cache::CacheGeneration{1}, 1000, 500, persist), true);
  ASSERT_EQ(*access.record(chunkId, cache::CacheGeneration{1}, 1100, 500, persist), false);
  ASSERT_EQ(*access.record(chunkId, cache::CacheGeneration{1}, 900, 500, persist), false);
  ASSERT_EQ(*access.record(chunkId, cache::CacheGeneration{1}, 1499, 500, persist), false);
  ASSERT_EQ(*access.record(chunkId, cache::CacheGeneration{1}, 1500, 500, persist), true);
  ASSERT_EQ(persisted, (std::vector<uint64_t>{1000, 1500}));
}

TEST(TestLocalCacheAccess, DropsStaleGenerationState) {
  LocalCacheAccess access;
  const ChunkId chunkId{0xAC, 2};
  size_t calls = 0;
  auto stale = [&](cache::CacheGeneration, uint64_t) -> Result<bool> {
    ++calls;
    return false;
  };
  ASSERT_FALSE(*access.record(chunkId, cache::CacheGeneration{1}, 1000, 500, stale));
  ASSERT_FALSE(*access.record(chunkId, cache::CacheGeneration{1}, 1100, 500, stale));
  ASSERT_EQ(calls, 2);
}

TEST(TestLocalCacheAccess, RejectsOrdinaryChunkGeneration) {
  LocalCacheAccess access;
  bool persisted = false;
  auto result = access.record(ChunkId{0xAC, 3},
                              cache::CacheGeneration{},
                              1000,
                              500,
                              [&](cache::CacheGeneration, uint64_t) -> Result<bool> {
                                persisted = true;
                                return true;
                              });
  ASSERT_ERROR(result, StatusCode::kInvalidArg);
  ASSERT_FALSE(persisted);
}

TEST(TestLocalCacheAccess, PersistsAcrossRestartAndFencesOldGeneration) {
  for (bool chunkEngine : {false, true}) {
    folly::test::TemporaryDirectory tmpPath;
    CPUExecutorGroup executor(4, chunkEngine ? "local-access-engine" : "local-access-store");
    auto config = targetConfig(tmpPath.path());
    {
      AtomicallyTargetMap targetMap;
      StorageTargets targets(config, targetMap);
      ASSERT_OK(targets.create(createConfig(chunkEngine)));
    }

    const ChunkId chunkId{0xAC, static_cast<uint8_t>(chunkEngine)};
    {
      AtomicallyTargetMap targetMap;
      StorageTargets targets(config, targetMap);
      ASSERT_OK(targets.load(executor));
      auto targetResult = targetMap.snapshot()->getTarget(TargetId{1});
      ASSERT_OK(targetResult);
      auto target = (*targetResult)->storageTarget;
      auto first = replaceItem(chunkId, target->chainId(), 1);
      folly::CPUThreadPoolExecutor background(2);
      ASSERT_OK(target->replaceCacheChunk(first, background));
      auto persisted =
          folly::coro::blockingWait(target->recordCacheAccess(chunkId, cache::CacheGeneration{1}, 2000, 1_ns));
      ASSERT_OK(persisted);
      ASSERT_TRUE(*persisted);
      auto stored = target->queryCacheChunkDescriptor(chunkId);
      ASSERT_OK(stored);
      ASSERT_TRUE(stored->has_value());
      ASSERT_EQ((*stored)->lastAccessAtNs, 2000);
    }
    {
      AtomicallyTargetMap targetMap;
      StorageTargets targets(config, targetMap);
      ASSERT_OK(targets.load(executor));
      auto targetResult = targetMap.snapshot()->getTarget(TargetId{1});
      ASSERT_OK(targetResult);
      auto target = (*targetResult)->storageTarget;
      auto restored = target->queryCacheChunkDescriptor(chunkId);
      ASSERT_OK(restored);
      ASSERT_TRUE(restored->has_value());
      ASSERT_EQ((*restored)->lastAccessAtNs, 2000);

      folly::CPUThreadPoolExecutor background(2);
      auto second = replaceItem(chunkId, target->chainId(), 2);
      ASSERT_OK(target->replaceCacheChunk(second, background));
      auto stale = folly::coro::blockingWait(target->recordCacheAccess(chunkId, cache::CacheGeneration{1}, 3000, 1_ns));
      ASSERT_OK(stale);
      ASSERT_FALSE(*stale);
      auto current = target->queryCacheChunkDescriptor(chunkId);
      ASSERT_OK(current);
      ASSERT_TRUE(current->has_value());
      ASSERT_EQ((*current)->generation, cache::CacheGeneration{2});
      ASSERT_EQ((*current)->lastAccessAtNs, 1000);
    }
  }
}

}  // namespace
}  // namespace hf3fs::storage::test
