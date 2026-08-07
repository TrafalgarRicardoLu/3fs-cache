#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/utils/Size.h"
#include "storage/aio/BatchReadJob.h"
#include "storage/service/TargetMap.h"
#include "storage/store/ChunkEngine.h"
#include "storage/store/StorageTarget.h"
#include "storage/store/StorageTargets.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

ReplaceCacheChunkItem replaceItem(ChunkId chunkId,
                                  uint64_t generation,
                                  Uuid operationId,
                                  std::string_view data,
                                  uint32_t chunkSize = 128_KB) {
  ReplaceCacheChunkItem item;
  item.key.chunkId = chunkId;
  item.key.vChainId = VersionedChainId{ChainId{1}, ChainVer{1}};
  item.cacheGeneration = cache::CacheGeneration{generation};
  item.operationId = operationId;
  item.data.assign(data.begin(), data.end());
  item.chunkSize = chunkSize;
  item.checksumType = ChecksumType::CRC32C;
  return item;
}

RetireCacheChunkItem retireItem(ChunkId chunkId, uint64_t generation, Uuid operationId) {
  RetireCacheChunkItem item;
  item.key.chunkId = chunkId;
  item.key.vChainId = VersionedChainId{ChainId{1}, ChainVer{1}};
  item.expectedGeneration = cache::CacheGeneration{generation};
  item.operationId = operationId;
  return item;
}

CacheChunkDescriptor descriptor(uint64_t generation, uint32_t block = 0) {
  auto placement =
      *PlacementIdentity::create({ChainId{1}, ChainVer{1}}, {TargetId{1}}, TargetId{1}, Uuid::from(9, generation));
  return {{7, cache::CacheBlockIndex{block}},
          cache::CacheGeneration{generation},
          std::move(placement),
          TargetId{1},
          1000,
          1000};
}

void addPhase2Identity(ReplaceCacheChunkItem &item, uint64_t generation, uint32_t block = 0) {
  item.logicalKey = cache::CacheBlockKey{7, cache::CacheBlockIndex{block}};
  item.permit = PermitIdentity{Uuid::from(8, generation),
                               descriptor(generation, block).placement,
                               1,
                               {{TargetId{1}, item.chunkSize}}};
  item.descriptor = descriptor(generation, block);
}

Result<cache::CacheGeneration> prepareCacheRead(StorageTarget &target, ChunkId chunkId, uint32_t length) {
  BatchReadReq request;
  request.payloads.emplace_back();
  request.payloads.front().key.chunkId = chunkId;
  request.payloads.front().length = length;
  BatchReadRsp response;
  response.results.resize(1);
  BatchReadJob job(request.payloads, response.results, ChecksumType::NONE);
  job.front().state().storageTarget = &target;
  RETURN_ON_ERROR(target.aioPrepareRead(job.front()));
  return job.front().result().cacheGeneration;
}

TEST(TestCacheGeneration, ReplaceRetireAndFence) {
  folly::test::TemporaryDirectory tmpPath;
  CPUExecutorGroup executor(4, "cache-generation");

  StorageTargets::Config config;
  config.set_target_num_per_path(1);
  config.set_target_paths({tmpPath.path()});
  config.storage_target().file_store().set_preopen_chunk_size_list({128_KB});
  config.set_allow_disk_without_uuid(true);

  StorageTargets::CreateConfig createConfig;
  createConfig.set_chunk_size_list({128_KB});
  createConfig.set_physical_file_count(2);
  createConfig.set_allow_disk_without_uuid(true);
  createConfig.set_target_ids({TargetId{1}});
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.create(createConfig));
  }

  AtomicallyTargetMap targetMap;
  StorageTargets targets(config, targetMap);
  ASSERT_OK(targets.load(executor));
  auto targetResult = targetMap.snapshot()->getTarget(TargetId{1});
  ASSERT_OK(targetResult);
  auto target = (*targetResult)->storageTarget;
  folly::CPUThreadPoolExecutor background(2);

  const ChunkId chunkId{0xCA, 1};
  auto firstOperation = Uuid::random();
  auto first = replaceItem(chunkId, 1, firstOperation, "first-generation-payload");
  auto result = target->replaceCacheChunk(first, background);
  ASSERT_OK(result);
  ASSERT_EQ(result->cacheGeneration, cache::CacheGeneration{1});
  ASSERT_FALSE(result->retired);
  ASSERT_EQ(*prepareCacheRead(*target, chunkId, result->length), cache::CacheGeneration{1});

  ASSERT_OK(target->replaceCacheChunk(first, background));
  auto conflict = first;
  conflict.operationId = Uuid::random();
  conflict.data[0] ^= 1;
  ASSERT_EQ(target->replaceCacheChunk(conflict, background).error().code(), CacheCode::kStateConflict);

  auto second = replaceItem(chunkId, 2, Uuid::random(), "short");
  result = target->replaceCacheChunk(second, background);
  ASSERT_OK(result);
  ASSERT_EQ(result->length, 5);
  ASSERT_EQ(*prepareCacheRead(*target, chunkId, result->length), cache::CacheGeneration{2});
  ASSERT_EQ(target->replaceCacheChunk(first, background).error().code(), CacheCode::kStaleGeneration);

  result = target->retireCacheChunk(retireItem(chunkId, 2, Uuid::random()));
  ASSERT_OK(result);
  ASSERT_TRUE(result->retired);
  ASSERT_EQ(result->length, 0);
  ASSERT_EQ(prepareCacheRead(*target, chunkId, 1).error().code(), CacheCode::kNotFound);
  ASSERT_EQ(target->replaceCacheChunk(second, background).error().code(), CacheCode::kStaleGeneration);
  ASSERT_EQ(target->retireCacheChunk(retireItem(chunkId, 1, Uuid::random())).error().code(),
            CacheCode::kGenerationAdvanced);

  auto third = replaceItem(chunkId, 3, Uuid::random(), "new-after-tombstone");
  result = target->replaceCacheChunk(third, background);
  ASSERT_OK(result);
  ASSERT_EQ(result->cacheGeneration, cache::CacheGeneration{3});
  ASSERT_FALSE(result->retired);
  ASSERT_EQ(*prepareCacheRead(*target, chunkId, result->length), cache::CacheGeneration{3});
}

TEST(TestCacheGeneration, TombstoneWithoutData) {
  folly::test::TemporaryDirectory tmpPath;
  CPUExecutorGroup executor(4, "cache-tombstone");

  StorageTargets::Config config;
  config.set_target_num_per_path(1);
  config.set_target_paths({tmpPath.path()});
  config.storage_target().file_store().set_preopen_chunk_size_list({128_KB});
  config.set_allow_disk_without_uuid(true);
  StorageTargets::CreateConfig createConfig;
  createConfig.set_chunk_size_list({128_KB});
  createConfig.set_physical_file_count(2);
  createConfig.set_allow_disk_without_uuid(true);
  createConfig.set_target_ids({TargetId{2}});
  createConfig.set_only_chunk_engine(true);
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.create(createConfig));
  }

  AtomicallyTargetMap targetMap;
  StorageTargets targets(config, targetMap);
  ASSERT_OK(targets.load(executor));
  auto targetResult = targetMap.snapshot()->getTarget(TargetId{2});
  ASSERT_OK(targetResult);
  auto target = (*targetResult)->storageTarget;
  folly::CPUThreadPoolExecutor background(2);

  const ChunkId chunkId{0xCA, 2};
  ASSERT_OK(target->retireCacheChunk(retireItem(chunkId, 5, Uuid::random())));
  auto query = target->queryCacheChunk(chunkId);
  ASSERT_OK(query);
  ASSERT_TRUE(query->retired);
  ASSERT_EQ(query->cacheGeneration, cache::CacheGeneration{5});
  ASSERT_EQ(prepareCacheRead(*target, chunkId, 1).error().code(), CacheCode::kNotFound);
  ASSERT_EQ(target->replaceCacheChunk(replaceItem(chunkId, 4, Uuid::random(), "old"), background).error().code(),
            CacheCode::kStaleGeneration);
  ASSERT_OK(target->replaceCacheChunk(replaceItem(chunkId, 6, Uuid::random(), "new"), background));
  ASSERT_EQ(*prepareCacheRead(*target, chunkId, 3), cache::CacheGeneration{6});
}

TEST(TestCacheGeneration, DescriptorPersistsAcrossTargetRestart) {
  folly::test::TemporaryDirectory tmpPath;
  CPUExecutorGroup executor(4, "cache-descriptor");

  StorageTargets::Config config;
  config.set_target_num_per_path(1);
  config.set_target_paths({tmpPath.path()});
  config.storage_target().file_store().set_preopen_chunk_size_list({128_KB});
  config.set_allow_disk_without_uuid(true);
  StorageTargets::CreateConfig createConfig;
  createConfig.set_chunk_size_list({128_KB});
  createConfig.set_physical_file_count(2);
  createConfig.set_allow_disk_without_uuid(true);
  createConfig.set_target_ids({TargetId{1}});
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.create(createConfig));
  }

  const ChunkId chunkId{0xCD, 1};
  auto item = replaceItem(chunkId, 1, Uuid::random(), "descriptor-payload");
  addPhase2Identity(item, 1);
  ASSERT_OK(item.valid());
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.load(executor));
    auto target = targetMap.snapshot()->getTarget(TargetId{1});
    ASSERT_OK(target);
    folly::CPUThreadPoolExecutor background(2);
    auto stored = (*target)->storageTarget->replaceCacheChunk(item, background);
    ASSERT_OK(stored);
    auto storedDescriptor = (*target)->storageTarget->queryCacheChunkDescriptor(chunkId);
    ASSERT_OK(storedDescriptor);
    ASSERT_EQ(*storedDescriptor, item.descriptor);
  }
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.load(executor));
    auto target = targetMap.snapshot()->getTarget(TargetId{1});
    ASSERT_OK(target);
    auto restored = (*target)->storageTarget->queryCacheChunkDescriptor(chunkId);
    ASSERT_OK(restored);
    ASSERT_EQ(*restored, item.descriptor);
  }
}

TEST(TestCacheGeneration, ChunkEngineTagRoundTripsDescriptorAndLegacyTag) {
  auto expected = descriptor(3, 2);
  auto encoded =
      ChunkEngine::encodeCacheTag({CacheChunkState::ACTIVE, cache::CacheGeneration{3}, Uuid::from(1, 2), expected});
  auto decoded = ChunkEngine::decodeCacheTag(
      rust::Slice<const uint8_t>{reinterpret_cast<const uint8_t *>(encoded.data()), encoded.size()});
  ASSERT_TRUE(decoded.has_value());
  ASSERT_EQ(decoded->descriptor, std::optional<CacheChunkDescriptor>{expected});

  auto legacy =
      ChunkEngine::encodeCacheTag({CacheChunkState::ACTIVE, cache::CacheGeneration{2}, Uuid::from(1, 1), std::nullopt});
  decoded = ChunkEngine::decodeCacheTag(
      rust::Slice<const uint8_t>{reinterpret_cast<const uint8_t *>(legacy.data()), legacy.size()});
  ASSERT_TRUE(decoded.has_value());
  ASSERT_FALSE(decoded->descriptor.has_value());
}

TEST(TestCacheGeneration, Phase2ReplaceRejectsPlacementMismatch) {
  auto item = replaceItem(ChunkId{0xCE, 1}, 1, Uuid::random(), "payload");
  addPhase2Identity(item, 1);
  item.key.vChainId = VersionedChainId{ChainId{2}, ChainVer{1}};
  ASSERT_ERROR(item.valid(), CacheCode::kPlacementMismatch);

  auto invalidDescriptor = descriptor(1);
  invalidDescriptor.targetId = TargetId{2};
  ASSERT_ERROR(invalidDescriptor.valid(), CacheCode::kPlacementMismatch);
}

TEST(TestCacheGeneration, InventoryUsesPersistentDescriptorAndFiltersRetired) {
  for (bool chunkEngine : {false, true}) {
    folly::test::TemporaryDirectory tmpPath;
    CPUExecutorGroup executor(4, chunkEngine ? "cache-inventory-engine" : "cache-inventory-store");
    StorageTargets::Config config;
    config.set_target_num_per_path(1);
    config.set_target_paths({tmpPath.path()});
    config.set_disk_roles({StorageRole::CACHE_ONLY});
    config.storage_target().file_store().set_preopen_chunk_size_list({128_KB});
    config.set_allow_disk_without_uuid(true);
    StorageTargets::CreateConfig createConfig;
    createConfig.set_chunk_size_list({128_KB});
    createConfig.set_physical_file_count(2);
    createConfig.set_allow_disk_without_uuid(true);
    createConfig.set_target_ids({TargetId{1}});
    createConfig.set_only_chunk_engine(chunkEngine);
    {
      AtomicallyTargetMap targetMap;
      StorageTargets targets(config, targetMap);
      ASSERT_OK(targets.create(createConfig));
    }

    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.load(executor));
    auto targetResult = targetMap.snapshot()->getTarget(TargetId{1});
    ASSERT_OK(targetResult);
    auto target = (*targetResult)->storageTarget;
    folly::CPUThreadPoolExecutor background(2);
    const ChunkId chunkId{0xCF, static_cast<uint64_t>(chunkEngine)};
    auto item = replaceItem(chunkId, 4, Uuid::random(), "inventory-payload");
    addPhase2Identity(item, 4);
    ASSERT_OK(target->replaceCacheChunk(item, background));

    auto active = target->listCacheInventory();
    ASSERT_OK(active);
    ASSERT_EQ(active->size(), 1);
    EXPECT_EQ(active->front().key.vChainId, item.key.vChainId);
    EXPECT_EQ(active->front().key.chunkId, item.key.chunkId);
    EXPECT_EQ(active->front().descriptor, *item.descriptor);
    EXPECT_EQ(active->front().generation.cacheGeneration, cache::CacheGeneration{4});
    EXPECT_FALSE(active->front().generation.retired);

    ASSERT_OK(target->retireCacheChunk(retireItem(chunkId, 4, Uuid::random())));
    auto retired = target->listCacheInventory();
    ASSERT_OK(retired);
    EXPECT_TRUE(retired->empty());
  }
}

}  // namespace
}  // namespace hf3fs::storage::test
