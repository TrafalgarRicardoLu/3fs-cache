#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/utils/Size.h"
#include "storage/aio/BatchReadJob.h"
#include "storage/service/TargetMap.h"
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

}  // namespace
}  // namespace hf3fs::storage::test
