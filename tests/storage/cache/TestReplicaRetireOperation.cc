#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include "common/utils/Size.h"
#include "storage/aio/BatchReadJob.h"
#include "storage/service/TargetMap.h"
#include "storage/store/StorageTargets.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

constexpr TargetId kTarget{1};
constexpr VersionedChainId kChain{ChainId{1}, ChainVer{1}};

PlacementIdentity placement(uint64_t attempt) {
  return *PlacementIdentity::create(kChain, {kTarget}, kTarget, Uuid::from(40, attempt));
}

ReplaceCacheChunkItem replaceItem(ChunkId chunkId, uint64_t generation, uint64_t operation) {
  ReplaceCacheChunkItem item;
  item.key = {kChain, chunkId};
  item.cacheGeneration = cache::CacheGeneration{generation};
  item.operationId = Uuid::from(41, operation);
  item.data = {'c', 'a', 'c', 'h', 'e'};
  item.chunkSize = 128_KB;
  item.checksumType = ChecksumType::CRC32C;
  item.logicalKey = cache::CacheBlockKey{100, cache::CacheBlockIndex{static_cast<uint32_t>(generation)}};
  item.permit = PermitIdentity{Uuid::from(42, operation),
                               placement(generation),
                               1,
                               {{kTarget, static_cast<uint64_t>(item.chunkSize)}}};
  item.descriptor =
      CacheChunkDescriptor{*item.logicalKey, item.cacheGeneration, item.permit->placement, kTarget, 1000, 1000};
  return item;
}

RetireCacheChunkItem retireItem(ChunkId chunkId, uint64_t generation, Uuid operationId) {
  return {{kChain, chunkId}, cache::CacheGeneration{generation}, operationId};
}

Result<cache::CacheGeneration> prepareRead(StorageTarget &target, ChunkId chunkId) {
  BatchReadReq request;
  request.payloads.emplace_back();
  request.payloads.front().key = {kChain, chunkId};
  request.payloads.front().length = 5;
  BatchReadRsp response;
  response.results.resize(1);
  BatchReadJob job(request.payloads, response.results, ChecksumType::NONE);
  job.front().state().storageTarget = &target;
  RETURN_ON_ERROR(target.aioPrepareRead(job.front()));
  return job.front().result().cacheGeneration;
}

class TestReplicaRetireOperation : public ::testing::TestWithParam<bool> {};

TEST_P(TestReplicaRetireOperation, DurableTombstoneIsIdempotentAndGenerationFenced) {
  folly::test::TemporaryDirectory directory;
  StorageTargets::Config config;
  config.set_target_num_per_path(1);
  config.set_target_paths({directory.path()});
  config.set_disk_roles({StorageRole::CACHE_ONLY});
  config.set_allow_disk_without_uuid(true);
  config.storage_target().file_store().set_preopen_chunk_size_list({128_KB});

  StorageTargets::CreateConfig create;
  create.set_target_ids({kTarget});
  create.set_chunk_size_list({128_KB});
  create.set_physical_file_count(2);
  create.set_allow_disk_without_uuid(true);
  create.set_only_chunk_engine(GetParam());
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.create(create));
    targetMap.release();
  }

  CPUExecutorGroup executor(4, "replica-retire");
  folly::CPUThreadPoolExecutor background(2);
  const ChunkId chunk{0xDA, 1};
  const ChunkId absent{0xDA, 2};
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.load(executor));
    auto targetResult = targetMap.snapshot()->getTarget(kTarget);
    ASSERT_OK(targetResult);
    auto target = (*targetResult)->storageTarget;

    auto first = replaceItem(chunk, 1, 1);
    ASSERT_OK(first.valid());
    ASSERT_OK(target->replaceCacheChunk(first, background));
    auto firstRead = prepareRead(*target, chunk);
    ASSERT_OK(firstRead);
    ASSERT_EQ(*firstRead, cache::CacheGeneration{1});
    ASSERT_OK(target->setChainId(ChainId{2}));

    auto operation = Uuid::from(43, 1);
    auto retired = target->retireCacheChunkDurable(retireItem(chunk, 1, operation));
    ASSERT_OK(retired);
    ASSERT_TRUE(retired->retired);
    ASSERT_EQ(retired->cacheGeneration, cache::CacheGeneration{1});
    ASSERT_OK(target->retireCacheChunkDurable(retireItem(chunk, 1, operation)));
    ASSERT_OK(target->retireCacheChunkDurable(retireItem(chunk, 1, Uuid::from(43, 2))));
    ASSERT_ERROR(prepareRead(*target, chunk), CacheCode::kNotFound);
    ASSERT_ERROR(target->replaceCacheChunk(first, background), CacheCode::kStaleGeneration);

    auto second = replaceItem(chunk, 2, 2);
    ASSERT_OK(target->replaceCacheChunk(second, background));
    ASSERT_ERROR(target->retireCacheChunkDurable(retireItem(chunk, 1, operation)), CacheCode::kGenerationAdvanced);
    auto secondRead = prepareRead(*target, chunk);
    ASSERT_OK(secondRead);
    ASSERT_EQ(*secondRead, cache::CacheGeneration{2});

    auto missingRetire = target->retireCacheChunkDurable(retireItem(absent, 5, Uuid::from(43, 5)));
    ASSERT_OK(missingRetire);
    ASSERT_TRUE(missingRetire->retired);
    ASSERT_EQ(missingRetire->cacheGeneration, cache::CacheGeneration{5});

    target.reset();
    targetMap.release();
  }

  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.load(executor));
    auto targetResult = targetMap.snapshot()->getTarget(kTarget);
    ASSERT_OK(targetResult);
    auto target = (*targetResult)->storageTarget;
    auto tombstone = target->queryCacheChunk(CacheChunkKey{kChain, absent});
    ASSERT_OK(tombstone);
    ASSERT_TRUE(tombstone->retired);
    ASSERT_EQ(tombstone->cacheGeneration, cache::CacheGeneration{5});
    ASSERT_ERROR(target->replaceCacheChunk(replaceItem(absent, 4, 4), background), CacheCode::kStaleGeneration);
    target.reset();
    targetMap.release();
  }
}

INSTANTIATE_TEST_SUITE_P(ChunkStoreAndChunkEngine, TestReplicaRetireOperation, ::testing::Values(false, true));

TEST(TestReplicaRetireWire, RequiresExactPlacementChain) {
  RetireCacheReplicaItem item;
  item.key = {kChain, ChunkId{0xDA, 3}};
  item.targetId = kTarget;
  item.expectedGeneration = cache::CacheGeneration{1};
  item.placement = placement(1);
  item.evictionEpoch = cache::EvictionEpoch{1};
  item.operationId = Uuid::from(44, 1);
  ASSERT_OK(item.valid());
  item.key.vChainId.chainVer = ChainVer{2};
  ASSERT_ERROR(item.valid(), CacheCode::kPlacementMismatch);
}

}  // namespace
}  // namespace hf3fs::storage::test
