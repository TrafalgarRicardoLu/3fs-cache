#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>

#include "cache_manager/reconcile/StorageToMetadataChecker.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PlacementIdentity placement(std::vector<flat::TargetId> targets = {flat::TargetId{1}}) {
  return {{flat::ChainId{1}, flat::ChainVersion{1}}, targets, targets.front(), Uuid::from(1, 1)};
}

storage::CacheInventoryEntry inventoryEntry(uint32_t block,
                                            uint64_t generation,
                                            flat::TargetId target = flat::TargetId{1},
                                            std::vector<flat::TargetId> targets = {flat::TargetId{1}}) {
  auto placed = placement(std::move(targets));
  storage::CacheChunkDescriptor descriptor{{7, cache::CacheBlockIndex{block}},
                                           cache::CacheGeneration{generation},
                                           placed,
                                           target,
                                           1,
                                           1};
  return {target,
          storage::PhysicalDiskId{Uuid::from(2, target.toUnderType())},
          {placed.versionedChain, storage::ChunkId{block + 1, 0}},
          descriptor,
          {cache::CacheGeneration{generation}, false, 4096, {storage::ChecksumType::CRC32C, block + 1}}};
}

meta::ReconcileCacheBlockStatus status(uint32_t block, cache::CacheBlockState state, uint64_t generation) {
  meta::ReconcileCacheBlockStatus result;
  result.key = {7, cache::CacheBlockIndex{block}};
  result.state = state;
  if (state == cache::CacheBlockState::NONE) return result;
  result.blockLength = 4096;
  result.cacheGeneration = cache::CacheGeneration{generation};
  const auto placed = placement();
  const storage::PermitIdentity permit{Uuid::from(3, 1), placed, 1, {{flat::TargetId{1}, 4096}}};
  if (state == cache::CacheBlockState::LOADING) result.permit = permit;
  if (state == cache::CacheBlockState::READY || state == cache::CacheBlockState::CLEANING ||
      state == cache::CacheBlockState::EVICTING) {
    result.ready = cache::ReadyIdentity{1,
                                        cache::CacheGeneration{generation},
                                        static_cast<uint8_t>(storage::ChecksumType::CRC32C),
                                        block + 1,
                                        4096};
    result.placement = placed;
    result.permit = permit;
  }
  if (state == cache::CacheBlockState::CLEANING) {
    result.cleanupEpoch = cache::CleanupEpoch{1};
    result.deleteGeneration = cache::CacheGeneration{generation};
  }
  if (state == cache::CacheBlockState::EVICTING) {
    result.evictionEpoch = cache::EvictionEpoch{1};
    result.retireOperationId = Uuid::from(4, block + 1);
    result.evictionReason = cache::EvictionReason::CAPACITY_WATERMARK;
  }
  return result;
}

class ReconcileBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final { co_return makeError(StatusCode::kNotImplemented); }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &,
                                                       cache::CacheBlockIndex,
                                                       const meta::CacheBlockLease &,
                                                       std::vector<uint8_t>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &,
                         const meta::CacheBlockLease &,
                         const storage::CacheChunkGenerationInfo &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::ReconcileCacheBlocksRsp> reconcileCacheBlocks(std::vector<cache::CacheBlockKey> keys) final {
    lookupBatches.push_back(keys);
    meta::ReconcileCacheBlocksRsp response;
    for (const auto &key : keys) response.results.emplace_back(statuses.at(key.block.toUnderType()));
    co_return response;
  }
  CoTryTask<storage::RetireCacheReplicasRsp> retireCacheReplicas(
      std::vector<storage::RetireCacheReplicaItem> items) final {
    retired.insert(retired.end(), items.begin(), items.end());
    storage::RetireCacheReplicasRsp response;
    for (const auto &item : items) {
      if (generationAdvanced) {
        response.results.emplace_back(makeError(CacheCode::kGenerationAdvanced));
      } else {
        response.results.emplace_back(storage::RetireCacheReplicaResult{item.operationId, true});
      }
    }
    co_return response;
  }

  std::map<uint32_t, meta::ReconcileCacheBlockStatus> statuses;
  std::vector<std::vector<cache::CacheBlockKey>> lookupBatches;
  std::vector<storage::RetireCacheReplicaItem> retired;
  bool generationAdvanced{false};
};

TEST(TestStorageToMetadataChecker, AppliesCompleteGenerationAndStateMatrix) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->statuses = {{0, status(0, cache::CacheBlockState::READY, 1)},
                       {1, status(1, cache::CacheBlockState::LOADING, 1)},
                       {2, status(2, cache::CacheBlockState::EVICTING, 1)},
                       {3, status(3, cache::CacheBlockState::CLEANING, 1)},
                       {4, status(4, cache::CacheBlockState::READY, 2)},
                       {5, status(5, cache::CacheBlockState::READY, 1)},
                       {6, status(6, cache::CacheBlockState::NONE, 0)},
                       {7, status(7, cache::CacheBlockState::FAILED, 1)},
                       {8, status(8, cache::CacheBlockState::INVALID, 1)},
                       {9, status(9, cache::CacheBlockState::QUEUED, 1)}};
  TargetCacheInventory inventory{flat::TargetId{1}, Uuid::from(5, 1), {}};
  for (uint32_t block = 0; block < 10; ++block) {
    auto generation = block == 5 ? 2 : 1;
    inventory.entries.push_back(inventoryEntry(block, generation));
  }
  StorageToMetadataChecker checker(backend, 3);
  auto result = folly::coro::blockingWait(checker.run(std::span{&inventory, 1}));
  ASSERT_OK(result);
  EXPECT_EQ(result->scanned, 10);
  EXPECT_EQ(result->delegated, 4);
  EXPECT_EQ(result->older, 1);
  EXPECT_EQ(result->newer, 1);
  EXPECT_EQ(result->orphans, 2);
  EXPECT_EQ(result->retired, 3);
  EXPECT_EQ(result->conflicts, 3);
  ASSERT_EQ(backend->retired.size(), 3);
  for (const auto &item : backend->retired) {
    EXPECT_EQ(item.expectedGeneration, cache::CacheGeneration{1});
    EXPECT_EQ(item.targetId, flat::TargetId{1});
    EXPECT_EQ(item.placement, placement());
  }
}

TEST(TestStorageToMetadataChecker, DeduplicatesMetadataLookupButHandlesEveryReplica) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->statuses.emplace(0, status(0, cache::CacheBlockState::NONE, 0));
  const std::vector targets{flat::TargetId{1}, flat::TargetId{2}};
  TargetCacheInventory first{flat::TargetId{1}, Uuid::from(6, 1), {inventoryEntry(0, 1, flat::TargetId{1}, targets)}};
  TargetCacheInventory second{flat::TargetId{2}, Uuid::from(6, 2), {inventoryEntry(0, 1, flat::TargetId{2}, targets)}};
  const std::vector inventories{first, second};
  StorageToMetadataChecker checker(backend, 8);
  auto result = folly::coro::blockingWait(checker.run(inventories));
  ASSERT_OK(result);
  EXPECT_EQ(result->scanned, 2);
  EXPECT_EQ(result->orphans, 2);
  EXPECT_EQ(result->retired, 2);
  ASSERT_EQ(backend->lookupBatches.size(), 1);
  EXPECT_EQ(backend->lookupBatches.front().size(), 1);
  ASSERT_EQ(backend->retired.size(), 2);
  EXPECT_NE(backend->retired[0].targetId, backend->retired[1].targetId);
}

TEST(TestStorageToMetadataChecker, GenerationFenceProtectsWriteAfterInventoryScan) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->statuses.emplace(0, status(0, cache::CacheBlockState::READY, 2));
  backend->generationAdvanced = true;
  TargetCacheInventory inventory{flat::TargetId{1}, Uuid::from(7, 1), {inventoryEntry(0, 1)}};
  StorageToMetadataChecker checker(backend, 8);
  auto result = folly::coro::blockingWait(checker.run(std::span{&inventory, 1}));
  ASSERT_OK(result);
  EXPECT_EQ(result->older, 1);
  EXPECT_EQ(result->retired, 0);
  EXPECT_EQ(result->conflicts, 1);
  ASSERT_EQ(backend->retired.size(), 1);
  EXPECT_EQ(backend->retired.front().expectedGeneration, cache::CacheGeneration{1});
}

TEST(TestStorageToMetadataChecker, DryRunReportsWithoutMutation) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->statuses.emplace(0, status(0, cache::CacheBlockState::NONE, 0));
  TargetCacheInventory inventory{flat::TargetId{1}, Uuid::from(8, 1), {inventoryEntry(0, 1)}};
  StorageToMetadataChecker checker(backend, 8, true);
  auto result = folly::coro::blockingWait(checker.run(std::span{&inventory, 1}));
  ASSERT_OK(result);
  EXPECT_EQ(result->orphans, 1);
  EXPECT_EQ(result->retired, 0);
  EXPECT_TRUE(backend->retired.empty());
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
