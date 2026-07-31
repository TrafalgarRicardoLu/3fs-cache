#include <gtest/gtest.h>

#include "common/serde/Serde.h"
#include "fbs/cache_manager/Service.h"
#include "fbs/meta/Service.h"
#include "fbs/storage/Service.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::test {
namespace {

static_assert(meta::MetaSerde<>::importOriginFileMethodId == 22);
static_assert(meta::MetaSerde<>::listCacheBlocksMethodId == 33);
static_assert(storage::StorageSerde<>::replaceCacheChunksMethodId == 18);
static_assert(storage::StorageSerde<>::queryCacheChunkGenerationsMethodId == 20);
static_assert(cache_manager::CacheManagerSerde<>::ensureCachedMethodId == 1);
static_assert(cache_manager::CacheManagerSerde<>::getCacheStatusMethodId == 4);
static_assert(meta::MetaSerde<>::updateCacheBlockAccessMethodId == 34);
static_assert(meta::MetaSerde<>::listCacheEventDeadLettersMethodId == 38);
static_assert(storage::StorageSerde<>::queryCacheSpaceMethodId == 21);
static_assert(storage::StorageSerde<>::coordinateCacheRetiresMethodId == 27);
static_assert(cache_manager::CacheManagerSerde<>::reportCacheAccessMethodId == 5);
static_assert(cache_manager::CacheManagerSerde<>::getPhase2CacheStatusMethodId == 6);

TEST(ServiceContracts, RejectsOversizedMetadataBatch) {
  meta::BatchImportOriginFilesReq request;
  request.entries.resize(meta::kMaxCacheBatchItems + 1);
  EXPECT_TRUE(request.valid().hasError());

  meta::EnqueueCacheBlocksReq enqueue;
  enqueue.items.resize(meta::kMaxCacheBatchItems + 1);
  EXPECT_TRUE(enqueue.valid().hasError());
}

TEST(ServiceContracts, RejectsOversizedStorageBatch) {
  storage::QueryCacheChunkGenerationsReq request;
  request.keys.resize(storage::kMaxCacheStorageBatchItems + 1);
  EXPECT_TRUE(request.valid().hasError());
}

TEST(ServiceContracts, CacheManagerSerdeRoundTrip) {
  cache_manager::EnsureCachedReq original;
  original.service = cache_manager::ServiceIdentity{"cache-manager", "token"};
  original.inode = meta::InodeId{42};
  original.beginBlock = CacheBlockIndex{3};
  original.blockCount = 10;
  original.cacheProtocolVersion = kCacheProtocolVersion;

  cache_manager::EnsureCachedReq decoded;
  ASSERT_FALSE(serde::deserialize(decoded, serde::serialize(original)).hasError());
  EXPECT_EQ(decoded.inode, original.inode);
  EXPECT_EQ(decoded.beginBlock, original.beginBlock);
  EXPECT_EQ(decoded.blockCount, original.blockCount);
}

TEST(ServiceContracts, Phase2ContractsRoundTrip) {
  cache_manager::ReportCacheAccessReq original;
  original.items.push_back({CacheBlockKey{42, CacheBlockIndex{3}}, CacheGeneration{7}, 123});
  original.cacheProtocolVersion = kCacheProtocolVersion;

  cache_manager::ReportCacheAccessReq decoded;
  ASSERT_TRUE(serde::deserialize(decoded, serde::serialize(original)));
  ASSERT_EQ(decoded.items.size(), 1);
  EXPECT_EQ(decoded.items.front().key, original.items.front().key);
  EXPECT_EQ(decoded.items.front().generation, CacheGeneration{7});
}

TEST(ServiceContracts, RejectsOversizedPhase2Batches) {
  cache_manager::ReportCacheAccessReq access;
  access.items.resize(kMaxPhase2BatchItems + 1);
  ASSERT_ERROR(access.valid(), CacheCode::kRequestTooLarge);

  meta::BeginEvictCacheBlocksReq evict;
  evict.service = meta::CacheServiceIdentity{"cache-manager", "token"};
  evict.items.resize(kMaxPhase2BatchItems + 1);
  ASSERT_ERROR(evict.valid(), CacheCode::kRequestTooLarge);

  storage::PrepareCachePermitsReq permits;
  permits.items.resize(kMaxPhase2BatchItems + 1);
  ASSERT_ERROR(permits.valid(), CacheCode::kRequestTooLarge);
}

TEST(ServiceContracts, Phase2CapabilityRejectsOldOrDisabledComponents) {
  ASSERT_ERROR(checkPhase2Capability(kCacheProtocolVersion - 1, true), CacheCode::kUpgradeRequired);
  ASSERT_ERROR(checkPhase2Capability(kCacheProtocolVersion, false), CacheCode::kFeatureDisabled);
  EXPECT_TRUE(checkPhase2Capability(kCacheProtocolVersion, true));
}

}  // namespace
}  // namespace hf3fs::cache::test
