#include <gtest/gtest.h>

#include "common/serde/Serde.h"
#include "fbs/cache_manager/Service.h"
#include "fbs/meta/Service.h"
#include "fbs/storage/Service.h"

namespace hf3fs::cache::test {
namespace {

static_assert(meta::MetaSerde<>::importOriginFileMethodId == 22);
static_assert(meta::MetaSerde<>::listCacheBlocksMethodId == 33);
static_assert(storage::StorageSerde<>::replaceCacheChunksMethodId == 18);
static_assert(storage::StorageSerde<>::queryCacheChunkGenerationsMethodId == 20);
static_assert(cache_manager::CacheManagerSerde<>::ensureCachedMethodId == 1);
static_assert(cache_manager::CacheManagerSerde<>::getCacheStatusMethodId == 4);

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
  request.chunkIds.resize(storage::kMaxCacheStorageBatchItems + 1);
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

}  // namespace
}  // namespace hf3fs::cache::test
