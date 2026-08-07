#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>

#include "cache_manager/reconcile/StorageInventory.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::CacheInventoryEntry inventoryEntry(uint64_t chunk, flat::TargetId target = flat::TargetId{1}) {
  storage::PlacementIdentity placement{{flat::ChainId{1}, flat::ChainVersion{1}},
                                       {target},
                                       target,
                                       Uuid::from(1, chunk)};
  storage::CacheChunkDescriptor descriptor{{7, cache::CacheBlockIndex{static_cast<uint32_t>(chunk)}},
                                           cache::CacheGeneration{chunk},
                                           placement,
                                           target,
                                           1,
                                           1};
  return {target,
          storage::PhysicalDiskId{Uuid::from(2, target.toUnderType())},
          {{flat::ChainId{1}, flat::ChainVersion{1}}, storage::ChunkId{chunk, 0}},
          descriptor,
          {cache::CacheGeneration{chunk}, false, 4096, {storage::ChecksumType::CRC32C, static_cast<uint32_t>(chunk)}}};
}

storage::ListCacheInventoryRsp page(std::vector<storage::CacheInventoryEntry> entries,
                                    std::string cursor,
                                    bool done,
                                    Uuid epoch = Uuid::from(3, 1)) {
  return {std::move(entries), std::move(cursor), done, epoch};
}

class InventoryBackend : public CacheManagerBackend {
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
  CoTryTask<storage::ListCacheInventoryRsp> listCacheInventory(storage::ListCacheInventoryReq request) final {
    requests.push_back(request);
    auto &targetPages = pages[request.targetId];
    if (targetPages.empty()) co_return makeError(CacheCode::kUnavailable, "inventory node unavailable");
    auto result = std::move(targetPages.front());
    targetPages.erase(targetPages.begin());
    co_return result;
  }

  std::map<flat::TargetId, std::vector<Result<storage::ListCacheInventoryRsp>>> pages;
  std::vector<storage::ListCacheInventoryReq> requests;
};

TEST(TestStorageInventory, ReadsStablePagesWithBoundedRequests) {
  auto backend = std::make_shared<InventoryBackend>();
  backend->pages[flat::TargetId{1}] = {page({inventoryEntry(1)}, "next", false), page({inventoryEntry(2)}, {}, true)};
  StorageInventoryReader reader(backend, 1, 2);
  auto result = folly::coro::blockingWait(reader.readTarget(flat::TargetId{1}));
  ASSERT_OK(result);
  ASSERT_EQ(result->entries.size(), 2);
  EXPECT_EQ(result->inventoryEpoch, Uuid::from(3, 1));
  ASSERT_EQ(backend->requests.size(), 2);
  EXPECT_EQ(backend->requests[0].limit, 1);
  EXPECT_EQ(backend->requests[0].cacheProtocolVersion, cache::kCachePhase4ProtocolVersion);
  EXPECT_EQ(backend->requests[1].cursor, "next");
}

TEST(TestStorageInventory, RejectsRepeatedCursorAndChangedEpoch) {
  auto repeated = std::make_shared<InventoryBackend>();
  repeated->pages[flat::TargetId{1}] = {page({inventoryEntry(1)}, "same", false),
                                        page({inventoryEntry(2)}, "same", false)};
  StorageInventoryReader repeatedReader(repeated, 1, 1);
  ASSERT_ERROR(folly::coro::blockingWait(repeatedReader.readTarget(flat::TargetId{1})), CacheCode::kInvalidResponse);

  auto changed = std::make_shared<InventoryBackend>();
  changed->pages[flat::TargetId{1}] = {page({inventoryEntry(1)}, "next", false),
                                       page({inventoryEntry(2)}, {}, true, Uuid::from(3, 2))};
  StorageInventoryReader changedReader(changed, 1, 1);
  ASSERT_ERROR(folly::coro::blockingWait(changedReader.readTarget(flat::TargetId{1})), CacheCode::kInvalidResponse);
}

TEST(TestStorageInventory, RejectsMalformedOrForeignPages) {
  auto malformed = std::make_shared<InventoryBackend>();
  malformed->pages[flat::TargetId{1}] = {page({}, "next", false)};
  StorageInventoryReader malformedReader(malformed, 4, 1);
  ASSERT_ERROR(folly::coro::blockingWait(malformedReader.readTarget(flat::TargetId{1})), CacheCode::kInvalidResponse);

  auto foreign = std::make_shared<InventoryBackend>();
  foreign->pages[flat::TargetId{1}] = {page({inventoryEntry(1, flat::TargetId{2})}, {}, true)};
  StorageInventoryReader foreignReader(foreign, 4, 1);
  ASSERT_ERROR(folly::coro::blockingWait(foreignReader.readTarget(flat::TargetId{1})), CacheCode::kInvalidResponse);
}

TEST(TestStorageInventory, PreservesPartialTargetFailure) {
  auto backend = std::make_shared<InventoryBackend>();
  backend->pages[flat::TargetId{1}] = {page({inventoryEntry(1)}, {}, true)};
  backend->pages[flat::TargetId{2}] = {makeError(CacheCode::kUnavailable, "inventory node unavailable")};
  StorageInventoryReader reader(backend, 4, 2);
  const std::vector targets{flat::TargetId{1}, flat::TargetId{2}};
  auto results = folly::coro::blockingWait(reader.readTargets(targets));
  ASSERT_EQ(results.size(), 2);
  ASSERT_OK(results[0]);
  ASSERT_ERROR(results[1], CacheCode::kUnavailable);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
