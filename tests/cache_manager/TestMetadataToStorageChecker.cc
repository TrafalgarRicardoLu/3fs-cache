#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/reconcile/MetadataToStorageChecker.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PlacementIdentity placement() {
  return {{flat::ChainId{1}, flat::ChainVersion{1}}, {flat::TargetId{1}}, flat::TargetId{1}, Uuid::from(1, 1)};
}

meta::ReconcileCacheBlockStatus readyStatus(uint32_t block) {
  const cache::ReadyIdentity ready{1,
                                   cache::CacheGeneration{block + 1},
                                   static_cast<uint8_t>(storage::ChecksumType::CRC32C),
                                   block + 17,
                                   4096};
  const storage::PermitIdentity permit{Uuid::from(2, 1), placement(), 1, {{flat::TargetId{1}, 4096}}};
  meta::ReconcileCacheBlockStatus status;
  status.key = {7, cache::CacheBlockIndex{block}};
  status.state = cache::CacheBlockState::READY;
  status.blockLength = 4096;
  status.ready = ready;
  status.placement = placement();
  status.permit = permit;
  return status;
}

CacheReplicaObservation matchingObservation(const meta::ReconcileCacheBlockStatus &status) {
  storage::CacheChunkGenerationInfo generation{
      status.ready->cacheGeneration,
      false,
      status.blockLength,
      {static_cast<storage::ChecksumType>(status.ready->checksumType), status.ready->checksumValue}};
  storage::CacheChunkDescriptor descriptor{status.key,
                                           status.ready->cacheGeneration,
                                           *status.placement,
                                           flat::TargetId{1},
                                           1,
                                           1};
  return {generation, descriptor};
}

meta::ReconcileCacheBlockStatus cleaningStatus(uint32_t block) {
  auto status = readyStatus(block);
  status.state = cache::CacheBlockState::CLEANING;
  status.cleanupEpoch = cache::CleanupEpoch{3};
  status.terminalState = cache::CleanupTerminalState::REENQUEUE;
  status.deleteGeneration = status.ready->cacheGeneration;
  return status;
}

meta::ReconcileCacheBlockStatus evictingStatus(uint32_t block) {
  auto status = readyStatus(block);
  status.state = cache::CacheBlockState::EVICTING;
  status.evictionEpoch = cache::EvictionEpoch{4};
  status.retireOperationId = Uuid::from(5, block + 1);
  status.evictionReason = cache::EvictionReason::CAPACITY_WATERMARK;
  return status;
}

class ReconcileBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId inode) final {
    auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                                 "bucket",
                                                 "key",
                                                 {cache::VersionSelectorType::VERSION_ID, "v1"}};
    co_return meta::Inode{
        inode,
        meta::InodeData{
            meta::OriginFile{4096, meta::Layout::newEmpty(flat::ChainTableId{1}, 4096, 1), std::move(object)}}};
  }
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
  CoTryTask<meta::ListReconcileCacheBlocksRsp> listReconcileCacheBlocks(std::optional<cache::CacheBlockKey> after,
                                                                        uint32_t limit) final {
    size_t begin = 0;
    if (after) {
      while (begin < records.size() && records[begin].key != *after) ++begin;
      if (begin < records.size()) ++begin;
    }
    auto end = std::min(records.size(), begin + limit);
    meta::ListReconcileCacheBlocksRsp response;
    response.items.assign(records.begin() + begin, records.begin() + end);
    response.more = end < records.size();
    co_return response;
  }
  CoTryTask<CacheReplicaObservation> queryReconcile(const meta::ReconcileCacheBlockStatus &) final {
    auto index = queryCalls++;
    if (index >= observations.size()) co_return makeError(CacheCode::kUnavailable);
    co_return std::move(observations[index]);
  }
  CoTryTask<meta::BeginCleanCacheBlockResult> beginClean(const meta::BeginCleanCacheBlockItem &item) final {
    cleaned.push_back(item);
    if (staleReady) co_return makeError(CacheCode::kStateConflict);
    auto generation = item.observedGeneration.value_or(item.expectedReady->cacheGeneration);
    co_return meta::BeginCleanCacheBlockResult{item.key, cache::CleanupEpoch{1}, generation, placement()};
  }
  CoTryTask<storage::CacheChunkGenerationInfo> retire(const meta::Inode &,
                                                      cache::CacheBlockIndex,
                                                      cache::CacheGeneration generation) final {
    co_return storage::CacheChunkGenerationInfo{generation, true, 0, {}};
  }
  CoTryTask<void> finishClean(const meta::FinishCleanCacheBlockItem &) final {
    ++finished;
    co_return Void{};
  }
  CoTryTask<bool> coordinateRetire(const meta::CacheEvictionIdentity &identity) final {
    evictions.push_back(identity);
    co_return true;
  }

  std::vector<meta::ReconcileCacheBlockStatus> records;
  std::vector<Result<CacheReplicaObservation>> observations;
  std::vector<meta::BeginCleanCacheBlockItem> cleaned;
  std::vector<meta::CacheEvictionIdentity> evictions;
  size_t queryCalls{0};
  int finished{0};
  bool staleReady{false};
};

TEST(TestMetadataToStorageChecker, KeepsMatchingReplicasAcrossPages) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->records = {readyStatus(0), readyStatus(1)};
  backend->observations = {matchingObservation(backend->records[0]), matchingObservation(backend->records[1])};
  CacheCleanupWorker cleanup(backend);
  MetadataToStorageChecker checker(backend, cleanup, 1);
  auto result = folly::coro::blockingWait(checker.run());
  ASSERT_OK(result);
  EXPECT_EQ(result->scanned, 2);
  EXPECT_EQ(result->matched, 2);
  EXPECT_EQ(result->repaired, 0);
  EXPECT_TRUE(backend->cleaned.empty());
}

TEST(TestMetadataToStorageChecker, RepairsMissingReplicaWithReadyFence) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->records = {readyStatus(0)};
  backend->observations = {makeError(StorageClientCode::kChunkNotFound)};
  CacheCleanupWorker cleanup(backend);
  MetadataToStorageChecker checker(backend, cleanup, 8);
  auto result = folly::coro::blockingWait(checker.run());
  ASSERT_OK(result);
  EXPECT_EQ(result->missing, 1);
  EXPECT_EQ(result->repaired, 1);
  ASSERT_EQ(backend->cleaned.size(), 1);
  EXPECT_EQ(backend->cleaned[0].expectedReady, backend->records[0].ready);
  EXPECT_FALSE(backend->cleaned[0].observedGeneration.has_value());
  EXPECT_EQ(backend->finished, 1);
}

TEST(TestMetadataToStorageChecker, RepairsLengthAndChecksumMismatch) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->records = {readyStatus(0), readyStatus(1)};
  auto wrongLength = matchingObservation(backend->records[0]);
  wrongLength.generation.length = 2048;
  auto wrongChecksum = matchingObservation(backend->records[1]);
  ++wrongChecksum.generation.checksum.value;
  backend->observations = {wrongLength, wrongChecksum};
  CacheCleanupWorker cleanup(backend);
  MetadataToStorageChecker checker(backend, cleanup, 8);
  auto result = folly::coro::blockingWait(checker.run());
  ASSERT_OK(result);
  EXPECT_EQ(result->mismatched, 2);
  EXPECT_EQ(result->repaired, 2);
  ASSERT_EQ(backend->cleaned.size(), 2);
  EXPECT_EQ(backend->cleaned[0].observedGeneration, std::optional{backend->records[0].ready->cacheGeneration});
}

TEST(TestMetadataToStorageChecker, DoesNotMutateOnUnavailableAndAcceptsOldEventRace) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->records = {readyStatus(0), readyStatus(1)};
  auto mismatch = matchingObservation(backend->records[1]);
  mismatch.descriptor.generation = cache::CacheGeneration{99};
  backend->observations = {makeError(CacheCode::kUnavailable), mismatch};
  backend->staleReady = true;
  CacheCleanupWorker cleanup(backend);
  MetadataToStorageChecker checker(backend, cleanup, 8);
  auto result = folly::coro::blockingWait(checker.run());
  ASSERT_OK(result);
  EXPECT_EQ(result->retryable, 1);
  EXPECT_EQ(result->conflicts, 1);
  EXPECT_EQ(result->repaired, 0);
  ASSERT_EQ(backend->cleaned.size(), 1);
  EXPECT_EQ(backend->cleaned[0].key, backend->records[1].key);
}

TEST(TestMetadataToStorageChecker, ReplaysCleaningAndEvictingWithPersistedIdentity) {
  auto backend = std::make_shared<ReconcileBackend>();
  backend->records = {cleaningStatus(0), evictingStatus(1)};
  CacheCleanupWorker cleanup(backend);
  MetadataToStorageChecker checker(backend, cleanup, 8);
  auto result = folly::coro::blockingWait(checker.run());
  ASSERT_OK(result);
  EXPECT_EQ(result->scanned, 2);
  EXPECT_EQ(result->repaired, 2);
  ASSERT_EQ(backend->cleaned.size(), 1);
  EXPECT_EQ(backend->cleaned[0].terminalState, cache::CleanupTerminalState::REENQUEUE);
  EXPECT_EQ(backend->cleaned[0].observedGeneration, std::optional{backend->records[0].deleteGeneration});
  ASSERT_EQ(backend->evictions.size(), 1);
  EXPECT_EQ(backend->evictions[0].evictionEpoch, backend->records[1].evictionEpoch);
  EXPECT_EQ(backend->evictions[0].retireOperationId, backend->records[1].retireOperationId);
  EXPECT_EQ(backend->queryCalls, 0);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
