#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/service/AdminCleanupCacheBlocks.h"
#include "cache_manager/service/ReportCacheBlockInvalid.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

class CleanupBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final {
    auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                                 "bucket",
                                                 "key",
                                                 {cache::VersionSelectorType::VERSION_ID, "v1"}};
    co_return meta::Inode{
        meta::InodeId{5},
        meta::InodeData{
            meta::OriginFile{4096, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), std::move(object)}}};
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
  CoTryTask<void> validateReport(const ReportCacheBlockInvalidReq &) final {
    ++validated;
    co_return Void{};
  }
  CoTryTask<void> authorizeAdmin(const flat::UserInfo &, std::optional<meta::InodeId>) final {
    ++authorized;
    if (denyAdmin) co_return makeError(MetaCode::kNoPermission);
    co_return Void{};
  }
  CoTryTask<meta::BeginCleanCacheBlockResult> beginClean(const meta::BeginCleanCacheBlockItem &item) final {
    ++begins;
    if (staleBegin) co_return makeError(CacheCode::kStateConflict);
    auto generation = item.observedGeneration.value_or(cache::CacheGeneration{1});
    auto epoch = changeEpochOnUpdate && begins > 1 ? cache::CleanupEpoch{4} : cache::CleanupEpoch{3};
    std::optional<storage::PlacementIdentity> placement;
    if (withPlacement) {
      placement = storage::PlacementIdentity{{flat::ChainId{2}, flat::ChainVersion{3}},
                                             {flat::TargetId{1}},
                                             flat::TargetId{1},
                                             Uuid::from(8, 1)};
    }
    co_return meta::BeginCleanCacheBlockResult{item.key, epoch, generation, placement};
  }
  CoTryTask<storage::CacheChunkGenerationInfo> retire(const meta::Inode &,
                                                      cache::CacheBlockIndex,
                                                      cache::CacheGeneration generation) final {
    ++retires;
    if (advanceOnce && generation == cache::CacheGeneration{1}) {
      co_return makeError(CacheCode::kGenerationAdvanced);
    }
    co_return storage::CacheChunkGenerationInfo{generation, true, 0, {}};
  }
  CoTryTask<storage::CacheChunkGenerationInfo> query(const meta::Inode &, cache::CacheBlockIndex) final {
    ++queries;
    co_return storage::CacheChunkGenerationInfo{cache::CacheGeneration{2}, false, 4096, {}};
  }
  CoTryTask<storage::CacheChunkGenerationInfo> retirePlaced(const meta::Inode &inode,
                                                            cache::CacheBlockIndex block,
                                                            cache::CacheGeneration generation,
                                                            const storage::PlacementIdentity &placement) final {
    ++placedRetires;
    observedPlacement = placement;
    co_return co_await retire(inode, block, generation);
  }
  CoTryTask<storage::CacheChunkGenerationInfo> queryPlaced(const meta::Inode &inode,
                                                           cache::CacheBlockIndex block,
                                                           const storage::PlacementIdentity &placement) final {
    ++placedQueries;
    observedPlacement = placement;
    co_return co_await query(inode, block);
  }
  CoTryTask<void> finishClean(const meta::FinishCleanCacheBlockItem &item) final {
    ++finishes;
    finished = item;
    co_return Void{};
  }

  bool advanceOnce{false};
  bool changeEpochOnUpdate{false};
  bool staleBegin{false};
  bool denyAdmin{false};
  bool withPlacement{false};
  int validated{0};
  int authorized{0};
  int begins{0};
  int retires{0};
  int queries{0};
  int finishes{0};
  int placedRetires{0};
  int placedQueries{0};
  std::optional<storage::PlacementIdentity> observedPlacement;
  meta::FinishCleanCacheBlockItem finished;
};

meta::BeginCleanCacheBlockItem cleanupItem() {
  return {{5, cache::CacheBlockIndex{0}}, std::nullopt, std::nullopt, cache::CleanupTerminalState::REENQUEUE};
}

TEST(TestCacheCleanupWorker, AdvancesDeleteGenerationAndFinishesTombstone) {
  auto backend = std::make_shared<CleanupBackend>();
  backend->advanceOnce = true;
  CacheCleanupWorker worker(backend);
  ASSERT_OK(folly::coro::blockingWait(worker.clean(cleanupItem())));
  ASSERT_EQ(backend->begins, 2);
  ASSERT_EQ(backend->queries, 1);
  ASSERT_EQ(backend->retires, 2);
  ASSERT_EQ(backend->finishes, 1);
  ASSERT_EQ(backend->finished.cleanupEpoch, cache::CleanupEpoch{3});
  ASSERT_EQ(backend->finished.retiredGeneration, cache::CacheGeneration{2});
}

TEST(TestCacheCleanupWorker, RejectsChangedCleanupEpoch) {
  auto backend = std::make_shared<CleanupBackend>();
  backend->advanceOnce = true;
  backend->changeEpochOnUpdate = true;
  CacheCleanupWorker worker(backend);
  ASSERT_ERROR(folly::coro::blockingWait(worker.clean(cleanupItem())), CacheCode::kStateConflict);
  ASSERT_EQ(backend->begins, 2);
  ASSERT_EQ(backend->finishes, 0);
}

TEST(TestCacheCleanupWorker, UsesPersistedPlacementAcrossGenerationAdvance) {
  auto backend = std::make_shared<CleanupBackend>();
  backend->withPlacement = true;
  backend->advanceOnce = true;
  CacheCleanupWorker worker(backend);
  ASSERT_OK(folly::coro::blockingWait(worker.clean(cleanupItem())));
  ASSERT_EQ(backend->placedRetires, 2);
  ASSERT_EQ(backend->placedQueries, 1);
  ASSERT_TRUE(backend->observedPlacement.has_value());
  EXPECT_EQ(backend->observedPlacement->versionedChain,
            (storage::VersionedChainId{flat::ChainId{2}, flat::ChainVersion{3}}));
}

TEST(TestReportCacheBlockInvalid, StaleReadyIsNoOp) {
  auto backend = std::make_shared<CleanupBackend>();
  backend->staleBegin = true;
  CacheCleanupWorker worker(backend);
  ReportCacheBlockInvalid service(backend, worker);
  ReportCacheBlockInvalidReq req;
  req.openSessionId = Uuid::random();
  req.inode = meta::InodeId{5};
  req.expectedReady = {1, cache::CacheGeneration{1}, 1, 2, 4096};
  req.observedGeneration = cache::CacheGeneration{1};
  auto result = folly::coro::blockingWait(service.run(req));
  ASSERT_OK(result);
  ASSERT_FALSE(result->attachedCleanup);
  ASSERT_EQ(backend->validated, 1);
  ASSERT_EQ(backend->retires, 0);
}

TEST(TestAdminCleanupCacheBlocks, AuthenticatesAndReturnsPerBlockResults) {
  auto backend = std::make_shared<CleanupBackend>();
  CacheCleanupWorker worker(backend);
  AdminCleanupCacheBlocks service(backend, worker);
  AdminCleanupCacheBlocksReq req;
  req.inode = meta::InodeId{5};
  req.blockCount = 2;
  auto result = folly::coro::blockingWait(service.run(req));
  ASSERT_OK(result);
  ASSERT_EQ(result->results.size(), size_t{2});
  ASSERT_EQ(result->results[0]->status, CleanupBlockStatus::CLEANED);
  ASSERT_EQ(backend->authorized, 1);

  backend->denyAdmin = true;
  ASSERT_ERROR(folly::coro::blockingWait(service.run(req)), MetaCode::kNoPermission);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
