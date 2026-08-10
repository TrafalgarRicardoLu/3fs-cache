#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache/metrics/CacheMetrics.h"
#include "cache_manager/cleanup/CacheCleanupWorker.h"
#include "cache_manager/recovery/PermitRecovery.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PermitIdentity recoveryPermit(uint64_t generation = 1) {
  storage::PlacementIdentity placement{{flat::ChainId{1}, flat::ChainVersion{1}},
                                       {flat::TargetId{1}},
                                       flat::TargetId{1},
                                       Uuid::from(2, 1)};
  return {Uuid::from(3, 1), std::move(placement), generation, {{flat::TargetId{1}, 4096}}};
}

meta::RecoverableCachePermit recoveryItem(cache::CacheBlockState state = cache::CacheBlockState::QUEUED) {
  meta::RecoverableCachePermit item{{7, cache::CacheBlockIndex{0}}, state, 4096, recoveryPermit()};
  if (state == cache::CacheBlockState::LOADING) {
    item.loaderId = Uuid::from(4, 1);
    item.loadEpoch = 1;
  }
  return item;
}

meta::RecoverableCachePermit phase4LoadingItem(int64_t leaseExpiresAtUs) {
  auto item = recoveryItem(cache::CacheBlockState::LOADING);
  item.leaseExpiresAt = UtcTime::fromMicroseconds(leaseExpiresAtUs);
  item.cacheGeneration = cache::CacheGeneration{1};
  item.placement = item.permit.placement;
  return item;
}

class RecoveryBackend : public CacheManagerBackend {
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
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase> items) final {
    ++replaceCalls;
    replacements.push_back(items.front());
    meta::EnqueueCacheBlocksRsp response;
    if (casConflict) {
      response.results.push_back(makeError(CacheCode::kStateConflict));
      response.permits.push_back(std::nullopt);
    } else {
      response.results.push_back(meta::CacheBlockMutationResult{items.front().key,
                                                                cache::CacheBlockState::QUEUED,
                                                                cache::CacheEnqueueOutcome::QUEUED});
      response.permits.push_back(items.front().permit);
    }
    co_return response;
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
  CoTryTask<storage::CachePermitResult> queryPermit(const storage::PermitIdentity &permit) final {
    ++queryCalls;
    if (queryError) co_return makeError(*queryError);
    co_return storage::CachePermitResult{permit, queryState, queryExpiresAtNs};
  }
  CoTryTask<storage::CachePermitResult> renewPermit(const storage::PermitIdentity &permit, uint64_t expiresAtNs) final {
    renewed.push_back(permit);
    co_return storage::CachePermitResult{permit, cache::CachePermitState::RESERVED, expiresAtNs};
  }
  CoTryTask<storage::CachePermitResult> preparePermit(const storage::PermitIdentity &permit,
                                                      uint64_t expiresAtNs) final {
    prepared.push_back(permit);
    if (prepareError) co_return makeError(*prepareError);
    co_return storage::CachePermitResult{permit, cache::CachePermitState::RESERVED, expiresAtNs};
  }
  CoTryTask<void> releasePermit(const storage::PermitIdentity &permit) final {
    released.push_back(permit);
    co_return Void{};
  }
  CoTryTask<meta::ListRecoverableCachePermitsRsp> listRecoverablePermits(std::optional<cache::CacheBlockKey> after,
                                                                         uint32_t limit) final {
    meta::ListRecoverableCachePermitsRsp response;
    size_t begin = 0;
    if (after) {
      while (begin < records.size() && records[begin].key != *after) ++begin;
      if (begin < records.size()) ++begin;
    }
    auto end = std::min(records.size(), begin + limit);
    response.items.assign(records.begin() + begin, records.begin() + end);
    response.more = end < records.size();
    co_return response;
  }
  CoTryTask<void> refreshRouting() final {
    ++routingRefreshes;
    co_return Void{};
  }
  CoTryTask<void> cancelQueuedAdmission(const cache::CacheBlockKey &key, const storage::PermitIdentity &permit) final {
    cancelled.emplace_back(key, permit);
    co_return Void{};
  }
  CoTryTask<meta::CacheBlockMutationResult> recoverExpiredLoad(const meta::RecoverExpiredCacheLoadItem &item) final {
    recovered.push_back(item);
    if (recoveryError) co_return makeError(*recoveryError);
    co_return meta::CacheBlockMutationResult{item.key, cache::CacheBlockState::CLEANING};
  }
  CoTryTask<meta::BeginCleanCacheBlockResult> beginClean(const meta::BeginCleanCacheBlockItem &item) final {
    ++cleanupBegins;
    co_return meta::BeginCleanCacheBlockResult{item.key,
                                               cache::CleanupEpoch{1},
                                               cache::CacheGeneration{1},
                                               recoveryPermit().placement};
  }
  CoTryTask<storage::CacheChunkGenerationInfo> retire(const meta::Inode &,
                                                      cache::CacheBlockIndex,
                                                      cache::CacheGeneration generation) final {
    ++retires;
    if (cleanupError) co_return makeError(*cleanupError);
    co_return storage::CacheChunkGenerationInfo{generation, true, 0, {}};
  }
  CoTryTask<void> finishClean(const meta::FinishCleanCacheBlockItem &) final {
    ++cleanupFinishes;
    co_return Void{};
  }

  std::vector<meta::RecoverableCachePermit> records;
  std::optional<Status> queryError;
  std::optional<Status> prepareError;
  std::optional<Status> recoveryError;
  std::optional<Status> cleanupError;
  cache::CachePermitState queryState{cache::CachePermitState::RESERVED};
  uint64_t queryExpiresAtNs{2000};
  bool casConflict{false};
  int queryCalls{0};
  int replaceCalls{0};
  int routingRefreshes{0};
  int cleanupBegins{0};
  int retires{0};
  int cleanupFinishes{0};
  std::vector<storage::PermitIdentity> renewed;
  std::vector<storage::PermitIdentity> prepared;
  std::vector<storage::PermitIdentity> released;
  std::vector<meta::CacheBlockRequestBase> replacements;
  std::vector<std::pair<cache::CacheBlockKey, storage::PermitIdentity>> cancelled;
  std::vector<meta::RecoverExpiredCacheLoadItem> recovered;
};

TEST(TestPermitRecovery, RenewsQueuedPermitAndReattachesHint) {
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(recoveryItem());
  HintCoalescer hints;
  PermitRecovery recovery(backend, hints, Uuid::from(9, 1), 1_s, 1, [] { return uint64_t{1000}; });
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_EQ(backend->routingRefreshes, 1);
  ASSERT_EQ(backend->renewed, std::vector<storage::PermitIdentity>{recoveryPermit()});
  auto hint = hints.pop();
  ASSERT_TRUE(hint.has_value());
  ASSERT_EQ(hint->key(), backend->records.front().key);
}

TEST(TestPermitRecovery, AllowsStartupCoordinatorToOwnRoutingRefresh) {
  auto backend = std::make_shared<RecoveryBackend>();
  HintCoalescer hints;
  PermitRecovery recovery(backend, hints, Uuid::from(9, 1), 1_s, 1, [] { return uint64_t{1000}; });
  ASSERT_OK(folly::coro::blockingWait(recovery.run(false)));
  EXPECT_EQ(backend->routingRefreshes, 0);
}

TEST(TestPermitRecovery, ReplacesExpiredPermitWithNewEpoch) {
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(recoveryItem());
  backend->queryError = Status(CacheCode::kNotFound);
  HintCoalescer hints;
  auto newEpoch = Uuid::from(9, 2);
  PermitRecovery recovery(backend, hints, newEpoch, 1_s, 1, [] { return uint64_t{1000}; });
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_EQ(backend->prepared.size(), 1);
  ASSERT_EQ(backend->prepared.front().managerEpoch, newEpoch);
  ASSERT_EQ(backend->prepared.front().permitGeneration, uint64_t{2});
  ASSERT_EQ(backend->replacements.front().expectedPermit, std::optional{recoveryPermit()});
  ASSERT_TRUE(hints.pop().has_value());
}

TEST(TestPermitRecovery, CasLoserReleasesReplacement) {
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(recoveryItem());
  backend->queryError = Status(CacheCode::kNotFound);
  backend->casConflict = true;
  HintCoalescer hints;
  PermitRecovery recovery(backend, hints, Uuid::from(9, 3), 1_s, 1, [] { return uint64_t{1000}; });
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_EQ(backend->released, backend->prepared);
  ASSERT_FALSE(hints.pop().has_value());
}

TEST(TestPermitRecovery, DoesNotReplacePinnedLoadingPermit) {
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(recoveryItem(cache::CacheBlockState::LOADING));
  backend->queryState = cache::CachePermitState::PINNED;
  HintCoalescer hints;
  PermitRecovery recovery(backend, hints, Uuid::from(9, 4), 1_s, 1, [] { return uint64_t{1000}; });
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_TRUE(backend->prepared.empty());
  ASSERT_EQ(backend->replaceCalls, 0);
}

TEST(TestPermitRecovery, MissingLoadingPermitRecoversWithoutWaitingForLeaseExpiry) {
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(phase4LoadingItem(10));
  backend->queryError = Status(CacheCode::kNotFound);
  HintCoalescer hints;
  CacheCleanupWorker cleanup(backend);
  PermitRecovery recovery(
      backend, hints, Uuid::from(9, 8), 1_s, 1, [] { return uint64_t{1000}; }, &cleanup, true);
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_EQ(backend->recovered.size(), 1);
  ASSERT_EQ(backend->cleanupFinishes, 1);
}

TEST(TestPermitRecovery, CancelsQueuedAdmissionWhenCapacityCannotBeRecovered) {
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(recoveryItem());
  backend->queryError = Status(CacheCode::kNotFound);
  backend->prepareError = Status(CacheCode::kCapacityExceeded);
  HintCoalescer hints;
  PermitRecovery recovery(backend, hints, Uuid::from(9, 5), 1_s, 1, [] { return uint64_t{1000}; });
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_EQ(backend->cancelled.size(), 1);
  ASSERT_EQ(backend->released, std::vector<storage::PermitIdentity>{recoveryPermit()});
}

TEST(TestPermitRecovery, DefersPinnedLoadingBeforeMetadataLeaseExpires) {
  cache::metrics::resetForTest();
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(phase4LoadingItem(2));
  backend->queryState = cache::CachePermitState::PINNED;
  HintCoalescer hints;
  CacheCleanupWorker cleanup(backend);
  PermitRecovery recovery(
      backend,
      hints,
      Uuid::from(9, 6),
      1_s,
      1,
      [] { return uint64_t{1000}; },
      &cleanup,
      true);
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_TRUE(backend->recovered.empty());
  ASSERT_EQ(cache::metrics::countForTest(cache::metrics::Event::MANAGER_LEASE_RECOVERY), uint64_t{1});
  ASSERT_EQ(cache::metrics::lastTagsForTest(cache::metrics::Event::MANAGER_LEASE_RECOVERY).reason, "deferred");
}

TEST(TestPermitRecovery, RecoversExpiredLoadingThenCleansAndReleasesPermit) {
  cache::metrics::resetForTest();
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(phase4LoadingItem(1));
  backend->queryError = Status(CacheCode::kNotFound);
  HintCoalescer hints;
  CacheCleanupWorker cleanup(backend);
  PermitRecovery recovery(
      backend,
      hints,
      Uuid::from(9, 7),
      1_s,
      1,
      [] { return uint64_t{1000}; },
      &cleanup,
      true);
  ASSERT_OK(folly::coro::blockingWait(recovery.run()));
  ASSERT_EQ(backend->recovered.size(), size_t{1});
  ASSERT_EQ(backend->recovered.front().expectedPlacement, recoveryPermit().placement);
  ASSERT_EQ(backend->cleanupBegins, 1);
  ASSERT_EQ(backend->retires, 1);
  ASSERT_EQ(backend->cleanupFinishes, 1);
  ASSERT_EQ(backend->released, std::vector<storage::PermitIdentity>{recoveryPermit()});
  ASSERT_EQ(cache::metrics::lastTagsForTest(cache::metrics::Event::MANAGER_LEASE_RECOVERY).reason, "recovered");
}

TEST(TestPermitRecovery, PreservesCleaningAndPermitWhenPhysicalCleanupFails) {
  cache::metrics::resetForTest();
  auto backend = std::make_shared<RecoveryBackend>();
  backend->records.push_back(phase4LoadingItem(1));
  backend->queryError = Status(CacheCode::kPermitExpired);
  backend->cleanupError = Status(RPCCode::kTimeout);
  HintCoalescer hints;
  CacheCleanupWorker cleanup(backend);
  PermitRecovery recovery(
      backend,
      hints,
      Uuid::from(9, 8),
      1_s,
      1,
      [] { return uint64_t{1000}; },
      &cleanup,
      true);
  ASSERT_ERROR(folly::coro::blockingWait(recovery.run()), RPCCode::kTimeout);
  ASSERT_EQ(backend->recovered.size(), size_t{1});
  ASSERT_TRUE(backend->released.empty());
  ASSERT_EQ(backend->cleanupFinishes, 0);
  ASSERT_EQ(cache::metrics::lastTagsForTest(cache::metrics::Event::MANAGER_LEASE_RECOVERY).reason, "failed");
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
