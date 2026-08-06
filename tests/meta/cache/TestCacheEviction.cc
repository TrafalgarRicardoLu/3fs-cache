#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <gtest/gtest.h>
#include <limits>

#include "common/serde/Serde.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "meta/store/cache/PinStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr std::string_view kServiceName = "cache-manager";
constexpr std::string_view kServiceToken = "cache-eviction-test-token";
constexpr uint64_t kBlockSize = 4096;

class TestCacheEviction : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster() {
    static MockCluster::Config config = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().event_trace_log().set_enabled(false);
      value.mock_meta().set_cache_service_name(std::string{kServiceName});
      value.mock_meta().set_cache_service_token(std::string{kServiceToken});
      value.mock_meta().set_enable_cache_phase2(true);
      return value;
    }();
    return createMockCluster(config);
  }
};

CacheServiceIdentity service() { return {std::string{kServiceName}, std::string{kServiceToken}}; }

void enableCacheFeature(MockCluster &cluster) {
  auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
  for (auto &[_, node] : routing->raw()->nodes) {
    if (node.type == flat::NodeType::META) {
      node.cacheSchemaVersion = cache::kCacheSchemaVersion;
      node.cacheProtocolVersion = cache::kCacheProtocolVersion;
    }
  }
  auto &table = routing->raw()->chainTables.at(flat::ChainTableId{2}).rbegin()->second;
  table.role = flat::ChainTableRole::CACHE_DATA;
  table.logicalCapacity = 1ULL << 30;
  table.checksumType = flat::ChainTableChecksumType::CRC32C;
  cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
}

CoTryTask<Inode> prepare(MockCluster &cluster, std::string path, uint64_t blocks) {
  enableCacheFeature(cluster);
  auto txn = cluster.kvEngine()->createReadWriteTransaction();
  CO_RETURN_ON_ERROR(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1ULL << 30));
  CO_RETURN_ON_ERROR(co_await txn->commit());
  OriginFileMetadata metadata;
  metadata.object = {cache::OriginId{1},
                     "bucket",
                     "eviction-object",
                     cache::VersionSelector{cache::VersionSelectorType::VERSION_ID, "version-1"}};
  metadata.objectSize = blocks * kBlockSize;
  metadata.tableId = flat::ChainTableId{2};
  metadata.blockSize = kBlockSize;
  metadata.stripeSize = 1;
  metadata.permission = Permission{0600};
  ImportOriginFileReq request;
  request.user = SUPER_USER;
  request.entry = {PathAt(std::move(path)), std::move(metadata)};
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto imported = co_await cluster.meta().getOperator().importOriginFile(std::move(request));
  CO_RETURN_ON_ERROR(imported);
  co_return Inode(std::move(imported->inode));
}

Result<storage::PermitIdentity> permitFor(MockCluster &cluster,
                                          const Inode &inode,
                                          uint32_t block,
                                          uint64_t generation) {
  auto routing = cluster.mgmtdClient()->getRoutingInfo();
  if (!routing || !routing->raw()) return makeError(CacheCode::kUnavailable);
  auto chainId = inode.getChainId(inode, uint64_t{block} * kBlockSize, *routing->raw());
  RETURN_ON_ERROR(chainId);
  auto chain = routing->raw()->getChain(*chainId);
  if (!chain) return makeError(CacheCode::kUnavailable);
  std::vector<flat::TargetId> targets;
  storage::FootprintByTarget footprints;
  for (const auto &target : chain->targets) {
    targets.push_back(target.targetId);
    footprints.emplace(target.targetId, kBlockSize);
  }
  if (targets.empty()) return makeError(CacheCode::kUnavailable);
  std::sort(targets.begin(), targets.end());
  auto placement = storage::PlacementIdentity::create({*chainId, chain->chainVersion},
                                                      targets,
                                                      targets.front(),
                                                      Uuid::from(8, block));
  RETURN_ON_ERROR(placement);
  return storage::PermitIdentity{Uuid::from(7, 1), *placement, generation, std::move(footprints)};
}

CoTryTask<CacheBlockRecord> seedReady(MockCluster &cluster,
                                      const Inode &inode,
                                      uint32_t block,
                                      uint64_t generation = 1) {
  auto permit = permitFor(cluster, inode, block, generation);
  CO_RETURN_ON_ERROR(permit);
  CacheBlockRecord record;
  record.key = {inode.id.u64(), cache::CacheBlockIndex{block}};
  record.state = cache::CacheBlockState::READY;
  record.chainId = permit->placement.versionedChain.chainId;
  record.blockLength = kBlockSize;
  record.loadEpoch = generation;
  record.cacheGeneration = cache::CacheGeneration{generation};
  record.ready = cache::ReadyIdentity{generation, record.cacheGeneration, 1, 1234, kBlockSize};
  record.chargeKind = cache::ChargeKind::COMMITTED;
  record.chargedBytes = kBlockSize;
  record.placement = permit->placement;
  record.committedPermit = *permit;
  record.readyAt = UtcTime::fromMicroseconds(100);
  record.lastAccessAt = record.readyAt;
  auto txn = cluster.kvEngine()->createReadWriteTransaction();
  CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(*txn, record));
  CO_RETURN_ON_ERROR(co_await txn->commit());
  co_return record;
}

BeginEvictCacheBlocksReq evictReq(const CacheBlockRecord &ready, cache::EvictionReason reason) {
  BeginEvictCacheBlocksReq request;
  request.service = service();
  request.items.push_back({ready.key, *ready.ready, reason});
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return request;
}

CoTryTask<SessionInfo> openOrigin(MockCluster &cluster, std::string path) {
  auto session = MetaTestHelper::randomSession();
  OpenReq request(SUPER_USER, PathAt(std::move(path)), session, O_RDONLY);
  request.client = session.client;
  auto opened = co_await cluster.meta().getOperator().open(std::move(request));
  CO_RETURN_ON_ERROR(opened);
  co_return session;
}

TEST_F(TestCacheEviction, PersistsStableIdentityAndReadPlanFallsBack) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/evict-ready", 1);
    CO_ASSERT_OK(inode);
    if (!inode) co_return;
    auto ready = co_await seedReady(cluster, *inode, 0);
    CO_ASSERT_OK(ready);
    if (!ready) co_return;

    auto &meta = cluster.meta().getOperator();
    auto first = co_await meta.beginEvictCacheBlocks(evictReq(*ready, cache::EvictionReason::CAPACITY_WATERMARK));
    CO_ASSERT_OK(first);
    if (!first) co_return;
    CO_ASSERT_OK(first->results.front());
    if (!first->results.front()) co_return;
    auto identity = *first->results.front();
    CO_ASSERT_EQ(identity.evictionEpoch, cache::EvictionEpoch{1});
    CO_ASSERT_NE(identity.retireOperationId, Uuid::zero());
    CO_ASSERT_EQ(identity.placement, *ready->placement);
    CO_ASSERT_EQ(identity.reason, cache::EvictionReason::CAPACITY_WATERMARK);

    auto replay = co_await meta.beginEvictCacheBlocks(evictReq(*ready, cache::EvictionReason::LOCAL_SAFETY));
    CO_ASSERT_OK(replay);
    if (!replay) co_return;
    CO_ASSERT_OK(replay->results.front());
    if (!replay->results.front()) co_return;
    CO_ASSERT_EQ(*replay->results.front(), identity);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto stored = co_await CacheBlockStore::snapshotLoad(*read, ready->key);
    CO_ASSERT_OK(stored);
    CO_ASSERT_TRUE(stored->has_value());
    CO_ASSERT_EQ((*stored)->state, cache::CacheBlockState::EVICTING);
    CO_ASSERT_EQ((*stored)->chargeKind, cache::ChargeKind::COMMITTED);
    CO_ASSERT_EQ((*stored)->chargedBytes, kBlockSize);
    CO_ASSERT_EQ((*stored)->ready, ready->ready);
    CO_ASSERT_EQ((*stored)->placement, ready->placement);

    auto session = co_await openOrigin(cluster, "/evict-ready");
    CO_ASSERT_OK(session);
    if (!session) co_return;
    GetFileReadPlanReq planRequest;
    planRequest.user = SUPER_USER;
    planRequest.client = session->client;
    planRequest.openSessionId = session->session;
    planRequest.inode = inode->id;
    planRequest.offset = 0;
    planRequest.length = kBlockSize;
    planRequest.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto plan = co_await meta.getFileReadPlan(planRequest);
    CO_ASSERT_OK(plan);
    if (!plan) co_return;
    CO_ASSERT_EQ(plan->blocks.front().state, cache::CacheBlockState::EVICTING);
    CO_ASSERT_FALSE(plan->blocks.front().ready.has_value());
  }());
}

TEST_F(TestCacheEviction, ActivePinFencesBeginEvictInTheMutationTransaction) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/evict-pinned", 11);
    CO_ASSERT_OK(inode);
    if (!inode) co_return;
    auto ready = co_await seedReady(cluster, *inode, 0);
    CO_ASSERT_OK(ready);
    if (!ready) co_return;

    cache::PinRecord pin{ready->key,
                         {cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerId{Uuid::from(9, 1)}},
                         1,
                         std::numeric_limits<uint64_t>::max(),
                         ready->cacheGeneration};
    auto txn = cluster.kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await PinStore::upsert(*txn, pin));
    CO_ASSERT_OK(co_await txn->commit());

    auto evicted = co_await cluster.meta().getOperator().beginEvictCacheBlocks(
        evictReq(*ready, cache::EvictionReason::CAPACITY_WATERMARK));
    CO_ASSERT_OK(evicted);
    CO_ASSERT_EQ(evicted->results.size(), size_t{1});
    CO_ASSERT_ERROR(evicted->results.front(), CacheCode::kStateConflict);
    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto stored = co_await CacheBlockStore::snapshotLoad(*read, ready->key);
    CO_ASSERT_OK(stored);
    CO_ASSERT_TRUE(stored->has_value());
    CO_ASSERT_EQ((*stored)->state, cache::CacheBlockState::READY);
  }());
}

TEST_F(TestCacheEviction, CasRaceAndCleaningAreMutuallyExclusive) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/evict-race", 2);
    CO_ASSERT_OK(inode);
    if (!inode) co_return;
    auto ready = co_await seedReady(cluster, *inode, 0);
    CO_ASSERT_OK(ready);
    if (!ready) co_return;
    auto &meta = cluster.meta().getOperator();
    auto request = evictReq(*ready, cache::EvictionReason::CAPACITY_WATERMARK);
    auto [left, right] =
        co_await folly::coro::collectAll(meta.beginEvictCacheBlocks(request), meta.beginEvictCacheBlocks(request));
    CO_ASSERT_OK(left);
    CO_ASSERT_OK(right);
    if (!left || !right) co_return;
    CO_ASSERT_OK(left->results.front());
    CO_ASSERT_OK(right->results.front());
    if (!left->results.front() || !right->results.front()) co_return;
    CO_ASSERT_EQ(*left->results.front(), *right->results.front());

    BeginCleanCacheBlocksReq clean;
    clean.service = service();
    clean.items.push_back({ready->key, ready->ready});
    auto cleanAfterEvict = co_await meta.beginCleanCacheBlocks(clean);
    CO_ASSERT_OK(cleanAfterEvict);
    if (!cleanAfterEvict) co_return;
    CO_ASSERT_ERROR(cleanAfterEvict->results.front(), CacheCode::kStateConflict);

    auto second = co_await seedReady(cluster, *inode, 1, 2);
    CO_ASSERT_OK(second);
    if (!second) co_return;
    BeginCleanCacheBlocksReq cleanSecond;
    cleanSecond.service = service();
    cleanSecond.items.push_back({second->key, second->ready});
    auto cleaned = co_await meta.beginCleanCacheBlocks(cleanSecond);
    CO_ASSERT_OK(cleaned);
    if (!cleaned) co_return;
    CO_ASSERT_OK(cleaned->results.front());
    if (!cleaned->results.front()) co_return;
    auto evictAfterClean =
        co_await meta.beginEvictCacheBlocks(evictReq(*second, cache::EvictionReason::CAPACITY_WATERMARK));
    CO_ASSERT_OK(evictAfterClean);
    if (!evictAfterClean) co_return;
    CO_ASSERT_ERROR(evictAfterClean->results.front(), CacheCode::kStateConflict);
  }());
}

TEST_F(TestCacheEviction, RejectsOldReadyAndEpochOverflow) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/evict-overflow", 1);
    CO_ASSERT_OK(inode);
    if (!inode) co_return;
    auto ready = co_await seedReady(cluster, *inode, 0);
    CO_ASSERT_OK(ready);
    if (!ready) co_return;
    auto stale = *ready;
    stale.ready->cacheGeneration = cache::CacheGeneration{9};
    auto &meta = cluster.meta().getOperator();
    auto staleResult = co_await meta.beginEvictCacheBlocks(evictReq(stale, cache::EvictionReason::CAPACITY_WATERMARK));
    CO_ASSERT_OK(staleResult);
    if (!staleResult) co_return;
    CO_ASSERT_ERROR(staleResult->results.front(), CacheCode::kStateConflict);

    auto txn = cluster.kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await txn->set(CacheBlockStore::evictionEpochKey(ready->key),
                                   serde::serialize(cache::EvictionEpoch{std::numeric_limits<uint64_t>::max()})));
    CO_ASSERT_OK(co_await txn->commit());
    auto overflow = co_await meta.beginEvictCacheBlocks(evictReq(*ready, cache::EvictionReason::CAPACITY_WATERMARK));
    CO_ASSERT_OK(overflow);
    if (!overflow) co_return;
    CO_ASSERT_ERROR(overflow->results.front(), CacheCode::kStateConflict);
  }());
}

TEST_F(TestCacheEviction, ListsStablePages) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/evict-list", 3);
    CO_ASSERT_OK(inode);
    if (!inode) co_return;
    auto &meta = cluster.meta().getOperator();
    for (uint32_t block = 0; block < 3; ++block) {
      auto ready = co_await seedReady(cluster, *inode, block, block + 1);
      CO_ASSERT_OK(ready);
      if (!ready) co_return;
      auto evicted = co_await meta.beginEvictCacheBlocks(evictReq(*ready, cache::EvictionReason::CAPACITY_WATERMARK));
      CO_ASSERT_OK(evicted);
      if (!evicted) co_return;
      CO_ASSERT_OK(evicted->results.front());
      if (!evicted->results.front()) co_return;
    }

    ListEvictingCacheBlocksReq firstRequest;
    firstRequest.limit = 2;
    firstRequest.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto first = co_await meta.listEvictingCacheBlocks(firstRequest);
    CO_ASSERT_OK(first);
    if (!first) co_return;
    CO_ASSERT_EQ(first->items.size(), size_t{2});
    CO_ASSERT_TRUE(first->more);
    CO_ASSERT_EQ(first->items[0].key.block, cache::CacheBlockIndex{0});
    CO_ASSERT_EQ(first->items[1].key.block, cache::CacheBlockIndex{1});

    ListEvictingCacheBlocksReq secondRequest;
    secondRequest.beginInode = inode->id.u64();
    secondRequest.beginBlock = cache::CacheBlockIndex{2};
    secondRequest.limit = 2;
    secondRequest.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto second = co_await meta.listEvictingCacheBlocks(secondRequest);
    CO_ASSERT_OK(second);
    if (!second) co_return;
    CO_ASSERT_EQ(second->items.size(), size_t{1});
    CO_ASSERT_FALSE(second->more);
    CO_ASSERT_EQ(second->items[0].key.block, cache::CacheBlockIndex{2});
  }());
}

TEST_F(TestCacheEviction, ListsReadyPhysicalIdentitiesForManager) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/ready-list", 3);
    CO_ASSERT_OK(inode);
    if (!inode) co_return;
    auto firstReady = co_await seedReady(cluster, *inode, 0, 1);
    auto secondReady = co_await seedReady(cluster, *inode, 1, 2);
    auto evicting = co_await seedReady(cluster, *inode, 2, 3);
    CO_ASSERT_OK(firstReady);
    CO_ASSERT_OK(secondReady);
    CO_ASSERT_OK(evicting);
    if (!firstReady || !secondReady || !evicting) co_return;
    auto &meta = cluster.meta().getOperator();
    auto evicted = co_await meta.beginEvictCacheBlocks(evictReq(*evicting, cache::EvictionReason::CAPACITY_WATERMARK));
    CO_ASSERT_OK(evicted);
    if (!evicted) co_return;
    CO_ASSERT_OK(evicted->results.front());
    if (!evicted->results.front()) co_return;

    ListReadyCacheBlocksReq request;
    request.service = service();
    request.limit = 1;
    request.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto first = co_await meta.listReadyCacheBlocks(request);
    CO_ASSERT_OK(first);
    if (!first) co_return;
    CO_ASSERT_EQ(first->items.size(), size_t{1});
    CO_ASSERT_TRUE(first->more);
    CO_ASSERT_EQ(first->items.front().key, firstReady->key);
    CO_ASSERT_EQ(first->items.front().placement, *firstReady->placement);
    CO_ASSERT_EQ(first->items.front().committedPermit, *firstReady->committedPermit);

    request.after = first->items.front().key;
    auto second = co_await meta.listReadyCacheBlocks(request);
    CO_ASSERT_OK(second);
    if (!second) co_return;
    CO_ASSERT_EQ(second->items.size(), size_t{1});
    CO_ASSERT_FALSE(second->more);
    CO_ASSERT_EQ(second->items.front().key, secondReady->key);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
