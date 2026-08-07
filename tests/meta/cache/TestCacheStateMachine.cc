#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <folly/experimental/coro/Sleep.h>
#include <gtest/gtest.h>
#include <string>
#include <string_view>

#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr std::string_view kServiceName = "cache-manager";
constexpr std::string_view kServiceToken = "state-machine-test-token";

class TestCacheStateMachine : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster() {
    static MockCluster::Config config = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().event_trace_log().set_enabled(false);
      value.mock_meta().set_cache_service_name(std::string{kServiceName});
      value.mock_meta().set_cache_service_token(std::string{kServiceToken});
      value.mock_meta().set_cache_load_lease(10_ms);
      value.mock_meta().set_enable_cache_phase2(true);
      value.mock_meta().set_enable_cache_phase3(true);
      value.mock_meta().set_enable_cache_phase4(true);
      return value;
    }();
    return createMockCluster(config);
  }
};

void enableCacheFeature(MockCluster &cluster) {
  auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
  auto &raw = *routing->raw();
  for (auto &[_, node] : raw.nodes) {
    if (node.type == flat::NodeType::META) {
      node.cacheSchemaVersion = cache::kCacheSchemaVersion;
      node.cacheProtocolVersion = cache::kCacheProtocolVersion;
    }
  }
  auto &table = raw.chainTables.at(flat::ChainTableId{2}).rbegin()->second;
  table.role = flat::ChainTableRole::CACHE_DATA;
  table.logicalCapacity = 1ULL << 30;
  table.checksumType = flat::ChainTableChecksumType::CRC32C;
  cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
}

CacheServiceIdentity service(std::string token = std::string{kServiceToken}) {
  return {std::string{kServiceName}, std::move(token)};
}

OriginFileMetadata metadata(std::string key, uint64_t size) {
  OriginFileMetadata result;
  result.object = {cache::OriginId{1},
                   "bucket",
                   std::move(key),
                   cache::VersionSelector{cache::VersionSelectorType::VERSION_ID, "version-1"}};
  result.objectSize = size;
  result.tableId = flat::ChainTableId{2};
  result.blockSize = 4096;
  result.stripeSize = 1;
  result.permission = Permission{0600};
  return result;
}

CoTryTask<Inode> prepare(MockCluster &cluster, std::string path, uint64_t size = 4 * 4096) {
  enableCacheFeature(cluster);
  auto txn = cluster.kvEngine()->createReadWriteTransaction();
  CO_RETURN_ON_ERROR(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1ULL << 20));
  CO_RETURN_ON_ERROR(co_await txn->commit());

  ImportOriginFileReq req;
  req.user = SUPER_USER;
  req.entry = {PathAt(std::move(path)), metadata("object", size)};
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await cluster.meta().getOperator().importOriginFile(std::move(req));
  CO_RETURN_ON_ERROR(result);
  co_return Inode(std::move(result->inode));
}

CacheBlockRequestBase block(const Inode &inode, uint32_t index) {
  auto offset = uint64_t{index} * inode.fileLayout().chunkSize;
  return {{inode.id.u64(), cache::CacheBlockIndex{index}},
          std::min(uint64_t{inode.fileLayout().chunkSize}, inode.fileLength() - offset)};
}

EnqueueCacheBlocksReq enqueueReq(const Inode &inode, std::initializer_list<uint32_t> blocks) {
  EnqueueCacheBlocksReq req;
  req.service = service();
  for (auto index : blocks) req.items.push_back(block(inode, index));
  return req;
}

AcquireCacheBlocksReq acquireReq(const Inode &inode, uint32_t index) {
  AcquireCacheBlocksReq req;
  req.service = service();
  req.items.push_back(block(inode, index));
  return req;
}

CommitCacheBlocksReq commitReq(const Inode &inode, uint32_t index, const CacheBlockLease &lease) {
  CommitCacheBlocksReq req;
  req.service = service();
  auto item = block(inode, index);
  req.items.push_back({item.key, lease.loaderId, lease.loadEpoch, lease.cacheGeneration, item.blockLength, 1, 1234});
  if (lease.permit.has_value()) {
    req.items.back().permit = lease.permit;
    req.items.back().placement = lease.permit->placement;
  }
  return req;
}

Result<storage::PermitIdentity> permitFor(MockCluster &cluster,
                                          const Inode &inode,
                                          uint32_t index,
                                          Uuid managerEpoch,
                                          Uuid attempt,
                                          uint64_t generation) {
  auto routing = cluster.mgmtdClient()->getRoutingInfo();
  if (!routing || !routing->raw()) return makeError(CacheCode::kUnavailable);
  auto offset = uint64_t{index} * inode.fileLayout().chunkSize;
  auto chainId = inode.getChainId(inode, offset, *routing->raw());
  RETURN_ON_ERROR(chainId);
  auto chain = routing->raw()->getChain(*chainId);
  if (!chain) return makeError(CacheCode::kUnavailable);
  std::vector<flat::TargetId> targets;
  storage::FootprintByTarget footprints;
  for (const auto &target : chain->targets) {
    targets.push_back(target.targetId);
    footprints.emplace(target.targetId, inode.fileLayout().chunkSize);
  }
  if (targets.empty()) return makeError(CacheCode::kUnavailable);
  auto placement =
      storage::PlacementIdentity::create({*chainId, chain->chainVersion}, targets, targets.front(), attempt);
  RETURN_ON_ERROR(placement);
  return storage::PermitIdentity{managerEpoch, *placement, generation, std::move(footprints)};
}

FailCacheBlocksReq failReq(const Inode &inode, uint32_t index, const CacheBlockLease &lease) {
  FailCacheBlocksReq req;
  req.service = service();
  req.items.push_back({block(inode, index).key, lease.loaderId, lease.loadEpoch});
  return req;
}

TEST_F(TestCacheStateMachine, EnqueuePreservesOrderCapacityAndServiceBoundary) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/enqueue");
    CO_ASSERT_OK(inode);
    auto &meta = cluster.meta().getOperator();

    auto readableIdentity = serde::toJsonString(service());
    CO_ASSERT_EQ(readableIdentity.find(kServiceToken), std::string::npos);

    auto unauthorized = enqueueReq(*inode, {0});
    unauthorized.service = service("wrong-token");
    CO_ASSERT_ERROR(co_await meta.enqueueCacheBlocks(unauthorized), MetaCode::kNoPermission);

    auto request = enqueueReq(*inode, {0});
    request.items.push_back({{inode->id.u64(), cache::CacheBlockIndex{99}}, 4096});
    request.items.push_back({{inode->id.u64(), cache::CacheBlockIndex{1}}, 0});
    auto result = co_await meta.enqueueCacheBlocks(request);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(result->results.size(), size_t{3});
    CO_ASSERT_OK(result->results[0]);
    CO_ASSERT_EQ(result->results[0]->state, cache::CacheBlockState::QUEUED);
    CO_ASSERT_ERROR(result->results[1], StatusCode::kInvalidArg);
    CO_ASSERT_ERROR(result->results[2], StatusCode::kInvalidArg);

    auto repeated = co_await meta.enqueueCacheBlocks(enqueueReq(*inode, {0}));
    CO_ASSERT_OK(repeated);
    CO_ASSERT_OK(repeated->results[0]);
    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{4096});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{4096});
  }());
}

TEST_F(TestCacheStateMachine, CacheStatusRequiresServerSideAdminAuthorization) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    CO_ASSERT_OK(co_await prepare(cluster, "/status"));
    GetCacheStatusReq req;
    req.user = SUPER_USER;
    req.cacheProtocolVersion = cache::kCacheProtocolVersion;
    CO_ASSERT_OK(co_await cluster.meta().getOperator().getCacheStatus(req));
    req.user = UserInfo{Uid{1234}, Gid{1234}, "untrusted"};
    CO_ASSERT_ERROR(co_await cluster.meta().getOperator().getCacheStatus(req), MetaCode::kNoPermission);
  }());
}

TEST_F(TestCacheStateMachine, FencesConcurrentAcquireLeaseReclaimAndCommit) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/acquire");
    CO_ASSERT_OK(inode);
    auto &meta = cluster.meta().getOperator();
    auto enqueued = co_await meta.enqueueCacheBlocks(enqueueReq(*inode, {0}));
    CO_ASSERT_OK(enqueued);
    CO_ASSERT_OK(enqueued->results[0]);

    auto [first, second] = co_await folly::coro::collectAll(meta.acquireCacheBlocks(acquireReq(*inode, 0)),
                                                            meta.acquireCacheBlocks(acquireReq(*inode, 0)));
    CO_ASSERT_OK(first);
    CO_ASSERT_OK(second);
    CO_ASSERT_NE(first->results[0].hasValue(), second->results[0].hasValue());
    const auto &winner = first->results[0].hasValue() ? first->results[0] : second->results[0];
    const auto &loser = first->results[0].hasError() ? first->results[0] : second->results[0];
    CO_ASSERT_ERROR(loser, CacheCode::kStateConflict);
    auto oldLease = winner->lease;

    co_await folly::coro::sleep(20_ms);
    auto reclaimed = co_await meta.acquireCacheBlocks(acquireReq(*inode, 0));
    CO_ASSERT_OK(reclaimed);
    CO_ASSERT_OK(reclaimed->results[0]);
    auto newLease = reclaimed->results[0]->lease;
    CO_ASSERT_EQ(newLease.loadEpoch, oldLease.loadEpoch + 1);
    CO_ASSERT_GT(newLease.cacheGeneration, oldLease.cacheGeneration);

    auto staleCommit = co_await meta.commitCacheBlocks(commitReq(*inode, 0, oldLease));
    CO_ASSERT_OK(staleCommit);
    CO_ASSERT_ERROR(staleCommit->results[0], CacheCode::kStateConflict);
    auto staleFail = co_await meta.failCacheBlocks(failReq(*inode, 0, oldLease));
    CO_ASSERT_OK(staleFail);
    CO_ASSERT_ERROR(staleFail->results[0], CacheCode::kStateConflict);

    auto wrongChecksum = commitReq(*inode, 0, newLease);
    wrongChecksum.items[0].checksumType = static_cast<uint8_t>(flat::ChainTableChecksumType::CRC32);
    auto rejectedChecksum = co_await meta.commitCacheBlocks(wrongChecksum);
    CO_ASSERT_OK(rejectedChecksum);
    CO_ASSERT_ERROR(rejectedChecksum->results[0], CacheCode::kStateConflict);

    auto committed = co_await meta.commitCacheBlocks(commitReq(*inode, 0, newLease));
    CO_ASSERT_OK(committed);
    CO_ASSERT_OK(committed->results[0]);
    CO_ASSERT_EQ(committed->results[0]->state, cache::CacheBlockState::READY);
    auto repeated = co_await meta.commitCacheBlocks(commitReq(*inode, 0, newLease));
    CO_ASSERT_OK(repeated);
    CO_ASSERT_OK(repeated->results[0]);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{4096});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{0});
    CO_ASSERT_EQ(capacity->committedBytes, uint64_t{4096});
  }());
}

TEST_F(TestCacheStateMachine, FailMovesToCleaningWithoutReleasingCharge) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/fail");
    CO_ASSERT_OK(inode);
    auto &meta = cluster.meta().getOperator();
    CO_ASSERT_OK(co_await meta.enqueueCacheBlocks(enqueueReq(*inode, {1})));
    auto acquired = co_await meta.acquireCacheBlocks(acquireReq(*inode, 1));
    CO_ASSERT_OK(acquired);
    CO_ASSERT_OK(acquired->results[0]);
    auto lease = acquired->results[0]->lease;

    auto failed = co_await meta.failCacheBlocks(failReq(*inode, 1, lease));
    CO_ASSERT_OK(failed);
    CO_ASSERT_OK(failed->results[0]);
    CO_ASSERT_EQ(failed->results[0]->state, cache::CacheBlockState::CLEANING);
    auto repeated = co_await meta.failCacheBlocks(failReq(*inode, 1, lease));
    CO_ASSERT_OK(repeated);
    CO_ASSERT_OK(repeated->results[0]);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto record = co_await CacheBlockStore::snapshotLoad(*read, block(*inode, 1).key);
    CO_ASSERT_OK(record);
    CO_ASSERT_TRUE(record->has_value());
    CO_ASSERT_EQ((*record)->state, cache::CacheBlockState::CLEANING);
    CO_ASSERT_EQ((*record)->terminalState, cache::CleanupTerminalState::FAILED);
    CO_ASSERT_EQ((*record)->deleteGeneration, lease.cacheGeneration);
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{4096});
  }());
}

TEST_F(TestCacheStateMachine, RecoversOnlyExpiredLoadingWithExactPersistedFence) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/expired-loading-recovery");
    CO_ASSERT_OK(inode);
    auto permit = permitFor(cluster, *inode, 0, Uuid::from(7, 20), Uuid::from(8, 20), 1);
    CO_ASSERT_OK(permit);
    auto enqueue = enqueueReq(*inode, {0});
    enqueue.items[0].permit = *permit;
    auto &meta = cluster.meta().getOperator();
    CO_ASSERT_OK(co_await meta.enqueueCacheBlocks(enqueue));
    auto acquired = co_await meta.acquireCacheBlocks(acquireReq(*inode, 0));
    CO_ASSERT_OK(acquired);
    CO_ASSERT_OK(acquired->results[0]);
    const auto lease = acquired->results[0]->lease;

    ListRecoverableCachePermitsReq list;
    list.service = service();
    list.limit = 1;
    list.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto page = co_await meta.listRecoverableCachePermits(list);
    CO_ASSERT_OK(page);
    CO_ASSERT_EQ(page->items.size(), size_t{1});
    const auto &observed = page->items.front();

    RecoverExpiredCacheLoadItem item;
    item.key = observed.key;
    item.loaderId = observed.loaderId;
    item.loadEpoch = observed.loadEpoch;
    item.expectedLeaseExpiresAt = observed.leaseExpiresAt;
    item.expectedGeneration = observed.cacheGeneration;
    item.expectedPermit = observed.permit;
    item.expectedPlacement = *observed.placement;
    item.terminalState = cache::CleanupTerminalState::REENQUEUE;
    RecoverExpiredCacheLoadsReq recover;
    recover.service = service();
    recover.items.push_back(item);
    recover.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;

    auto notExpired = co_await meta.recoverExpiredCacheLoads(recover);
    CO_ASSERT_OK(notExpired);
    CO_ASSERT_ERROR(notExpired->results[0], CacheCode::kStateConflict);

    auto stale = recover;
    ++stale.items[0].expectedGeneration;
    co_await folly::coro::sleep(20_ms);
    auto staleResult = co_await meta.recoverExpiredCacheLoads(stale);
    CO_ASSERT_OK(staleResult);
    CO_ASSERT_ERROR(staleResult->results[0], CacheCode::kStateConflict);

    auto recovered = co_await meta.recoverExpiredCacheLoads(recover);
    CO_ASSERT_OK(recovered);
    CO_ASSERT_OK(recovered->results[0]);
    CO_ASSERT_EQ(recovered->results[0]->state, cache::CacheBlockState::CLEANING);
    auto repeated = co_await meta.recoverExpiredCacheLoads(recover);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_OK(repeated->results[0]);

    auto oldFail = co_await meta.failCacheBlocks(failReq(*inode, 0, lease));
    CO_ASSERT_OK(oldFail);
    CO_ASSERT_ERROR(oldFail->results[0], CacheCode::kStateConflict);
    auto oldCommit = co_await meta.commitCacheBlocks(commitReq(*inode, 0, lease));
    CO_ASSERT_OK(oldCommit);
    CO_ASSERT_ERROR(oldCommit->results[0], CacheCode::kStateConflict);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto record = co_await CacheBlockStore::snapshotLoad(*read, item.key);
    CO_ASSERT_OK(record);
    CO_ASSERT_TRUE(record->has_value());
    CO_ASSERT_EQ((*record)->state, cache::CacheBlockState::CLEANING);
    CO_ASSERT_EQ((*record)->terminalState, cache::CleanupTerminalState::REENQUEUE);
    CO_ASSERT_EQ((*record)->loadEpoch, lease.loadEpoch + 1);
    CO_ASSERT_EQ((*record)->deleteGeneration, lease.cacheGeneration);
    CO_ASSERT_EQ((*record)->placement, std::optional<storage::PlacementIdentity>{lease.permit->placement});
    CO_ASSERT_FALSE((*record)->permit.has_value());
  }());
}

TEST_F(TestCacheStateMachine, PersistsPermitAndPlacementAcrossAdmissionLifecycle) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/phase2-identities");
    CO_ASSERT_OK(inode);
    auto initial = permitFor(cluster, *inode, 0, Uuid::from(7, 1), Uuid::from(8, 1), 1);
    CO_ASSERT_OK(initial);
    auto &meta = cluster.meta().getOperator();

    auto enqueue = enqueueReq(*inode, {0});
    enqueue.items[0].permit = *initial;
    auto created = co_await meta.enqueueCacheBlocks(enqueue);
    CO_ASSERT_OK(created);
    CO_ASSERT_OK(created->results[0]);
    CO_ASSERT_EQ(created->results[0]->enqueueOutcome, cache::CacheEnqueueOutcome::CREATED);
    CO_ASSERT_EQ(created->permits, std::vector<std::optional<storage::PermitIdentity>>{*initial});

    auto repeated = co_await meta.enqueueCacheBlocks(enqueue);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_OK(repeated->results[0]);
    CO_ASSERT_EQ(repeated->results[0]->enqueueOutcome, cache::CacheEnqueueOutcome::QUEUED);
    CO_ASSERT_EQ(repeated->permits, std::vector<std::optional<storage::PermitIdentity>>{*initial});

    auto acquired = co_await meta.acquireCacheBlocks(acquireReq(*inode, 0));
    CO_ASSERT_OK(acquired);
    CO_ASSERT_OK(acquired->results[0]);
    auto lease = acquired->results[0]->lease;
    CO_ASSERT_EQ(lease.permit, std::optional<storage::PermitIdentity>{*initial});

    auto observingPermit = permitFor(cluster, *inode, 0, Uuid::from(7, 9), Uuid::from(8, 9), 1);
    CO_ASSERT_OK(observingPermit);
    auto observingEnqueue = enqueueReq(*inode, {0});
    observingEnqueue.items[0].permit = *observingPermit;
    auto observed = co_await meta.enqueueCacheBlocks(observingEnqueue);
    CO_ASSERT_OK(observed);
    CO_ASSERT_OK(observed->results[0]);
    CO_ASSERT_EQ(observed->results[0]->enqueueOutcome, cache::CacheEnqueueOutcome::LOADING);
    CO_ASSERT_EQ(observed->permits, std::vector<std::optional<storage::PermitIdentity>>{*initial});

    auto observedRead = cluster.kvEngine()->createReadonlyTransaction();
    auto observedRecord = co_await CacheBlockStore::snapshotLoad(*observedRead, block(*inode, 0).key);
    CO_ASSERT_OK(observedRecord);
    CO_ASSERT_TRUE(observedRecord->has_value());
    CO_ASSERT_EQ((*observedRecord)->permit, std::optional<storage::PermitIdentity>{*initial});

    auto replacement = *initial;
    replacement.permitGeneration = 2;
    auto competingReplacement = replacement;
    competingReplacement.permitGeneration = 3;
    auto replacementReq = [&](const storage::PermitIdentity &candidate) {
      auto request = enqueueReq(*inode, {0});
      request.items[0].permit = candidate;
      request.items[0].expectedPermit = *initial;
      request.items[0].expectedState = cache::CacheBlockState::LOADING;
      request.items[0].expectedLoaderId = lease.loaderId;
      request.items[0].expectedLoadEpoch = lease.loadEpoch;
      return request;
    };
    auto [firstReplace, secondReplace] =
        co_await folly::coro::collectAll(meta.enqueueCacheBlocks(replacementReq(replacement)),
                                         meta.enqueueCacheBlocks(replacementReq(competingReplacement)));
    CO_ASSERT_OK(firstReplace);
    CO_ASSERT_OK(secondReplace);
    CO_ASSERT_NE(firstReplace->results[0].hasValue(), secondReplace->results[0].hasValue());
    const auto &replaceLoser =
        firstReplace->results[0].hasError() ? firstReplace->results[0] : secondReplace->results[0];
    CO_ASSERT_ERROR(replaceLoser, CacheCode::kStateConflict);

    auto afterReplaceRead = cluster.kvEngine()->createReadonlyTransaction();
    auto afterReplace = co_await CacheBlockStore::snapshotLoad(*afterReplaceRead, block(*inode, 0).key);
    CO_ASSERT_OK(afterReplace);
    CO_ASSERT_TRUE(afterReplace->has_value());
    CO_ASSERT_TRUE((*afterReplace)->permit.has_value());
    replacement = *(*afterReplace)->permit;

    auto wrongPermit = permitFor(cluster, *inode, 0, Uuid::from(7, 2), Uuid::from(8, 2), 2);
    CO_ASSERT_OK(wrongPermit);
    auto wrongLease = lease;
    wrongLease.permit = *wrongPermit;
    auto rejected = co_await meta.commitCacheBlocks(commitReq(*inode, 0, wrongLease));
    CO_ASSERT_OK(rejected);
    CO_ASSERT_ERROR(rejected->results[0], CacheCode::kPlacementMismatch);

    lease.permit = replacement;
    auto committed = co_await meta.commitCacheBlocks(commitReq(*inode, 0, lease));
    CO_ASSERT_OK(committed);
    CO_ASSERT_OK(committed->results[0]);
    CO_ASSERT_EQ(committed->results[0]->placement, std::optional<storage::PlacementIdentity>{replacement.placement});

    auto readyAttempt = permitFor(cluster, *inode, 0, Uuid::from(7, 3), Uuid::from(8, 3), 1);
    CO_ASSERT_OK(readyAttempt);
    auto readyEnqueue = enqueueReq(*inode, {0});
    readyEnqueue.items[0].permit = *readyAttempt;
    auto ready = co_await meta.enqueueCacheBlocks(readyEnqueue);
    CO_ASSERT_OK(ready);
    CO_ASSERT_OK(ready->results[0]);
    CO_ASSERT_EQ(ready->results[0]->enqueueOutcome, cache::CacheEnqueueOutcome::READY);
    CO_ASSERT_EQ(ready->results[0]->placement, std::optional<storage::PlacementIdentity>{replacement.placement});

    BeginCleanCacheBlocksReq clean;
    clean.service = service();
    clean.items.push_back({block(*inode, 0).key});
    auto cleaning = co_await meta.beginCleanCacheBlocks(clean);
    CO_ASSERT_OK(cleaning);
    CO_ASSERT_OK(cleaning->results[0]);
    CO_ASSERT_EQ(cleaning->results[0]->placement, std::optional<storage::PlacementIdentity>{replacement.placement});

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto record = co_await CacheBlockStore::snapshotLoad(*read, block(*inode, 0).key);
    CO_ASSERT_OK(record);
    CO_ASSERT_TRUE(record->has_value());
    CO_ASSERT_EQ((*record)->state, cache::CacheBlockState::CLEANING);
    CO_ASSERT_FALSE((*record)->permit.has_value());
    CO_ASSERT_EQ((*record)->placement, std::optional<storage::PlacementIdentity>{replacement.placement});
  }());
}

TEST_F(TestCacheStateMachine, ListsAndCancelsExactQueuedAdmission) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/queued-recovery");
    CO_ASSERT_OK(inode);
    auto permit = permitFor(cluster, *inode, 0, Uuid::from(7, 11), Uuid::from(8, 11), 1);
    CO_ASSERT_OK(permit);
    auto enqueue = enqueueReq(*inode, {0});
    enqueue.items[0].permit = *permit;
    auto &meta = cluster.meta().getOperator();
    CO_ASSERT_OK(co_await meta.enqueueCacheBlocks(enqueue));

    ListRecoverableCachePermitsReq list;
    list.service = service();
    list.limit = 1;
    list.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto page = co_await meta.listRecoverableCachePermits(list);
    CO_ASSERT_OK(page);
    CO_ASSERT_EQ(page->items.size(), size_t{1});
    CO_ASSERT_EQ(page->items[0].permit, *permit);
    CO_ASSERT_EQ(page->items[0].state, cache::CacheBlockState::QUEUED);

    auto wrong = *permit;
    ++wrong.permitGeneration;
    CancelQueuedAdmissionsReq cancel;
    cancel.service = service();
    cancel.cacheProtocolVersion = cache::kCacheProtocolVersion;
    cancel.items.push_back({block(*inode, 0).key, wrong});
    auto rejected = co_await meta.cancelQueuedAdmissions(cancel);
    CO_ASSERT_OK(rejected);
    CO_ASSERT_ERROR(rejected->results[0], CacheCode::kStateConflict);

    cancel.items[0].expectedPermit = *permit;
    auto cancelled = co_await meta.cancelQueuedAdmissions(cancel);
    CO_ASSERT_OK(cancelled);
    CO_ASSERT_OK(cancelled->results[0]);
    CO_ASSERT_EQ(cancelled->results[0]->state, cache::CacheBlockState::NONE);
    auto repeated = co_await meta.cancelQueuedAdmissions(cancel);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_OK(repeated->results[0]);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto record = co_await CacheBlockStore::snapshotLoad(*read, block(*inode, 0).key);
    CO_ASSERT_OK(record);
    CO_ASSERT_FALSE(record->has_value());
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{0});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{0});
  }());
}

TEST_F(TestCacheStateMachine, SupersededInodeRejectsEveryLoadingMutation) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster, "/refresh");
    CO_ASSERT_OK(inode);
    auto &meta = cluster.meta().getOperator();
    CO_ASSERT_OK(co_await meta.enqueueCacheBlocks(enqueueReq(*inode, {0})));
    auto acquired = co_await meta.acquireCacheBlocks(acquireReq(*inode, 0));
    CO_ASSERT_OK(acquired);
    CO_ASSERT_OK(acquired->results[0]);
    auto lease = acquired->results[0]->lease;

    RefreshOriginFileReq refresh;
    refresh.user = SUPER_USER;
    refresh.requestId = Uuid::random();
    refresh.path = PathAt("/refresh");
    refresh.expectedInode = inode->id;
    refresh.oldObject = inode->asOriginFile().object;
    refresh.newMetadata = metadata("new-object", inode->fileLength());
    refresh.cacheProtocolVersion = cache::kCacheProtocolVersion;
    CO_ASSERT_OK(co_await meta.refreshOriginFile(refresh));

    auto enqueue = co_await meta.enqueueCacheBlocks(enqueueReq(*inode, {0}));
    CO_ASSERT_OK(enqueue);
    CO_ASSERT_ERROR(enqueue->results[0], CacheCode::kStateConflict);
    auto acquire = co_await meta.acquireCacheBlocks(acquireReq(*inode, 0));
    CO_ASSERT_OK(acquire);
    CO_ASSERT_ERROR(acquire->results[0], CacheCode::kStateConflict);
    auto commit = co_await meta.commitCacheBlocks(commitReq(*inode, 0, lease));
    CO_ASSERT_OK(commit);
    CO_ASSERT_ERROR(commit->results[0], CacheCode::kStateConflict);
    auto fail = co_await meta.failCacheBlocks(failReq(*inode, 0, lease));
    CO_ASSERT_OK(fail);
    CO_ASSERT_ERROR(fail->results[0], CacheCode::kStateConflict);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
