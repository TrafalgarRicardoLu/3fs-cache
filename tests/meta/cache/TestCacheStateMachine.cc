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
  return req;
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
