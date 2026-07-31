#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr std::string_view kServiceName = "cache-manager";
constexpr std::string_view kServiceToken = "cache-access-test-token";

class TestCacheAccess : public MetaTestBase<kv::mem::MemKV> {
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

CoTryTask<Inode> prepare(MockCluster &cluster) {
  enableCacheFeature(cluster);
  auto txn = cluster.kvEngine()->createReadWriteTransaction();
  CO_RETURN_ON_ERROR(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1ULL << 30));
  CO_RETURN_ON_ERROR(co_await txn->commit());

  OriginFileMetadata metadata;
  metadata.object = {cache::OriginId{1},
                     "bucket",
                     "access-object",
                     cache::VersionSelector{cache::VersionSelectorType::VERSION_ID, "version-1"}};
  metadata.objectSize = 4096;
  metadata.tableId = flat::ChainTableId{2};
  metadata.blockSize = 4096;
  metadata.stripeSize = 1;
  metadata.permission = Permission{0600};
  ImportOriginFileReq request;
  request.user = SUPER_USER;
  request.entry = {PathAt("/cache-access"), std::move(metadata)};
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto imported = co_await cluster.meta().getOperator().importOriginFile(std::move(request));
  CO_RETURN_ON_ERROR(imported);
  co_return Inode(std::move(imported->inode));
}

CacheBlockRequestBase block(const Inode &inode) {
  return {{inode.id.u64(), cache::CacheBlockIndex{0}}, inode.fileLength()};
}

CoTryTask<CacheBlockRecord> makeReady(MockCluster &cluster, const Inode &inode) {
  auto &meta = cluster.meta().getOperator();
  EnqueueCacheBlocksReq enqueue;
  enqueue.service = service();
  enqueue.items.push_back(block(inode));
  auto enqueued = co_await meta.enqueueCacheBlocks(enqueue);
  CO_RETURN_ON_ERROR(enqueued);
  CO_RETURN_ON_ERROR(enqueued->results.front());

  AcquireCacheBlocksReq acquire;
  acquire.service = service();
  acquire.items.push_back(block(inode));
  auto acquired = co_await meta.acquireCacheBlocks(acquire);
  CO_RETURN_ON_ERROR(acquired);
  CO_RETURN_ON_ERROR(acquired->results.front());
  const auto &lease = acquired->results.front()->lease;

  CommitCacheBlocksReq commit;
  commit.service = service();
  commit.items.push_back(
      {block(inode).key, lease.loaderId, lease.loadEpoch, lease.cacheGeneration, block(inode).blockLength, 1, 1234});
  auto committed = co_await meta.commitCacheBlocks(commit);
  CO_RETURN_ON_ERROR(committed);
  CO_RETURN_ON_ERROR(committed->results.front());

  auto read = cluster.kvEngine()->createReadonlyTransaction();
  auto ready = co_await CacheBlockStore::snapshotLoad(*read, block(inode).key);
  CO_RETURN_ON_ERROR(ready);
  if (!ready->has_value()) co_return makeError(CacheCode::kNotFound);
  co_return **ready;
}

TEST_F(TestCacheAccess, CommitInitializesAndListsReadyRecency) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster);
    CO_ASSERT_OK(inode);
    auto ready = co_await makeReady(cluster, *inode);
    CO_ASSERT_OK(ready);
    CO_ASSERT_FALSE(ready->readyAt.isZero());
    CO_ASSERT_EQ(ready->lastAccessAt, ready->readyAt);

    ListCacheBlocksReq list;
    list.user = SUPER_USER;
    list.inode = inode->id;
    list.limit = 1;
    list.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto listed = co_await cluster.meta().getOperator().listCacheBlocks(list);
    CO_ASSERT_OK(listed);
    CO_ASSERT_EQ(listed->blocks.size(), size_t{1});
    CO_ASSERT_EQ(listed->blocks.front().readyAt, ready->readyAt);
    CO_ASSERT_EQ(listed->blocks.front().lastAccessAt, ready->lastAccessAt);
  }());
}

TEST_F(TestCacheAccess, AppliesMonotonicGenerationFencedPartialBatch) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepare(cluster);
    CO_ASSERT_OK(inode);
    auto ready = co_await makeReady(cluster, *inode);
    CO_ASSERT_OK(ready);
    const auto newerNs = static_cast<uint64_t>(ready->lastAccessAt.toMicroseconds() + 100) * 1000;
    const auto olderNs = static_cast<uint64_t>(ready->lastAccessAt.toMicroseconds() - 1) * 1000;

    UpdateCacheBlockAccessReq update;
    update.service = service();
    update.cacheProtocolVersion = cache::kCacheProtocolVersion;
    update.items.push_back({ready->key, ready->cacheGeneration, newerNs});
    update.items.push_back({ready->key, ready->cacheGeneration, olderNs});
    update.items.push_back(
        {ready->key, cache::CacheGeneration{ready->cacheGeneration.toUnderType() + 1}, newerNs + 1000});
    update.items.push_back({ready->key, cache::CacheGeneration{}, newerNs});
    update.items.push_back({{ready->key.inode, cache::CacheBlockIndex{1}}, ready->cacheGeneration, newerNs});
    auto updated = co_await cluster.meta().getOperator().updateCacheBlockAccess(update);
    CO_ASSERT_OK(updated);
    CO_ASSERT_EQ(updated->results.size(), size_t{5});
    CO_ASSERT_TRUE(updated->results[0].hasValue() && updated->results[0]->updated);
    CO_ASSERT_TRUE(updated->results[1].hasValue() && !updated->results[1]->updated);
    CO_ASSERT_TRUE(updated->results[2].hasValue() && !updated->results[2]->updated);
    CO_ASSERT_ERROR(updated->results[3], StatusCode::kInvalidArg);
    CO_ASSERT_TRUE(updated->results[4].hasValue() && !updated->results[4]->updated);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto persisted = co_await CacheBlockStore::snapshotLoad(*read, ready->key);
    CO_ASSERT_OK(persisted);
    CO_ASSERT_TRUE(persisted->has_value());
    CO_ASSERT_EQ((*persisted)->lastAccessAt.toMicroseconds(), static_cast<int64_t>(newerNs / 1000));

    auto write = cluster.kvEngine()->createReadWriteTransaction();
    auto cleaning = co_await CacheBlockStore::load(*write, ready->key);
    CO_ASSERT_OK(cleaning);
    CO_ASSERT_TRUE(cleaning->has_value());
    (*cleaning)->state = cache::CacheBlockState::CLEANING;
    CO_ASSERT_OK(co_await CacheBlockStore::store(*write, **cleaning));
    CO_ASSERT_OK(co_await write->commit());

    update.items = {{ready->key, ready->cacheGeneration, newerNs + 1000}};
    auto raced = co_await cluster.meta().getOperator().updateCacheBlockAccess(update);
    CO_ASSERT_OK(raced);
    CO_ASSERT_TRUE(raced->results[0].hasValue() && !raced->results[0]->updated);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
