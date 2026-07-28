#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <gtest/gtest.h>

#include "core/user/UserStore.h"
#include "meta/components/OriginNamespaceManager.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

class TestOriginNamespace : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster() {
    static MockCluster::Config config = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().event_trace_log().set_enabled(false);
      return value;
    }();
    return createMockCluster(config);
  }
};

cache::ImmutableObjectIdentity object(std::string bucket, std::string key, std::string version = "version-1") {
  return cache::ImmutableObjectIdentity{
      cache::OriginId{1},
      std::move(bucket),
      std::move(key),
      cache::VersionSelector{cache::VersionSelectorType::VERSION_ID, std::move(version)}};
}

OriginFileMetadata metadata(cache::ImmutableObjectIdentity identity,
                            uint64_t size = 8192,
                            flat::ChainTableId tableId = flat::ChainTableId{2}) {
  OriginFileMetadata result;
  result.object = std::move(identity);
  result.objectSize = size;
  result.tableId = tableId;
  result.blockSize = 4096;
  result.stripeSize = 1;
  result.permission = p644;
  return result;
}

ImportOriginFileReq importReq(const UserInfo &user, std::string path, OriginFileMetadata value) {
  ImportOriginFileReq req;
  req.user = user;
  req.entry.path = PathAt(std::move(path));
  req.entry.metadata = std::move(value);
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return req;
}

RefreshOriginFileReq refreshReq(std::string path,
                                InodeId expected,
                                cache::ImmutableObjectIdentity oldObject,
                                OriginFileMetadata value,
                                Uuid requestId = Uuid::random()) {
  RefreshOriginFileReq req;
  req.user = SUPER_USER;
  req.requestId = requestId;
  req.path = PathAt(std::move(path));
  req.expectedInode = expected;
  req.oldObject = std::move(oldObject);
  req.newMetadata = std::move(value);
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return req;
}

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

TEST_F(TestOriginNamespace, ImportIsGatedAuthorizedAndIdempotent) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = this->createCluster();
    auto &meta = cluster.meta().getOperator();

    auto firstObject = object("bucket", "key");
    auto req = importReq(SUPER_USER, "/origin", metadata(firstObject));
    CO_ASSERT_ERROR(co_await meta.importOriginFile(req), CacheCode::kFeatureDisabled);

    enableCacheFeature(cluster);
    auto imported = co_await meta.importOriginFile(req);
    CO_ASSERT_OK(imported);
    CO_ASSERT_EQ(imported->outcome, ImportOriginFileOutcome::CREATED);
    CO_ASSERT_TRUE(imported->inode.isOriginFile());
    CO_ASSERT_EQ(imported->inode.asOriginFile().object, firstObject);
    CO_ASSERT_EQ(imported->inode.fileLayout().tableId, flat::ChainTableId{2});

    auto repeated = co_await meta.importOriginFile(req);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_EQ(repeated->outcome, ImportOriginFileOutcome::ALREADY_EXISTS);
    CO_ASSERT_EQ(repeated->inode.id, imported->inode.id);

    auto sameVersionDifferentKey = importReq(SUPER_USER, "/origin", metadata(object("bucket", "other-key")));
    CO_ASSERT_ERROR(co_await meta.importOriginFile(sameVersionDifferentKey), MetaCode::kExists);

    auto wrongProtocol = importReq(SUPER_USER, "/wrong-protocol", metadata(object("bucket", "protocol")));
    wrongProtocol.cacheProtocolVersion = 0;
    CO_ASSERT_ERROR(co_await meta.importOriginFile(wrongProtocol), CacheCode::kUpgradeRequired);

    auto wrongTable =
        importReq(SUPER_USER, "/wrong-table", metadata(object("bucket", "user-data"), 8192, flat::ChainTableId{1}));
    CO_ASSERT_ERROR(co_await meta.importOriginFile(wrongTable), MetaCode::kInvalidFileLayout);

    flat::UserInfo unauthorized(Uid{2}, Gid{2}, String{});
    CO_ASSERT_ERROR(co_await meta.importOriginFile(
                        importReq(unauthorized, "/unauthorized", metadata(object("bucket", "unauthorized")))),
                    MetaCode::kNoPermission);

    CO_ASSERT_OK(co_await meta.mkdirs({SUPER_USER, "/admin", p777, false}));
    core::UserStore users;
    auto txn = cluster.kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await users.addUser(*txn, Uid{3}, "cache-admin", {}, false, true, {}));
    CO_ASSERT_OK(co_await txn->commit());
    flat::UserInfo admin(Uid{3}, Gid{3}, String{});
    auto adminImport =
        co_await meta.importOriginFile(importReq(admin, "/admin/origin", metadata(object("bucket", "admin-object"))));
    CO_ASSERT_OK(adminImport);
  }());
}

TEST_F(TestOriginNamespace, BatchImportPreservesOrderAndIsolatesFailures) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = this->createCluster();
    enableCacheFeature(cluster);
    auto &meta = cluster.meta().getOperator();
    CO_ASSERT_OK(co_await meta.create(CreateReq(SUPER_USER, PathAt("/regular"), std::nullopt, OpenFlags{}, p644)));

    BatchImportOriginFilesReq req;
    req.user = SUPER_USER;
    req.cacheProtocolVersion = cache::kCacheProtocolVersion;
    req.entries = {
        {PathAt("/first"), metadata(object("bucket", "first"))},
        {PathAt("/regular"), metadata(object("bucket", "existing"))},
        {PathAt("/duplicate"), metadata(object("bucket", "duplicate-a"))},
        {PathAt("/duplicate"), metadata(object("bucket", "duplicate-b"))},
        {PathAt("/wrong-table"), metadata(object("bucket", "wrong"), 8192, flat::ChainTableId{1})},
        {PathAt("/last"), metadata(object("other-bucket", "last"))},
    };

    auto result = co_await meta.batchImportOriginFiles(req);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(result->results.size(), req.entries.size());
    CO_ASSERT_OK(result->results[0]);
    CO_ASSERT_ERROR(result->results[1], MetaCode::kExists);
    CO_ASSERT_ERROR(result->results[2], StatusCode::kInvalidArg);
    CO_ASSERT_ERROR(result->results[3], StatusCode::kInvalidArg);
    CO_ASSERT_ERROR(result->results[4], MetaCode::kInvalidFileLayout);
    CO_ASSERT_OK(result->results[5]);
  }());
}

TEST_F(TestOriginNamespace, RefreshIsAtomicIdempotentAndPersistsCleanupCursor) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = this->createCluster();
    enableCacheFeature(cluster);
    auto &meta = cluster.meta().getOperator();
    constexpr uint64_t blocks = 1001;
    auto oldObject = object("bucket-a", "key", "shared-version");
    auto imported =
        co_await meta.importOriginFile(importReq(SUPER_USER, "/origin", metadata(oldObject, blocks * uint64_t{4096})));
    CO_ASSERT_OK(imported);

    auto requestId = Uuid::random();
    auto newObject = object("bucket-b", "key", "shared-version");
    auto req = refreshReq("/origin", imported->inode.id, oldObject, metadata(newObject, 12345), requestId);
    auto refreshed = co_await meta.refreshOriginFile(req);
    CO_ASSERT_OK(refreshed);
    CO_ASSERT_NE(refreshed->newInode.id, imported->inode.id);
    CO_ASSERT_EQ(refreshed->newInode.asOriginFile().object, newObject);

    auto repeated = co_await meta.refreshOriginFile(req);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_EQ(repeated->newInode.id, refreshed->newInode.id);
    CO_ASSERT_EQ(repeated->cleanupJobId, refreshed->cleanupJobId);

    auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
    for (auto &[_, node] : routing->raw()->nodes) {
      if (node.type == flat::NodeType::META) node.cacheSchemaVersion = 0;
    }
    cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
    auto recoveredWithDisabledGate = co_await meta.refreshOriginFile(req);
    CO_ASSERT_OK(recoveredWithDisabledGate);
    CO_ASSERT_EQ(recoveredWithDisabledGate->cleanupJobId, refreshed->cleanupJobId);
    enableCacheFeature(cluster);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto oldInode = co_await Inode::snapshotLoad(*read, imported->inode.id);
    CO_ASSERT_OK(oldInode);
    CO_ASSERT_TRUE(oldInode->has_value());
    CO_ASSERT_TRUE((*oldInode)->asOriginFile().superseded);
    CO_ASSERT_TRUE((*oldInode)->asOriginFile().cacheAdmissionDisabled);
    CO_ASSERT_EQ((*oldInode)->asOriginFile().cleanupJobId, refreshed->cleanupJobId);
    CO_ASSERT_EQ((*oldInode)->nlink, uint16_t{0});
    auto cleanup = co_await OriginNamespaceManager::snapshotLoadCleanup(*read, refreshed->cleanupJobId);
    CO_ASSERT_OK(cleanup);
    CO_ASSERT_TRUE(cleanup->has_value());
    CO_ASSERT_EQ((*cleanup)->endBlock, blocks);
    CO_ASSERT_EQ((*cleanup)->cursor, uint64_t{0});
    CO_ASSERT_FALSE((*cleanup)->complete());

    auto update = cluster.kvEngine()->createReadWriteTransaction();
    auto mutableCleanup = co_await OriginNamespaceManager::loadCleanup(*update, refreshed->cleanupJobId);
    CO_ASSERT_OK(mutableCleanup);
    CO_ASSERT_TRUE(mutableCleanup->has_value());
    (**mutableCleanup).state = OriginCleanupJobState::RUNNING;
    (**mutableCleanup).cursor = kMaxCacheBatchItems;
    (**mutableCleanup).lastBatchBegin = 0;
    (**mutableCleanup).lastBatchEnd = kMaxCacheBatchItems;
    (**mutableCleanup).lastBatchSucceeded = kMaxCacheBatchItems;
    CO_ASSERT_OK(co_await OriginNamespaceManager::storeCleanup(*update, **mutableCleanup));
    CO_ASSERT_OK(co_await update->commit());

    read = cluster.kvEngine()->createReadonlyTransaction();
    cleanup = co_await OriginNamespaceManager::snapshotLoadCleanup(*read, refreshed->cleanupJobId);
    CO_ASSERT_OK(cleanup);
    CO_ASSERT_EQ((*cleanup)->cursor, uint64_t{kMaxCacheBatchItems});
    CO_ASSERT_EQ((*cleanup)->lastBatchSucceeded, uint32_t{kMaxCacheBatchItems});

    auto stale = refreshReq("/origin", imported->inode.id, oldObject, metadata(object("bucket-c", "key", "version-3")));
    CO_ASSERT_ERROR(co_await meta.refreshOriginFile(stale), CacheCode::kStateConflict);

    auto reused = req;
    reused.newMetadata.object = object("bucket-d", "key", "version-4");
    CO_ASSERT_ERROR(co_await meta.refreshOriginFile(reused), CacheCode::kStateConflict);
  }());
}

TEST_F(TestOriginNamespace, ConcurrentRefreshAllowsOnlyOneReplacement) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = this->createCluster();
    enableCacheFeature(cluster);
    auto &meta = cluster.meta().getOperator();
    auto oldObject = object("bucket", "concurrent", "version-1");
    auto imported = co_await meta.importOriginFile(importReq(SUPER_USER, "/concurrent", metadata(oldObject, 4096)));
    CO_ASSERT_OK(imported);

    auto firstReq =
        refreshReq("/concurrent", imported->inode.id, oldObject, metadata(object("bucket", "concurrent", "version-2")));
    auto secondReq =
        refreshReq("/concurrent", imported->inode.id, oldObject, metadata(object("bucket", "concurrent", "version-3")));
    auto [first, second] =
        co_await folly::coro::collectAll(meta.refreshOriginFile(firstReq), meta.refreshOriginFile(secondReq));
    CO_ASSERT_NE(first.hasValue(), second.hasValue());
    const auto &failed = first.hasError() ? first : second;
    CO_ASSERT_ERROR(failed, CacheCode::kStateConflict);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
