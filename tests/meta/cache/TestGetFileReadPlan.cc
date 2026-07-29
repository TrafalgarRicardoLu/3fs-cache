#include <fcntl.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "meta/store/cache/CacheBlockStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

class TestGetFileReadPlan : public MetaTestBase<kv::mem::MemKV> {
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

OriginFileMetadata originMetadata(std::string key, uint64_t size, Permission permission = Permission{0600}) {
  OriginFileMetadata result;
  result.object = {cache::OriginId{1},
                   "bucket",
                   std::move(key),
                   cache::VersionSelector{cache::VersionSelectorType::VERSION_ID, "version-1"}};
  result.objectSize = size;
  result.tableId = flat::ChainTableId{2};
  result.blockSize = 4096;
  result.stripeSize = 1;
  result.permission = permission;
  return result;
}

CoTryTask<Inode> importOrigin(MockCluster &cluster, std::string path, OriginFileMetadata metadata) {
  ImportOriginFileReq req;
  req.user = SUPER_USER;
  req.entry = {PathAt(std::move(path)), std::move(metadata)};
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await cluster.meta().getOperator().importOriginFile(std::move(req));
  CO_RETURN_ON_ERROR(result);
  co_return Inode(std::move(result->inode));
}

CoTryTask<SessionInfo> openOrigin(MockCluster &cluster, const PathAt &path) {
  auto session = MetaTestHelper::randomSession();
  OpenReq req(SUPER_USER, path, session, O_RDONLY);
  req.client = session.client;
  auto opened = co_await cluster.meta().getOperator().open(std::move(req));
  CO_RETURN_ON_ERROR(opened);
  co_return session;
}

GetFileReadPlanReq planReq(const UserInfo &user,
                           const ClientId &client,
                           Uuid session,
                           InodeId inode,
                           uint64_t offset,
                           uint64_t length) {
  GetFileReadPlanReq req;
  req.user = user;
  req.client = client;
  req.openSessionId = session;
  req.inode = inode;
  req.offset = offset;
  req.length = length;
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return req;
}

TEST_F(TestGetFileReadPlan, EnforcesRangeLimitAndEof) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableCacheFeature(cluster);
    constexpr uint64_t blockSize = 4096;
    auto inode = co_await importOrigin(cluster, "/large", originMetadata("large", 1002 * blockSize));
    CO_ASSERT_OK(inode);
    auto session = co_await openOrigin(cluster, PathAt("/large"));
    CO_ASSERT_OK(session);
    auto &meta = cluster.meta().getOperator();

    for (auto blocks : {uint64_t{0}, uint64_t{1}, uint64_t{100}, uint64_t{1000}}) {
      auto result = co_await meta.getFileReadPlan(
          planReq(SUPER_USER, session->client, session->session, inode->id, 0, blocks * blockSize));
      CO_ASSERT_OK(result);
      CO_ASSERT_EQ(result->inode, inode->id);
      CO_ASSERT_EQ(result->object, inode->asOriginFile().object);
      CO_ASSERT_EQ(result->blocks.size(), static_cast<size_t>(blocks));
    }

    auto eof = co_await meta.getFileReadPlan(
        planReq(SUPER_USER, session->client, session->session, inode->id, inode->fileLength(), blockSize));
    CO_ASSERT_OK(eof);
    CO_ASSERT_TRUE(eof->blocks.empty());

    auto tooLarge = co_await meta.getFileReadPlan(
        planReq(SUPER_USER, session->client, session->session, inode->id, 0, 1001 * blockSize));
    CO_ASSERT_ERROR(tooLarge, CacheCode::kRequestTooLarge);
  }());
}

TEST_F(TestGetFileReadPlan, ReturnsNoneReadyAndQueuedBlocks) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableCacheFeature(cluster);
    constexpr uint64_t blockSize = 4096;
    auto inode = co_await importOrigin(cluster, "/mixed", originMetadata("mixed", 4 * blockSize));
    CO_ASSERT_OK(inode);
    auto session = co_await openOrigin(cluster, PathAt("/mixed"));
    CO_ASSERT_OK(session);
    auto routing = cluster.mgmtdClient()->getRoutingInfo();

    auto txn = cluster.kvEngine()->createReadWriteTransaction();
    for (uint32_t block : {1U, 2U}) {
      auto offset = uint64_t{block} * blockSize;
      auto chain = inode->getChainId(*inode, offset, *routing->raw());
      CO_ASSERT_OK(chain);
      CacheBlockRecord record;
      record.key = {inode->id.u64(), cache::CacheBlockIndex{block}};
      record.chainId = *chain;
      record.blockLength = blockSize;
      record.state = block == 1 ? cache::CacheBlockState::READY : cache::CacheBlockState::QUEUED;
      record.chargeKind = block == 1 ? cache::ChargeKind::COMMITTED : cache::ChargeKind::RESERVED;
      record.chargedBytes = blockSize;
      if (block == 1) {
        record.loadEpoch = 7;
        record.cacheGeneration = cache::CacheGeneration{9};
        record.ready = cache::ReadyIdentity{7, cache::CacheGeneration{9}, 1, 1234, blockSize};
      }
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, record));
    }
    CO_ASSERT_OK(co_await txn->commit());

    auto result = co_await cluster.meta().getOperator().getFileReadPlan(
        planReq(SUPER_USER, session->client, session->session, inode->id, 1, 3 * blockSize));
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(result->blocks.size(), size_t{4});
    CO_ASSERT_EQ(result->blocks[0].state, cache::CacheBlockState::NONE);
    CO_ASSERT_EQ(result->blocks[1].state, cache::CacheBlockState::READY);
    CO_ASSERT_EQ(result->blocks[1].loadEpoch, uint64_t{7});
    CO_ASSERT_TRUE(result->blocks[1].ready.has_value());
    CO_ASSERT_EQ(result->blocks[1].ready->cacheGeneration, cache::CacheGeneration{9});
    CO_ASSERT_EQ(result->blocks[2].state, cache::CacheBlockState::QUEUED);
    for (size_t i = 0; i < result->blocks.size(); ++i) {
      CO_ASSERT_EQ(result->blocks[i].key.block, cache::CacheBlockIndex{static_cast<uint32_t>(i)});
      CO_ASSERT_EQ(result->blocks[i].fileRange, (cache::ByteRange{i * blockSize, blockSize}));
      CO_ASSERT_EQ(result->blocks[i].originRange, result->blocks[i].fileRange);
      CO_ASSERT_EQ(result->blocks[i].actualBlockLength, blockSize);
      CO_ASSERT_TRUE(static_cast<bool>(result->blocks[i].chunkId));
      CO_ASSERT_NE(result->blocks[i].chainId, flat::ChainId{});
    }
  }());
}

TEST_F(TestGetFileReadPlan, ChecksPermissionSessionAndSupersededOldFd) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableCacheFeature(cluster);
    auto oldObject = originMetadata("old", 8192);
    auto inode = co_await importOrigin(cluster, "/refresh", oldObject);
    CO_ASSERT_OK(inode);
    auto session = co_await openOrigin(cluster, PathAt("/refresh"));
    CO_ASSERT_OK(session);
    auto &meta = cluster.meta().getOperator();

    auto missing = planReq(SUPER_USER, session->client, Uuid::random(), inode->id, 0, 1);
    CO_ASSERT_ERROR(co_await meta.getFileReadPlan(missing), MetaCode::kNoPermission);
    auto wrongClient = planReq(SUPER_USER, ClientId::random(), session->session, inode->id, 0, 1);
    CO_ASSERT_ERROR(co_await meta.getFileReadPlan(wrongClient), MetaCode::kNoPermission);
    flat::UserInfo other(Uid{2}, Gid{2}, String{});
    CO_ASSERT_ERROR(co_await meta.getFileReadPlan(planReq(other, session->client, session->session, inode->id, 0, 1)),
                    MetaCode::kNoPermission);

    RefreshOriginFileReq refresh;
    refresh.user = SUPER_USER;
    refresh.requestId = Uuid::random();
    refresh.path = PathAt("/refresh");
    refresh.expectedInode = inode->id;
    refresh.oldObject = inode->asOriginFile().object;
    refresh.newMetadata = originMetadata("new", 4096);
    refresh.cacheProtocolVersion = cache::kCacheProtocolVersion;
    CO_ASSERT_OK(co_await meta.refreshOriginFile(refresh));

    auto oldPlan = co_await meta.getFileReadPlan(
        planReq(SUPER_USER, session->client, session->session, inode->id, 0, inode->fileLength()));
    CO_ASSERT_OK(oldPlan);
    CO_ASSERT_EQ(oldPlan->object, inode->asOriginFile().object);

    CloseReq close(SUPER_USER, inode->id, *session, false, std::nullopt, std::nullopt);
    close.client = session->client;
    CO_ASSERT_OK(co_await meta.close(close));
    CO_ASSERT_ERROR(
        co_await meta.getFileReadPlan(planReq(SUPER_USER, session->client, session->session, inode->id, 0, 1)),
        MetaCode::kNoPermission);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
