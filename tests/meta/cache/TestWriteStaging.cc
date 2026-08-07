#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "meta/store/cache/UploadJobStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

class TestWriteStaging : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster() {
    static MockCluster::Config config = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().set_enable_cache_phase2(true);
      value.mock_meta().set_enable_cache_phase3(true);
      value.mock_meta().set_enable_cache_phase4(true);
      value.mock_meta().event_trace_log().set_enabled(false);
      return value;
    }();
    return createMockCluster(config);
  }

  void enableWriteStaging(MockCluster &cluster) {
    auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
    auto &raw = *routing->raw();
    for (auto &[_, node] : raw.nodes) {
      if (node.type == flat::NodeType::META) {
        node.cacheSchemaVersion = cache::kCachePhase4SchemaVersion;
        node.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
      }
    }
    auto &table = raw.chainTables.at(flat::ChainTableId{2}).rbegin()->second;
    table.role = flat::ChainTableRole::WRITE_STAGING;
    table.logicalCapacity = 0;
    table.checksumType = flat::ChainTableChecksumType::NONE;
    cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
  }
};

CreateWriteStagingReq stagingReq(std::string path, cache::UploadJobId jobId = cache::UploadJobId{Uuid::from(1, 2)}) {
  CreateWriteStagingReq req;
  req.user = SUPER_USER;
  req.path = PathAt(std::move(path));
  req.jobId = jobId;
  req.destination = {cache::OriginId{1}, "bucket", "objects/staged"};
  req.tableId = flat::ChainTableId{2};
  req.chunkSize = 4096;
  req.stripeSize = 1;
  req.permission = p644;
  req.session = MetaTestHelper::randomSession();
  req.writerLeaseId = Uuid::from(3, 4);
  req.writerLeaseExpiresAtMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000) + 60'000;
  req.mode = WriteStagingMode::SEQUENTIAL;
  req.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  return req;
}

TEST_F(TestWriteStaging, CreatesInodeSessionAndJobAtomicallyAndRetriesByJobId) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto &meta = cluster.meta().getOperator();
    auto req = stagingReq("/staged");
    CO_ASSERT_ERROR(co_await meta.createWriteStaging(req), CacheCode::kFeatureDisabled);

    enableWriteStaging(cluster);
    auto created = co_await meta.createWriteStaging(req);
    CO_ASSERT_OK(created);
    CO_ASSERT_TRUE(created->created);
    CO_ASSERT_TRUE(created->inode.isFile());
    CO_ASSERT_EQ(created->inode.fileLayout().tableId, req.tableId);
    CO_ASSERT_EQ(created->job.stagingInode, created->inode.id.u64());
    CO_ASSERT_EQ(created->job.state, cache::UploadJobState::OPEN);

    auto retry = co_await meta.createWriteStaging(req);
    CO_ASSERT_OK(retry);
    CO_ASSERT_FALSE(retry->created);
    CO_ASSERT_EQ(retry->inode.id, created->inode.id);
    CO_ASSERT_EQ(retry->job, created->job);

    auto read = this->kvEngine()->createReadonlyTransaction();
    auto loaded = co_await UploadJobStore::snapshotLoad(*read, req.jobId);
    CO_ASSERT_OK(loaded);
    CO_ASSERT_TRUE(loaded->has_value());
    CO_ASSERT_EQ(**loaded, created->job);
  }());
}

TEST_F(TestWriteStaging, RejectsPathSpecPermissionRoleAndWriteModeConflicts) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto &store = cluster.meta().getStore();

    auto req = stagingReq("/conflict");
    CO_ASSERT_OK(co_await meta.createWriteStaging(req));

    auto anotherJob = req;
    anotherJob.jobId = cache::UploadJobId{Uuid::from(5, 6)};
    CO_ASSERT_ERROR(co_await meta.createWriteStaging(anotherJob), MetaCode::kExists);
    auto anotherSpec = req;
    anotherSpec.destination.key = "objects/other";
    CO_ASSERT_ERROR(co_await meta.createWriteStaging(anotherSpec), MetaCode::kExists);

    auto wrongRole = stagingReq("/wrong-role", cache::UploadJobId{Uuid::from(7, 8)});
    wrongRole.tableId = flat::ChainTableId{1};
    CO_ASSERT_ERROR(co_await meta.createWriteStaging(wrongRole), MetaCode::kInvalidFileLayout);
    auto wrongMode = stagingReq("/wrong-mode", cache::UploadJobId{Uuid::from(9, 10)});
    wrongMode.mode = WriteStagingMode::INVALID;
    CO_ASSERT_ERROR(co_await meta.createWriteStaging(wrongMode), StatusCode::kInvalidArg);

    CO_ASSERT_OK(co_await meta.mkdirs({SUPER_USER, "/private", p700, false}));
    auto denied = stagingReq("/private/denied", cache::UploadJobId{Uuid::from(11, 12)});
    denied.user = UserInfo{Uid{123}, Gid{123}};
    auto txn = this->kvEngine()->createReadWriteTransaction();
    auto op = store.createWriteStaging(denied);
    CO_ASSERT_ERROR(co_await op->run(*txn), MetaCode::kNoPermission);
  }());
}

TEST_F(TestWriteStaging, ConcurrentCreatesConflictWithoutPartialJob) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &store = cluster.meta().getStore();
    auto req = stagingReq("/raced", cache::UploadJobId{Uuid::from(13, 14)});

    auto firstTxn = this->kvEngine()->createReadWriteTransaction();
    auto secondTxn = this->kvEngine()->createReadWriteTransaction();
    auto firstOp = store.createWriteStaging(req);
    auto secondReq = req;
    secondReq.jobId = cache::UploadJobId{Uuid::from(15, 16)};
    secondReq.writerLeaseId = Uuid::from(17, 18);
    auto secondOp = store.createWriteStaging(secondReq);
    CO_ASSERT_OK(co_await firstOp->run(*firstTxn));
    CO_ASSERT_OK(co_await secondOp->run(*secondTxn));
    CO_ASSERT_OK(co_await firstTxn->commit());
    CO_ASSERT_ERROR(co_await secondTxn->commit(), TransactionCode::kConflict);

    auto read = this->kvEngine()->createReadonlyTransaction();
    auto winner = co_await UploadJobStore::snapshotLoad(*read, req.jobId);
    CO_ASSERT_OK(winner);
    CO_ASSERT_TRUE(winner->has_value());
    auto loser = co_await UploadJobStore::snapshotLoad(*read, secondReq.jobId);
    CO_ASSERT_OK(loser);
    CO_ASSERT_FALSE(loser->has_value());
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
