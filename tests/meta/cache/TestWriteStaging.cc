#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Sleep.h>
#include <gtest/gtest.h>

#include "meta/store/cache/UploadJobStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr std::string_view kServiceName = "cache-manager";
constexpr std::string_view kServiceToken = "write-staging-test-token";

class TestWriteStaging : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster(bool cancelExpired = false) {
    static MockCluster::Config recover = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().set_enable_cache_phase2(true);
      value.mock_meta().set_enable_cache_phase3(true);
      value.mock_meta().set_enable_cache_phase4(true);
      value.mock_meta().set_cache_service_name(std::string{kServiceName});
      value.mock_meta().set_cache_service_token(std::string{kServiceToken});
      value.mock_meta().event_trace_log().set_enabled(false);
      return value;
    }();
    static MockCluster::Config cancel = [] {
      auto value = recover;
      value.mock_meta().set_write_staging_expired_action("cancel");
      return value;
    }();
    return createMockCluster(cancelExpired ? cancel : recover);
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

RenewWriteStagingLeaseReq renewReq(const CreateWriteStagingRsp &created, uint64_t expiresAtMs) {
  RenewWriteStagingLeaseReq req;
  req.user = SUPER_USER;
  req.jobId = created.job.jobId;
  req.expectedStateVersion = created.job.stateVersion;
  req.writerLeaseId = created.job.writerLeaseId;
  req.writerLeaseExpiresAtMs = expiresAtMs;
  req.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  return req;
}

SealWriteStagingReq sealReq(const CreateWriteStagingRsp &created, VersionedLength length) {
  SealWriteStagingReq req;
  req.user = SUPER_USER;
  req.jobId = created.job.jobId;
  req.expectedStateVersion = created.job.stateVersion;
  req.stagingInode = created.inode.id;
  req.writerLeaseId = created.job.writerLeaseId;
  req.finalLength = length;
  req.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  return req;
}

CoTryTask<Inode> setStagingLength(std::shared_ptr<kv::IKVEngine> engine, InodeId inodeId, VersionedLength length) {
  auto txn = engine->createReadWriteTransaction();
  auto inode = (co_await Inode::load(*txn, inodeId)).then(checkMetaFound<Inode>);
  CO_RETURN_ON_ERROR(inode);
  inode->asFile().setVersionedLength(length);
  CO_RETURN_ON_ERROR(co_await inode->store(*txn));
  CO_RETURN_ON_ERROR(co_await txn->commit());
  co_return std::move(*inode);
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

TEST_F(TestWriteStaging, RenewsOnlyTheActiveUnexpiredWriterLease) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto created = co_await meta.createWriteStaging(stagingReq("/renew", cache::UploadJobId{Uuid::from(21, 22)}));
    CO_ASSERT_OK(created);

    auto renew = renewReq(*created, created->job.writerLeaseExpiresAtMs + 60'000);
    auto renewed = co_await meta.renewWriteStagingLease(renew);
    CO_ASSERT_OK(renewed);
    CO_ASSERT_EQ(renewed->job.stateVersion, created->job.stateVersion + 1);
    CO_ASSERT_EQ(renewed->job.writerLeaseExpiresAtMs, renew.writerLeaseExpiresAtMs);
    auto retry = co_await meta.renewWriteStagingLease(renew);
    CO_ASSERT_OK(retry);
    CO_ASSERT_EQ(retry->job, renewed->job);

    auto competing = renew;
    competing.expectedStateVersion = renewed->job.stateVersion;
    competing.writerLeaseId = Uuid::from(23, 24);
    competing.writerLeaseExpiresAtMs++;
    CO_ASSERT_ERROR(co_await meta.renewWriteStagingLease(competing), CacheCode::kStateConflict);
  }());
}

TEST_F(TestWriteStaging, SealsExactLengthAndRejectsFurtherMetadataWrites) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto created = co_await meta.createWriteStaging(stagingReq("/seal", cache::UploadJobId{Uuid::from(25, 26)}));
    CO_ASSERT_OK(created);
    VersionedLength finalLength{8192, 0};
    CO_ASSERT_OK(co_await setStagingLength(this->kvEngine(), created->inode.id, finalLength));

    auto seal = sealReq(*created, VersionedLength{4096, 0});
    CO_ASSERT_ERROR(co_await meta.sealWriteStaging(seal), CacheCode::kStateConflict);
    seal.finalLength = finalLength;
    auto sealed = co_await meta.sealWriteStaging(seal);
    CO_ASSERT_OK(sealed);
    CO_ASSERT_EQ(sealed->job.state, cache::UploadJobState::SEALED);
    CO_ASSERT_EQ(sealed->job.stagingLength, finalLength.length);
    CO_ASSERT_TRUE(sealed->inode.acl.iflags & FS_IMMUTABLE_FL);
    auto retry = co_await meta.sealWriteStaging(seal);
    CO_ASSERT_OK(retry);
    CO_ASSERT_EQ(retry->job, sealed->job);

    auto open = OpenReq(SUPER_USER, Path("/seal"), MetaTestHelper::randomSession(), O_WRONLY);
    CO_ASSERT_ERROR(co_await meta.open(open), MetaCode::kNoPermission);
    auto sync = SyncReq(SUPER_USER, created->inode.id, true, {}, {}, false, VersionedLength{16384, 0});
    CO_ASSERT_ERROR(co_await meta.sync(sync), MetaCode::kNoPermission);
    auto read = this->kvEngine()->createReadonlyTransaction();
    auto session = co_await FileSession::snapshotCheckExists(*read, created->inode.id);
    CO_ASSERT_OK(session);
    CO_ASSERT_FALSE(session->has_value());
  }());
}

TEST_F(TestWriteStaging, SealConflictsWithAConcurrentLengthUpdate) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &store = cluster.meta().getStore();
    auto created = co_await cluster.meta().getOperator().createWriteStaging(
        stagingReq("/seal-race", cache::UploadJobId{Uuid::from(31, 32)}));
    CO_ASSERT_OK(created);
    VersionedLength sealedLength{4096, 0};
    CO_ASSERT_OK(co_await setStagingLength(this->kvEngine(), created->inode.id, sealedLength));

    auto writeTxn = this->kvEngine()->createReadWriteTransaction();
    auto writing = (co_await Inode::load(*writeTxn, created->inode.id)).then(checkMetaFound<Inode>);
    CO_ASSERT_OK(writing);
    writing->asFile().setVersionedLength(VersionedLength{8192, 0});
    CO_ASSERT_OK(co_await writing->store(*writeTxn));

    auto sealTxn = this->kvEngine()->createReadWriteTransaction();
    auto seal = sealReq(*created, sealedLength);
    auto operation = store.sealWriteStaging(seal);
    auto sealed = co_await operation->run(*sealTxn);
    CO_ASSERT_OK(sealed);
    CO_ASSERT_OK(co_await sealTxn->commit());
    CO_ASSERT_ERROR(co_await writeTxn->commit(), TransactionCode::kConflict);

    auto read = this->kvEngine()->createReadonlyTransaction();
    auto inode = (co_await Inode::snapshotLoad(*read, created->inode.id)).then(checkMetaFound<Inode>);
    CO_ASSERT_OK(inode);
    CO_ASSERT_EQ(inode->asFile().getVersionedLength(), sealedLength);
    CO_ASSERT_TRUE(inode->acl.iflags & FS_IMMUTABLE_FL);
  }());
}

TEST_F(TestWriteStaging, ExpiredLeaseRecoversOrCancelsWithExactFence) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto recoverCluster = createCluster();
    enableWriteStaging(recoverCluster);
    auto &recoverMeta = recoverCluster.meta().getOperator();
    auto recoverReq = stagingReq("/expired-recover", cache::UploadJobId{Uuid::from(27, 28)});
    recoverReq.writerLeaseExpiresAtMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000) + 100;
    auto recoverCreated = co_await recoverMeta.createWriteStaging(recoverReq);
    CO_ASSERT_OK(recoverCreated);
    CO_ASSERT_OK(co_await setStagingLength(this->kvEngine(), recoverCreated->inode.id, VersionedLength{4096, 0}));
    co_await folly::coro::sleep(std::chrono::milliseconds(110));

    RecoverExpiredWriteStagingReq recover;
    recover.user = SUPER_USER;
    recover.jobId = recoverCreated->job.jobId;
    recover.expectedStateVersion = recoverCreated->job.stateVersion;
    recover.expectedWriterLeaseId = recoverCreated->job.writerLeaseId;
    recover.expectedWriterLeaseExpiresAtMs = recoverCreated->job.writerLeaseExpiresAtMs;
    recover.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto recovered = co_await recoverMeta.recoverExpiredWriteStaging(recover);
    CO_ASSERT_OK(recovered);
    CO_ASSERT_EQ(recovered->job.state, cache::UploadJobState::SEALED);
    auto retry = co_await recoverMeta.recoverExpiredWriteStaging(recover);
    CO_ASSERT_OK(retry);
    CO_ASSERT_EQ(retry->job, recovered->job);

    auto cancelCluster = createCluster(true);
    enableWriteStaging(cancelCluster);
    auto &cancelMeta = cancelCluster.meta().getOperator();
    auto cancelReq = stagingReq("/expired-cancel", cache::UploadJobId{Uuid::from(29, 30)});
    cancelReq.writerLeaseExpiresAtMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000) + 100;
    auto cancelCreated = co_await cancelMeta.createWriteStaging(cancelReq);
    CO_ASSERT_OK(cancelCreated);
    co_await folly::coro::sleep(std::chrono::milliseconds(110));
    recover.jobId = cancelCreated->job.jobId;
    recover.expectedStateVersion = cancelCreated->job.stateVersion;
    recover.expectedWriterLeaseId = cancelCreated->job.writerLeaseId;
    recover.expectedWriterLeaseExpiresAtMs = cancelCreated->job.writerLeaseExpiresAtMs;
    auto cancelled = co_await cancelMeta.recoverExpiredWriteStaging(recover);
    CO_ASSERT_OK(cancelled);
    CO_ASSERT_EQ(cancelled->job.state, cache::UploadJobState::CANCELLED);
    CO_ASSERT_TRUE(cancelled->inode.acl.iflags & FS_IMMUTABLE_FL);
  }());
}

TEST_F(TestWriteStaging, BeginsAndCheckpointsMultipartProgressWithIdempotentFences) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto created = co_await meta.createWriteStaging(stagingReq("/multipart", cache::UploadJobId{Uuid::from(33, 34)}));
    CO_ASSERT_OK(created);
    CO_ASSERT_OK(co_await setStagingLength(this->kvEngine(), created->inode.id, VersionedLength{6144, 0}));
    auto sealed = co_await meta.sealWriteStaging(sealReq(*created, VersionedLength{6144, 0}));
    CO_ASSERT_OK(sealed);

    BeginMultipartUploadReq begin;
    begin.service = {std::string{kServiceName}, std::string{kServiceToken}};
    begin.jobId = sealed->job.jobId;
    begin.expectedStateVersion = sealed->job.stateVersion;
    begin.multipartId = "upload-1";
    begin.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto denied = begin;
    denied.service.token = "wrong-token";
    CO_ASSERT_ERROR(co_await meta.beginMultipartUpload(denied), MetaCode::kNoPermission);
    auto begun = co_await meta.beginMultipartUpload(begin);
    CO_ASSERT_OK(begun);
    CO_ASSERT_EQ(begun->job.state, cache::UploadJobState::UPLOADING);
    CO_ASSERT_EQ(begun->job.multipartId, begin.multipartId);
    auto beginRetry = co_await meta.beginMultipartUpload(begin);
    CO_ASSERT_OK(beginRetry);
    CO_ASSERT_EQ(beginRetry->job, begun->job);

    CheckpointUploadPartReq checkpoint;
    checkpoint.service = begin.service;
    checkpoint.jobId = begun->job.jobId;
    checkpoint.expectedStateVersion = begun->job.stateVersion;
    checkpoint.multipartId = begun->job.multipartId;
    checkpoint.part = {1, 4096, "etag-1", "crc32c:00000001"};
    checkpoint.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto first = co_await meta.checkpointUploadPart(checkpoint);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->job.parts, std::vector<cache::CompletedUploadPart>{checkpoint.part});
    CO_ASSERT_EQ(first->job.nextPartNumber, 2);
    auto checkpointRetry = co_await meta.checkpointUploadPart(checkpoint);
    CO_ASSERT_OK(checkpointRetry);
    CO_ASSERT_EQ(checkpointRetry->job, first->job);

    checkpoint.part.etag = "different";
    CO_ASSERT_ERROR(co_await meta.checkpointUploadPart(checkpoint), CacheCode::kStateConflict);
    checkpoint.expectedStateVersion = first->job.stateVersion;
    checkpoint.part = {2, 2048, "etag-2", "crc32c:00000002"};
    auto tail = co_await meta.checkpointUploadPart(checkpoint);
    CO_ASSERT_OK(tail);
    CO_ASSERT_EQ(tail->job.parts.size(), 2);
    CO_ASSERT_EQ(tail->job.nextPartNumber, 3);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
