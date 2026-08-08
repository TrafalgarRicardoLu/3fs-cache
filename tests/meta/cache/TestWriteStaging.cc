#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Sleep.h>
#include <gtest/gtest.h>

#include "meta/store/FileSession.h"
#include "meta/store/cache/PrefetchJobStore.h"
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
    auto &cacheTable = raw.chainTables.at(flat::ChainTableId{1}).rbegin()->second;
    cacheTable.role = flat::ChainTableRole::CACHE_DATA;
    cacheTable.logicalCapacity = 1ULL << 30;
    cacheTable.checksumType = flat::ChainTableChecksumType::CRC32C;
    auto &stagingTable = raw.chainTables.at(flat::ChainTableId{2}).rbegin()->second;
    stagingTable.role = flat::ChainTableRole::WRITE_STAGING;
    stagingTable.logicalCapacity = 0;
    stagingTable.checksumType = flat::ChainTableChecksumType::NONE;
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

CoTryTask<PublishOriginFileFromStagingReq> preparePublish(MockCluster &cluster,
                                                          std::string path,
                                                          cache::UploadJobId jobId,
                                                          uint64_t length = 4096) {
  auto &meta = cluster.meta().getOperator();
  auto created = co_await meta.createWriteStaging(stagingReq(std::move(path), jobId));
  CO_RETURN_ON_ERROR(created);
  CO_RETURN_ON_ERROR(co_await setStagingLength(cluster.kvEngine(), created->inode.id, VersionedLength{length, 0}));
  auto sealed = co_await meta.sealWriteStaging(sealReq(*created, VersionedLength{length, 0}));
  CO_RETURN_ON_ERROR(sealed);

  BeginMultipartUploadReq begin;
  begin.service = {std::string{kServiceName}, std::string{kServiceToken}};
  begin.jobId = jobId;
  begin.expectedStateVersion = sealed->job.stateVersion;
  begin.multipartId = "upload-test";
  begin.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto begun = co_await meta.beginMultipartUpload(begin);
  CO_RETURN_ON_ERROR(begun);

  CheckpointUploadPartReq checkpoint;
  checkpoint.service = begin.service;
  checkpoint.jobId = jobId;
  checkpoint.expectedStateVersion = begun->job.stateVersion;
  checkpoint.multipartId = begun->job.multipartId;
  checkpoint.part = {1, length, "etag", "crc32c:00000001"};
  checkpoint.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto uploaded = co_await meta.checkpointUploadPart(checkpoint);
  CO_RETURN_ON_ERROR(uploaded);

  MutateMultipartUploadReq mutation;
  mutation.service = begin.service;
  mutation.jobId = jobId;
  mutation.expectedStateVersion = uploaded->job.stateVersion;
  mutation.multipartId = uploaded->job.multipartId;
  mutation.mutation = MultipartUploadMutation::PREPARE_COMPLETE;
  mutation.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto completing = co_await meta.mutateMultipartUpload(mutation);
  CO_RETURN_ON_ERROR(completing);
  mutation.expectedStateVersion = completing->job.stateVersion;
  mutation.mutation = MultipartUploadMutation::SAVE_COMPLETED;
  mutation.completedObject = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                                            "bucket",
                                                            "objects/staged",
                                                            {cache::VersionSelectorType::VERSION_ID, "version-1"}};
  auto publishing = co_await meta.mutateMultipartUpload(mutation);
  CO_RETURN_ON_ERROR(publishing);

  PublishOriginFileFromStagingReq publish;
  publish.user = SUPER_USER;
  publish.service = begin.service;
  publish.jobId = jobId;
  publish.expectedStateVersion = publishing->job.stateVersion;
  publish.expectedStagingInode = created->inode.id;
  publish.metadata.object = *publishing->job.completedObject;
  publish.metadata.objectSize = length;
  publish.metadata.tableId = flat::ChainTableId{1};
  publish.metadata.blockSize = 4096;
  publish.metadata.stripeSize = 1;
  publish.metadata.permission = p644;
  publish.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  co_return publish;
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
    CO_ASSERT_TRUE(session->has_value());
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

    MutateMultipartUploadReq mutation;
    mutation.service = begin.service;
    mutation.jobId = tail->job.jobId;
    mutation.expectedStateVersion = tail->job.stateVersion;
    mutation.multipartId = tail->job.multipartId;
    mutation.mutation = MultipartUploadMutation::PREPARE_COMPLETE;
    mutation.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto completing = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(completing);
    CO_ASSERT_EQ(completing->job.state, cache::UploadJobState::COMPLETING);
    auto prepareRetry = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(prepareRetry);
    CO_ASSERT_EQ(prepareRetry->job, completing->job);

    mutation.expectedStateVersion = completing->job.stateVersion;
    mutation.mutation = MultipartUploadMutation::SAVE_COMPLETED;
    mutation.completedObject = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                                              "bucket",
                                                              "objects/staged",
                                                              {cache::VersionSelectorType::VERSION_ID, "version-1"}};
    auto publishing = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(publishing);
    CO_ASSERT_EQ(publishing->job.state, cache::UploadJobState::PUBLISHING);
    CO_ASSERT_EQ(publishing->job.completedObject, mutation.completedObject);
  }());
}

TEST_F(TestWriteStaging, AbortsMultipartProgressIdempotently) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto created = co_await meta.createWriteStaging(stagingReq("/abort", cache::UploadJobId{Uuid::from(35, 36)}));
    CO_ASSERT_OK(created);
    CO_ASSERT_OK(co_await setStagingLength(this->kvEngine(), created->inode.id, VersionedLength{4096, 0}));
    auto sealed = co_await meta.sealWriteStaging(sealReq(*created, VersionedLength{4096, 0}));
    CO_ASSERT_OK(sealed);

    BeginMultipartUploadReq begin;
    begin.service = {std::string{kServiceName}, std::string{kServiceToken}};
    begin.jobId = sealed->job.jobId;
    begin.expectedStateVersion = sealed->job.stateVersion;
    begin.multipartId = "upload-abort";
    begin.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto begun = co_await meta.beginMultipartUpload(begin);
    CO_ASSERT_OK(begun);

    MutateMultipartUploadReq mutation;
    mutation.service = begin.service;
    mutation.jobId = begun->job.jobId;
    mutation.expectedStateVersion = begun->job.stateVersion;
    mutation.multipartId = begun->job.multipartId;
    mutation.mutation = MultipartUploadMutation::BEGIN_ABORT;
    mutation.error = "cancelled by test";
    mutation.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto aborting = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(aborting);
    CO_ASSERT_EQ(aborting->job.state, cache::UploadJobState::ABORTING);
    auto abortRetry = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(abortRetry);
    CO_ASSERT_EQ(abortRetry->job, aborting->job);

    mutation.expectedStateVersion = aborting->job.stateVersion;
    mutation.mutation = MultipartUploadMutation::FINISH_ABORT;
    mutation.error.clear();
    auto cancelled = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(cancelled);
    CO_ASSERT_EQ(cancelled->job.state, cache::UploadJobState::CANCELLED);
    auto finishRetry = co_await meta.mutateMultipartUpload(mutation);
    CO_ASSERT_OK(finishRetry);
    CO_ASSERT_EQ(finishRetry->job, cancelled->job);
  }());
}

TEST_F(TestWriteStaging, PublishesOriginAndUploadJobAtomicallyAndRetriesSameInode) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto request = co_await preparePublish(cluster, "/published", cache::UploadJobId{Uuid::from(39, 40)});
    CO_ASSERT_OK(request);

    auto session = MetaTestHelper::randomSession();
    auto sessionTxn = this->kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await FileSession::create(request->expectedStagingInode, session).store(*sessionTxn));
    CO_ASSERT_OK(co_await sessionTxn->commit());

    auto &meta = cluster.meta().getOperator();
    auto published = co_await meta.publishOriginFileFromStaging(*request);
    CO_ASSERT_OK(published);
    CO_ASSERT_EQ(published->outcome, PublishOriginFileOutcome::PUBLISHED);
    CO_ASSERT_TRUE(published->inode.isOriginFile());
    CO_ASSERT_EQ(published->inode.asOriginFile().object, request->metadata.object);

    auto retry = co_await meta.publishOriginFileFromStaging(*request);
    CO_ASSERT_OK(retry);
    CO_ASSERT_EQ(retry->outcome, PublishOriginFileOutcome::ALREADY_PUBLISHED);
    CO_ASSERT_EQ(retry->inode.id, published->inode.id);

    auto read = this->kvEngine()->createReadonlyTransaction();
    auto job = co_await UploadJobStore::snapshotLoad(*read, request->jobId);
    CO_ASSERT_OK(job);
    CO_ASSERT_TRUE(job->has_value());
    CO_ASSERT_EQ((**job).state, cache::UploadJobState::PUBLISHED);
    CO_ASSERT_EQ((**job).publishedInode, published->inode.id.u64());
    auto staging = co_await Inode::snapshotLoad(*read, request->expectedStagingInode);
    CO_ASSERT_OK(staging);
    CO_ASSERT_TRUE(staging->has_value());
    CO_ASSERT_EQ((**staging).nlink, 0);
    auto openSession = co_await FileSession::load(*read, request->expectedStagingInode, session.session);
    CO_ASSERT_OK(openSession);
    CO_ASSERT_TRUE(openSession->has_value());

    GetUploadJobReq get;
    get.jobId = request->jobId;
    get.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto queried = co_await meta.getUploadJob(get);
    CO_ASSERT_OK(queried);
    CO_ASSERT_EQ(queried->stagingCleanupState, cache::StagingCleanupState::WAITING_FOR_HANDLES);
    CO_ASSERT_EQ(queried->cleanupPolicy, cache::UploadCleanupPolicy::DELETE_STAGING_AFTER_LAST_HANDLE);
    CO_ASSERT_EQ(queried->warmState, cache::UploadWarmState::PENDING);

    cache::PrefetchJobRecord warm;
    warm.spec.jobId = cache::publishedPrefetchJobId(request->jobId);
    warm.spec.ownerUid = SUPER_USER.uid;
    cache::DatasetSource warmSource;
    warmSource.source = cache::NamespacePathSource{"/published", false};
    warm.spec.sources = {std::move(warmSource)};
    warm.state = cache::PrefetchJobState::PENDING;
    warm.stateVersion = 1;
    warm.createdAtMs = warm.updatedAtMs = 1;
    auto warmTxn = this->kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await PrefetchJobStore::create(*warmTxn, warm));
    CO_ASSERT_OK(co_await warmTxn->commit());
    queried = co_await meta.getUploadJob(get);
    CO_ASSERT_OK(queried);
    CO_ASSERT_EQ(queried->prefetchJobId, warm.spec.jobId);
    CO_ASSERT_EQ(queried->warmState, cache::UploadWarmState::SUBMITTED);

    auto closeTxn = this->kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await FileSession::removeAll(*closeTxn, request->expectedStagingInode));
    CO_ASSERT_OK(co_await closeTxn->commit());
    queried = co_await meta.getUploadJob(get);
    CO_ASSERT_OK(queried);
    CO_ASSERT_EQ(queried->stagingCleanupState, cache::StagingCleanupState::QUEUED);

    ListUploadJobsReq active;
    active.service = {std::string{kServiceName}, std::string{kServiceToken}};
    active.ownerUid = SUPER_USER.uid;
    active.includeTerminal = false;
    active.limit = 10;
    active.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto activeJobs = co_await meta.listUploadJobs(active);
    CO_ASSERT_OK(activeJobs);
    CO_ASSERT_TRUE(activeJobs->jobs.empty());
  }());
}

TEST_F(TestWriteStaging, PublishCasConflictsWithConcurrentRenameAndRemovedPath) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto &store = cluster.meta().getStore();
    auto request = co_await preparePublish(cluster, "/rename-race", cache::UploadJobId{Uuid::from(41, 42)});
    CO_ASSERT_OK(request);

    auto publishTxn = this->kvEngine()->createReadWriteTransaction();
    auto operation = store.publishOriginFileFromStaging(*request);
    auto uncommitted = co_await operation->run(*publishTxn);
    CO_ASSERT_OK(uncommitted);
    auto renameTxn = this->kvEngine()->createReadWriteTransaction();
    auto oldEntry = co_await DirEntry::load(*renameTxn, InodeId::root(), "rename-race");
    CO_ASSERT_OK(oldEntry);
    CO_ASSERT_TRUE(oldEntry->has_value());
    DirEntry renamed(InodeId::root(), "renamed");
    renamed.data() = (**oldEntry).data();
    CO_ASSERT_OK(co_await (**oldEntry).remove(*renameTxn));
    CO_ASSERT_OK(co_await renamed.store(*renameTxn));
    CO_ASSERT_OK(co_await renameTxn->commit());
    CO_ASSERT_ERROR(co_await publishTxn->commit(), TransactionCode::kConflict);

    auto read = this->kvEngine()->createReadonlyTransaction();
    auto unpublishedInode = co_await Inode::snapshotLoad(*read, uncommitted->inode.id);
    CO_ASSERT_OK(unpublishedInode);
    CO_ASSERT_FALSE(unpublishedInode->has_value());
    auto job = co_await UploadJobStore::snapshotLoad(*read, request->jobId);
    CO_ASSERT_OK(job);
    CO_ASSERT_TRUE(job->has_value());
    CO_ASSERT_EQ((**job).state, cache::UploadJobState::PUBLISHING);
    CO_ASSERT_ERROR(co_await meta.publishOriginFileFromStaging(*request), CacheCode::kStateConflict);

    auto removed = co_await preparePublish(cluster, "/remove-race", cache::UploadJobId{Uuid::from(43, 44)});
    CO_ASSERT_OK(removed);
    auto removeTxn = this->kvEngine()->createReadWriteTransaction();
    auto entry = co_await DirEntry::load(*removeTxn, InodeId::root(), "remove-race");
    CO_ASSERT_OK(entry);
    CO_ASSERT_TRUE(entry->has_value());
    CO_ASSERT_OK(co_await (**entry).remove(*removeTxn));
    CO_ASSERT_OK(co_await removeTxn->commit());
    CO_ASSERT_ERROR(co_await meta.publishOriginFileFromStaging(*removed), CacheCode::kStateConflict);
  }());
}

TEST_F(TestWriteStaging, StagedPublishContractEnforcesServiceProtocolAndCacheLayout) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    PublishOriginFileFromStagingReq request;
    request.service = {std::string{kServiceName}, "wrong-token"};
    request.jobId = cache::UploadJobId{Uuid::from(37, 38)};
    request.expectedStateVersion = 7;
    request.expectedStagingInode = InodeId{42};
    request.metadata.object = {cache::OriginId{1},
                               "bucket",
                               "objects/published",
                               {cache::VersionSelectorType::VERSION_ID, "version-1"}};
    request.metadata.objectSize = 4096;
    request.metadata.tableId = flat::ChainTableId{2};
    request.metadata.blockSize = 4096;
    request.metadata.stripeSize = 1;
    request.metadata.permission = p644;
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto &meta = cluster.meta().getOperator();
    CO_ASSERT_ERROR(co_await meta.publishOriginFileFromStaging(request), MetaCode::kNoPermission);

    request.service.token = std::string{kServiceToken};
    request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    CO_ASSERT_ERROR(co_await meta.publishOriginFileFromStaging(request), CacheCode::kUpgradeRequired);
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    CO_ASSERT_ERROR(co_await meta.publishOriginFileFromStaging(request), MetaCode::kInvalidFileLayout);
  }());
}

TEST_F(TestWriteStaging, ListsUploadJobsWithServiceAuthOwnerFilterAndPagination) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    for (uint64_t id = 45; id != 48; ++id) {
      CO_ASSERT_OK(co_await meta.createWriteStaging(
          stagingReq("/listed-" + std::to_string(id), cache::UploadJobId{Uuid::from(1, id)})));
    }

    ListUploadJobsReq request;
    request.service = {std::string{kServiceName}, std::string{kServiceToken}};
    request.ownerUid = SUPER_USER.uid;
    request.limit = 2;
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto first = co_await meta.listUploadJobs(request);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->jobs.size(), 2);
    CO_ASSERT_TRUE(first->more);

    request.after = first->jobs.back().jobId;
    auto second = co_await meta.listUploadJobs(request);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->jobs.size(), 1);
    CO_ASSERT_FALSE(second->more);
    request.service.token = "wrong-token";
    CO_ASSERT_ERROR(co_await meta.listUploadJobs(request), MetaCode::kNoPermission);
  }());
}

TEST_F(TestWriteStaging, GetsUploadJobWithOwnerIsolationAndProtocolGate) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto id = cache::UploadJobId{Uuid::from(49, 50)};
    auto created = co_await meta.createWriteStaging(stagingReq("/queried", id));
    CO_ASSERT_OK(created);

    GetUploadJobReq request;
    request.jobId = id;
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto loaded = co_await meta.getUploadJob(request);
    CO_ASSERT_OK(loaded);
    CO_ASSERT_EQ(loaded->job.jobId, id);
    CO_ASSERT_EQ(loaded->job.ownerUid, flat::Uid{0});

    request.user = flat::UserInfo{flat::Uid{123}, flat::Gid{123}};
    CO_ASSERT_ERROR(co_await meta.getUploadJob(request), MetaCode::kNoPermission);
    request.user = SUPER_USER;
    request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    CO_ASSERT_ERROR(co_await meta.getUploadJob(request), CacheCode::kUpgradeRequired);
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    request.jobId = cache::UploadJobId{Uuid::from(51, 52)};
    CO_ASSERT_ERROR(co_await meta.getUploadJob(request), CacheCode::kNotFound);
  }());
}

TEST_F(TestWriteStaging, AdminListsCancelsAndRetriesUploadsWithFences) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    enableWriteStaging(cluster);
    auto &meta = cluster.meta().getOperator();
    auto cancelledId = cache::UploadJobId{Uuid::from(53, 54)};
    auto cancelled = co_await meta.createWriteStaging(stagingReq("/admin-cancel", cancelledId));
    CO_ASSERT_OK(cancelled);

    AdminListUploadJobsReq list;
    list.user = flat::UserInfo{flat::Uid{123}, flat::Gid{123}};
    list.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    CO_ASSERT_ERROR(co_await meta.adminListUploadJobs(list), MetaCode::kNoPermission);
    list.user = SUPER_USER;
    list.limit = 1;
    auto page = co_await meta.adminListUploadJobs(list);
    CO_ASSERT_OK(page);
    CO_ASSERT_EQ(page->jobs.size(), 1);
    list.jobId = cancelledId;
    auto exact = co_await meta.adminListUploadJobs(list);
    CO_ASSERT_OK(exact);
    CO_ASSERT_EQ(exact->jobs.size(), 1);
    CO_ASSERT_EQ(exact->jobs.front().jobId, cancelledId);
    list.jobId = cache::UploadJobId{Uuid::from(99, 100)};
    CO_ASSERT_ERROR(co_await meta.adminListUploadJobs(list), CacheCode::kNotFound);

    AdminMutateUploadJobReq mutate;
    mutate.user = SUPER_USER;
    mutate.jobId = cancelledId;
    mutate.expectedStateVersion = cancelled->job.stateVersion;
    mutate.mutation = AdminUploadMutation::CANCEL;
    mutate.confirm = true;
    mutate.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto cancelledResult = co_await meta.adminMutateUploadJob(mutate);
    CO_ASSERT_OK(cancelledResult);
    CO_ASSERT_EQ(cancelledResult->job.state, cache::UploadJobState::CANCELLED);
    CO_ASSERT_EQ(cancelledResult->job.writerLeaseId, Uuid::zero());
    CO_ASSERT_ERROR(co_await meta.adminMutateUploadJob(mutate), CacheCode::kStateConflict);

    auto failedId = cache::UploadJobId{Uuid::from(55, 56)};
    auto created = co_await meta.createWriteStaging(stagingReq("/admin-retry", failedId));
    CO_ASSERT_OK(created);
    CO_ASSERT_OK(co_await setStagingLength(cluster.kvEngine(), created->inode.id, VersionedLength{4096, 0}));
    auto sealed = co_await meta.sealWriteStaging(sealReq(*created, VersionedLength{4096, 0}));
    CO_ASSERT_OK(sealed);
    auto failed = sealed->job;
    failed.state = cache::UploadJobState::FAILED;
    failed.error = "credential failure";
    ++failed.stateVersion;
    auto txn = cluster.kvEngine()->createReadWriteTransaction();
    CO_ASSERT_OK(co_await UploadJobStore::update(*txn, sealed->job.stateVersion, failed));
    CO_ASSERT_OK(co_await txn->commit());

    mutate.jobId = failedId;
    mutate.expectedStateVersion = failed.stateVersion;
    mutate.mutation = AdminUploadMutation::RETRY;
    auto retried = co_await meta.adminMutateUploadJob(mutate);
    CO_ASSERT_OK(retried);
    CO_ASSERT_EQ(retried->job.state, cache::UploadJobState::SEALED);
    CO_ASSERT_TRUE(retried->job.error.empty());
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
