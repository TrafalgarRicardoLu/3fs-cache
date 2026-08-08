#pragma once

#include <folly/concurrency/AtomicSharedPtr.h>
#include <optional>

#include "common/serde/ClientContext.h"
#include "common/serde/MessagePacket.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "fbs/meta/Service.h"
#include "fbs/mgmtd/ChainRef.h"
#include "stubs/MetaService/IMetaServiceStub.h"
#include "stubs/MetaService/MetaServiceStub.h"
#include "stubs/common/Stub.h"

namespace hf3fs::meta {

using flat::ChainTableId;
using flat::Uid;
using flat::UserInfo;

class DummyMetaServiceStub : public IMetaServiceStub {
 public:
  ~DummyMetaServiceStub() override = default;

#define NOT_IMPLEMENTED_FUNC(NAME, REQ, RESP)                                                          \
  CoTryTask<RESP> NAME(const REQ &req, const net::UserRequestOptions &, serde::Timestamp *) override { \
    co_return co_await NAME(req);                                                                      \
  }                                                                                                    \
  virtual CoTryTask<RESP> NAME(const REQ &) { co_return makeError(StatusCode::kNotImplemented); }

  NOT_IMPLEMENTED_FUNC(statFs, StatFsReq, StatFsRsp);
  NOT_IMPLEMENTED_FUNC(stat, StatReq, StatRsp);
  NOT_IMPLEMENTED_FUNC(create, CreateReq, CreateRsp);
  NOT_IMPLEMENTED_FUNC(mkdirs, MkdirsReq, MkdirsRsp);
  NOT_IMPLEMENTED_FUNC(symlink, SymlinkReq, SymlinkRsp);
  NOT_IMPLEMENTED_FUNC(hardLink, HardLinkReq, HardLinkRsp);
  NOT_IMPLEMENTED_FUNC(remove, RemoveReq, RemoveRsp);
  NOT_IMPLEMENTED_FUNC(open, OpenReq, OpenRsp);
  NOT_IMPLEMENTED_FUNC(sync, SyncReq, SyncRsp);
  NOT_IMPLEMENTED_FUNC(close, CloseReq, CloseRsp);
  NOT_IMPLEMENTED_FUNC(rename, RenameReq, RenameRsp);
  NOT_IMPLEMENTED_FUNC(list, ListReq, ListRsp);
  NOT_IMPLEMENTED_FUNC(truncate, TruncateReq, TruncateRsp);
  NOT_IMPLEMENTED_FUNC(getRealPath, GetRealPathReq, GetRealPathRsp);
  NOT_IMPLEMENTED_FUNC(setAttr, SetAttrReq, SetAttrRsp);
  NOT_IMPLEMENTED_FUNC(pruneSession, PruneSessionReq, PruneSessionRsp);
  NOT_IMPLEMENTED_FUNC(dropUserCache, DropUserCacheReq, DropUserCacheRsp);
  NOT_IMPLEMENTED_FUNC(lockDirectory, LockDirectoryReq, LockDirectoryRsp);
  NOT_IMPLEMENTED_FUNC(testRpc, TestRpcReq, TestRpcRsp);
  NOT_IMPLEMENTED_FUNC(batchStat, BatchStatReq, BatchStatRsp);
  NOT_IMPLEMENTED_FUNC(batchStatByPath, BatchStatByPathReq, BatchStatByPathRsp);
  NOT_IMPLEMENTED_FUNC(importOriginFile, ImportOriginFileReq, ImportOriginFileRsp);
  NOT_IMPLEMENTED_FUNC(batchImportOriginFiles, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  NOT_IMPLEMENTED_FUNC(refreshOriginFile, RefreshOriginFileReq, RefreshOriginFileRsp);
  NOT_IMPLEMENTED_FUNC(getFileReadPlan, GetFileReadPlanReq, GetFileReadPlanRsp);
  NOT_IMPLEMENTED_FUNC(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  NOT_IMPLEMENTED_FUNC(listCacheBlocks, ListCacheBlocksReq, ListCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(listRecoverableCachePermits, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  NOT_IMPLEMENTED_FUNC(cancelQueuedAdmissions, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);
  NOT_IMPLEMENTED_FUNC(updateCacheBlockAccess, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
  NOT_IMPLEMENTED_FUNC(beginEvictCacheBlocks, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(listEvictingCacheBlocks, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(listReadyCacheBlocks, ListReadyCacheBlocksReq, ListReadyCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(reportCacheStorageEvents, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
  NOT_IMPLEMENTED_FUNC(listCacheEventDeadLetters, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
  NOT_IMPLEMENTED_FUNC(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  NOT_IMPLEMENTED_FUNC(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  NOT_IMPLEMENTED_FUNC(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  NOT_IMPLEMENTED_FUNC(updatePrefetchJob, UpdatePrefetchJobReq, UpdatePrefetchJobRsp);
  NOT_IMPLEMENTED_FUNC(appendPrefetchPlan, AppendPrefetchPlanReq, AppendPrefetchPlanRsp);
  NOT_IMPLEMENTED_FUNC(listPrefetchPlan, ListPrefetchPlanReq, ListPrefetchPlanRsp);
  NOT_IMPLEMENTED_FUNC(upsertCachePins, UpsertCachePinsReq, UpsertCachePinsRsp);
  NOT_IMPLEMENTED_FUNC(removeCachePins, RemoveCachePinsReq, RemoveCachePinsRsp);
  NOT_IMPLEMENTED_FUNC(listCachePinsByOwner, ListCachePinsByOwnerReq, ListCachePinsByOwnerRsp);
  NOT_IMPLEMENTED_FUNC(queryCachePins, QueryCachePinsReq, QueryCachePinsRsp);
  NOT_IMPLEMENTED_FUNC(updatePrefetchPlanEntries, UpdatePrefetchPlanEntriesReq, UpdatePrefetchPlanEntriesRsp);
  NOT_IMPLEMENTED_FUNC(trackPrefetchReady, TrackPrefetchReadyReq, TrackPrefetchReadyRsp);
  NOT_IMPLEMENTED_FUNC(advancePrefetchJobState, AdvancePrefetchJobStateReq, AdvancePrefetchJobStateRsp);
  NOT_IMPLEMENTED_FUNC(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  NOT_IMPLEMENTED_FUNC(convertActiveJobPins, ConvertActiveJobPinsReq, ConvertActiveJobPinsRsp);
  NOT_IMPLEMENTED_FUNC(reconcileCacheBlocks, ReconcileCacheBlocksReq, ReconcileCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(listReconcileCacheBlocks, ListReconcileCacheBlocksReq, ListReconcileCacheBlocksRsp);
  NOT_IMPLEMENTED_FUNC(recoverExpiredCacheLoads, RecoverExpiredCacheLoadsReq, RecoverExpiredCacheLoadsRsp);
  NOT_IMPLEMENTED_FUNC(createWriteStaging, CreateWriteStagingReq, CreateWriteStagingRsp);
  NOT_IMPLEMENTED_FUNC(renewWriteStagingLease, RenewWriteStagingLeaseReq, RenewWriteStagingLeaseRsp);
  NOT_IMPLEMENTED_FUNC(sealWriteStaging, SealWriteStagingReq, SealWriteStagingRsp);
  NOT_IMPLEMENTED_FUNC(recoverExpiredWriteStaging, RecoverExpiredWriteStagingReq, RecoverExpiredWriteStagingRsp);
  NOT_IMPLEMENTED_FUNC(beginMultipartUpload, BeginMultipartUploadReq, BeginMultipartUploadRsp);
  NOT_IMPLEMENTED_FUNC(checkpointUploadPart, CheckpointUploadPartReq, CheckpointUploadPartRsp);
  NOT_IMPLEMENTED_FUNC(mutateMultipartUpload, MutateMultipartUploadReq, MutateMultipartUploadRsp);
  NOT_IMPLEMENTED_FUNC(publishOriginFileFromStaging, PublishOriginFileFromStagingReq, PublishOriginFileFromStagingRsp);
  NOT_IMPLEMENTED_FUNC(listUploadJobs, ListUploadJobsReq, ListUploadJobsRsp);
  NOT_IMPLEMENTED_FUNC(getUploadJob, GetUploadJobReq, GetUploadJobRsp);

  virtual CoTryTask<AuthRsp> authenticate(const AuthReq &req) { co_return AuthRsp{req.user}; }
  CoTryTask<AuthRsp> authenticate(const AuthReq &req, const net::UserRequestOptions &, serde::Timestamp *) override {
    co_return co_await authenticate(req);
  }

#undef NOT_IMPLEMENTED_FUNC
};

struct MockMetaStubHolder {
  void setStub(std::unique_ptr<DummyMetaServiceStub> st) { stub = std::move(st); }

  folly::atomic_shared_ptr<IMetaServiceStub> stub;
};

class DummyMetaServiceStubWithInode : public DummyMetaServiceStub {
 public:
  DummyMetaServiceStubWithInode(std::variant<File, Directory, Symlink, OriginFile> data)
      : inode_(InodeId(0x10de1d), InodeData{data, Acl{Uid(0), Gid(0), Permission(0777)}}) {}

  CoTryTask<CreateRsp> create(const CreateReq &) override { co_return CreateRsp(inode_, false); }
  CoTryTask<OpenRsp> open(const OpenReq &) override { co_return OpenRsp(inode_, false); }
  CoTryTask<StatRsp> stat(const StatReq &) override { co_return StatRsp(inode_); }

 protected:
  Inode inode_;
};
class DummyMetaServiceStubWithDir : public DummyMetaServiceStubWithInode {
 public:
  DummyMetaServiceStubWithDir()
      : DummyMetaServiceStubWithInode(Directory{InodeId(0x10de1dff), Layout::newEmpty(ChainTableId(1), 512 << 10, 8)}) {
  }
};
class DummyMetaServiceStubWithFile : public DummyMetaServiceStubWithInode {
 public:
  DummyMetaServiceStubWithFile()
      : DummyMetaServiceStubWithInode(File(Layout::newEmpty(ChainTableId(1), 512 << 10, 8))) {}
};
class DummyMetaServiceStubWithSymlink : public DummyMetaServiceStubWithInode {
 public:
  DummyMetaServiceStubWithSymlink()
      : DummyMetaServiceStubWithInode(Symlink{"/b"}) {}
};
}  // namespace hf3fs::meta

template <>
struct ::hf3fs::stubs::StubMockContext<hf3fs::meta::IMetaServiceStub> {
  std::shared_ptr<meta::MockMetaStubHolder> stub;
};

namespace hf3fs::meta {

template <>
class MetaServiceStub<hf3fs::stubs::StubMockContext<IMetaServiceStub>> : public IMetaServiceStub {
 public:
  using ContextType = hf3fs::stubs::StubMockContext<IMetaServiceStub>;

  MetaServiceStub(ContextType ctx)
      : stub_(std::move(ctx.stub)) {}
  ~MetaServiceStub() override = default;

#define FORWARD_RPC_FUNC(NAME, REQ, RESP)                                                                           \
  CoTryTask<RESP> NAME(const REQ &req, const net::UserRequestOptions &opts, serde::Timestamp *timestamp) override { \
    co_return co_await stub_->stub.load()->NAME(req, opts, timestamp);                                              \
  }

  FORWARD_RPC_FUNC(statFs, StatFsReq, StatFsRsp);
  FORWARD_RPC_FUNC(stat, StatReq, StatRsp);
  FORWARD_RPC_FUNC(create, CreateReq, CreateRsp);
  FORWARD_RPC_FUNC(mkdirs, MkdirsReq, MkdirsRsp);
  FORWARD_RPC_FUNC(symlink, SymlinkReq, SymlinkRsp);
  FORWARD_RPC_FUNC(hardLink, HardLinkReq, HardLinkRsp);
  FORWARD_RPC_FUNC(remove, RemoveReq, RemoveRsp);
  FORWARD_RPC_FUNC(open, OpenReq, OpenRsp);
  FORWARD_RPC_FUNC(sync, SyncReq, SyncRsp);
  FORWARD_RPC_FUNC(close, CloseReq, CloseRsp);
  FORWARD_RPC_FUNC(rename, RenameReq, RenameRsp);
  FORWARD_RPC_FUNC(list, ListReq, ListRsp);
  FORWARD_RPC_FUNC(truncate, TruncateReq, TruncateRsp);
  FORWARD_RPC_FUNC(getRealPath, GetRealPathReq, GetRealPathRsp);
  FORWARD_RPC_FUNC(setAttr, SetAttrReq, SetAttrRsp);
  FORWARD_RPC_FUNC(pruneSession, PruneSessionReq, PruneSessionRsp);
  FORWARD_RPC_FUNC(dropUserCache, DropUserCacheReq, DropUserCacheRsp);
  FORWARD_RPC_FUNC(authenticate, AuthReq, AuthRsp);
  FORWARD_RPC_FUNC(lockDirectory, LockDirectoryReq, LockDirectoryRsp);
  FORWARD_RPC_FUNC(testRpc, TestRpcReq, TestRpcRsp);
  FORWARD_RPC_FUNC(batchStat, BatchStatReq, BatchStatRsp);
  FORWARD_RPC_FUNC(batchStatByPath, BatchStatByPathReq, BatchStatByPathRsp);
  FORWARD_RPC_FUNC(importOriginFile, ImportOriginFileReq, ImportOriginFileRsp);
  FORWARD_RPC_FUNC(batchImportOriginFiles, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  FORWARD_RPC_FUNC(refreshOriginFile, RefreshOriginFileReq, RefreshOriginFileRsp);
  FORWARD_RPC_FUNC(getFileReadPlan, GetFileReadPlanReq, GetFileReadPlanRsp);
  FORWARD_RPC_FUNC(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  FORWARD_RPC_FUNC(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  FORWARD_RPC_FUNC(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  FORWARD_RPC_FUNC(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
  FORWARD_RPC_FUNC(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  FORWARD_RPC_FUNC(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  FORWARD_RPC_FUNC(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  FORWARD_RPC_FUNC(listCacheBlocks, ListCacheBlocksReq, ListCacheBlocksRsp);
  FORWARD_RPC_FUNC(listRecoverableCachePermits, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  FORWARD_RPC_FUNC(cancelQueuedAdmissions, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);
  FORWARD_RPC_FUNC(updateCacheBlockAccess, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
  FORWARD_RPC_FUNC(beginEvictCacheBlocks, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
  FORWARD_RPC_FUNC(listEvictingCacheBlocks, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
  FORWARD_RPC_FUNC(listReadyCacheBlocks, ListReadyCacheBlocksReq, ListReadyCacheBlocksRsp);
  FORWARD_RPC_FUNC(reportCacheStorageEvents, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
  FORWARD_RPC_FUNC(listCacheEventDeadLetters, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
  FORWARD_RPC_FUNC(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  FORWARD_RPC_FUNC(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  FORWARD_RPC_FUNC(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  FORWARD_RPC_FUNC(updatePrefetchJob, UpdatePrefetchJobReq, UpdatePrefetchJobRsp);
  FORWARD_RPC_FUNC(appendPrefetchPlan, AppendPrefetchPlanReq, AppendPrefetchPlanRsp);
  FORWARD_RPC_FUNC(listPrefetchPlan, ListPrefetchPlanReq, ListPrefetchPlanRsp);
  FORWARD_RPC_FUNC(upsertCachePins, UpsertCachePinsReq, UpsertCachePinsRsp);
  FORWARD_RPC_FUNC(removeCachePins, RemoveCachePinsReq, RemoveCachePinsRsp);
  FORWARD_RPC_FUNC(listCachePinsByOwner, ListCachePinsByOwnerReq, ListCachePinsByOwnerRsp);
  FORWARD_RPC_FUNC(queryCachePins, QueryCachePinsReq, QueryCachePinsRsp);
  FORWARD_RPC_FUNC(updatePrefetchPlanEntries, UpdatePrefetchPlanEntriesReq, UpdatePrefetchPlanEntriesRsp);
  FORWARD_RPC_FUNC(trackPrefetchReady, TrackPrefetchReadyReq, TrackPrefetchReadyRsp);
  FORWARD_RPC_FUNC(advancePrefetchJobState, AdvancePrefetchJobStateReq, AdvancePrefetchJobStateRsp);
  FORWARD_RPC_FUNC(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  FORWARD_RPC_FUNC(convertActiveJobPins, ConvertActiveJobPinsReq, ConvertActiveJobPinsRsp);
  FORWARD_RPC_FUNC(reconcileCacheBlocks, ReconcileCacheBlocksReq, ReconcileCacheBlocksRsp);
  FORWARD_RPC_FUNC(listReconcileCacheBlocks, ListReconcileCacheBlocksReq, ListReconcileCacheBlocksRsp);
  FORWARD_RPC_FUNC(recoverExpiredCacheLoads, RecoverExpiredCacheLoadsReq, RecoverExpiredCacheLoadsRsp);
  FORWARD_RPC_FUNC(createWriteStaging, CreateWriteStagingReq, CreateWriteStagingRsp);
  FORWARD_RPC_FUNC(renewWriteStagingLease, RenewWriteStagingLeaseReq, RenewWriteStagingLeaseRsp);
  FORWARD_RPC_FUNC(sealWriteStaging, SealWriteStagingReq, SealWriteStagingRsp);
  FORWARD_RPC_FUNC(recoverExpiredWriteStaging, RecoverExpiredWriteStagingReq, RecoverExpiredWriteStagingRsp);
  FORWARD_RPC_FUNC(beginMultipartUpload, BeginMultipartUploadReq, BeginMultipartUploadRsp);
  FORWARD_RPC_FUNC(checkpointUploadPart, CheckpointUploadPartReq, CheckpointUploadPartRsp);
  FORWARD_RPC_FUNC(mutateMultipartUpload, MutateMultipartUploadReq, MutateMultipartUploadRsp);
  FORWARD_RPC_FUNC(publishOriginFileFromStaging, PublishOriginFileFromStagingReq, PublishOriginFileFromStagingRsp);
  FORWARD_RPC_FUNC(listUploadJobs, ListUploadJobsReq, ListUploadJobsRsp);
  FORWARD_RPC_FUNC(getUploadJob, GetUploadJobReq, GetUploadJobRsp);

#undef FORWARD_RPC_FUNC

 private:
  std::shared_ptr<MockMetaStubHolder> stub_;
};

}  // namespace hf3fs::meta
