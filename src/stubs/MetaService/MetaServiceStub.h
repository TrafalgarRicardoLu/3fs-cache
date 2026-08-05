#pragma once

#include "common/serde/ClientContext.h"
#include "fbs/meta/Service.h"
#include "stubs/MetaService/IMetaServiceStub.h"

namespace hf3fs::meta {

template <typename Ctx>
class MetaServiceStub : public IMetaServiceStub {
 public:
  explicit MetaServiceStub(Ctx ctx)
      : context_(std::move(ctx)) {}

#define META_STUB_METHOD(NAME, REQ, RESP)                                                                             \
  CoTryTask<RESP> NAME(const REQ &req, const net::UserRequestOptions &options, serde::Timestamp *timestamp = nullptr) \
      override

  META_STUB_METHOD(statFs, StatFsReq, StatFsRsp);
  META_STUB_METHOD(stat, StatReq, StatRsp);
  META_STUB_METHOD(create, CreateReq, CreateRsp);
  META_STUB_METHOD(mkdirs, MkdirsReq, MkdirsRsp);
  META_STUB_METHOD(symlink, SymlinkReq, SymlinkRsp);
  META_STUB_METHOD(hardLink, HardLinkReq, HardLinkRsp);
  META_STUB_METHOD(remove, RemoveReq, RemoveRsp);
  META_STUB_METHOD(open, OpenReq, OpenRsp);
  META_STUB_METHOD(sync, SyncReq, SyncRsp);
  META_STUB_METHOD(close, CloseReq, CloseRsp);
  META_STUB_METHOD(rename, RenameReq, RenameRsp);
  META_STUB_METHOD(list, ListReq, ListRsp);
  META_STUB_METHOD(truncate, TruncateReq, TruncateRsp);
  META_STUB_METHOD(getRealPath, GetRealPathReq, GetRealPathRsp);
  META_STUB_METHOD(setAttr, SetAttrReq, SetAttrRsp);
  META_STUB_METHOD(pruneSession, PruneSessionReq, PruneSessionRsp);
  META_STUB_METHOD(dropUserCache, DropUserCacheReq, DropUserCacheRsp);
  META_STUB_METHOD(authenticate, AuthReq, AuthRsp);
  META_STUB_METHOD(lockDirectory, LockDirectoryReq, LockDirectoryRsp);
  META_STUB_METHOD(testRpc, TestRpcReq, TestRpcRsp);
  META_STUB_METHOD(batchStat, BatchStatReq, BatchStatRsp);
  META_STUB_METHOD(batchStatByPath, BatchStatByPathReq, BatchStatByPathRsp);
  META_STUB_METHOD(importOriginFile, ImportOriginFileReq, ImportOriginFileRsp);
  META_STUB_METHOD(batchImportOriginFiles, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  META_STUB_METHOD(refreshOriginFile, RefreshOriginFileReq, RefreshOriginFileRsp);
  META_STUB_METHOD(getFileReadPlan, GetFileReadPlanReq, GetFileReadPlanRsp);
  META_STUB_METHOD(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  META_STUB_METHOD(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  META_STUB_METHOD(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  META_STUB_METHOD(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
  META_STUB_METHOD(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  META_STUB_METHOD(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  META_STUB_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  META_STUB_METHOD(listCacheBlocks, ListCacheBlocksReq, ListCacheBlocksRsp);
  META_STUB_METHOD(listRecoverableCachePermits, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  META_STUB_METHOD(cancelQueuedAdmissions, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);
  META_STUB_METHOD(updateCacheBlockAccess, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
  META_STUB_METHOD(beginEvictCacheBlocks, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
  META_STUB_METHOD(listEvictingCacheBlocks, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
  META_STUB_METHOD(listReadyCacheBlocks, ListReadyCacheBlocksReq, ListReadyCacheBlocksRsp);
  META_STUB_METHOD(reportCacheStorageEvents, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
  META_STUB_METHOD(listCacheEventDeadLetters, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
  META_STUB_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  META_STUB_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  META_STUB_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  META_STUB_METHOD(updatePrefetchJob, UpdatePrefetchJobReq, UpdatePrefetchJobRsp);
  META_STUB_METHOD(appendPrefetchPlan, AppendPrefetchPlanReq, AppendPrefetchPlanRsp);
  META_STUB_METHOD(listPrefetchPlan, ListPrefetchPlanReq, ListPrefetchPlanRsp);
  META_STUB_METHOD(upsertCachePins, UpsertCachePinsReq, UpsertCachePinsRsp);
  META_STUB_METHOD(removeCachePins, RemoveCachePinsReq, RemoveCachePinsRsp);
  META_STUB_METHOD(listCachePinsByOwner, ListCachePinsByOwnerReq, ListCachePinsByOwnerRsp);
  META_STUB_METHOD(queryCachePins, QueryCachePinsReq, QueryCachePinsRsp);
  META_STUB_METHOD(updatePrefetchPlanEntries, UpdatePrefetchPlanEntriesReq, UpdatePrefetchPlanEntriesRsp);
  META_STUB_METHOD(trackPrefetchReady, TrackPrefetchReadyReq, TrackPrefetchReadyRsp);

#undef META_STUB_METHOD

 private:
  Ctx context_;
};

}  // namespace hf3fs::meta
