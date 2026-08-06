#pragma once

#include "common/serde/CallContext.h"
#include "common/serde/Service.h"
#include "common/utils/Result.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta {

class MockMetaService : public serde::ServiceWrapper<MockMetaService, MetaSerde> {
 public:
  virtual ~MockMetaService() = default;

#define META_MOCK_SERVICE_METHOD(NAME, REQ, RESP)                         \
  virtual CoTryTask<RESP> NAME(serde::CallContext &ctx, const REQ &req) { \
    co_return makeError(StatusCode::kNotImplemented);                     \
  }

  META_MOCK_SERVICE_METHOD(statFs, StatFsReq, StatFsRsp);
  META_MOCK_SERVICE_METHOD(stat, StatReq, StatRsp);
  META_MOCK_SERVICE_METHOD(create, CreateReq, CreateRsp);
  META_MOCK_SERVICE_METHOD(mkdirs, MkdirsReq, MkdirsRsp);
  META_MOCK_SERVICE_METHOD(symlink, SymlinkReq, SymlinkRsp);
  META_MOCK_SERVICE_METHOD(hardLink, HardLinkReq, HardLinkRsp);
  META_MOCK_SERVICE_METHOD(remove, RemoveReq, RemoveRsp);
  META_MOCK_SERVICE_METHOD(open, OpenReq, OpenRsp);
  META_MOCK_SERVICE_METHOD(sync, SyncReq, SyncRsp);
  META_MOCK_SERVICE_METHOD(close, CloseReq, CloseRsp);
  META_MOCK_SERVICE_METHOD(rename, RenameReq, RenameRsp);
  META_MOCK_SERVICE_METHOD(list, ListReq, ListRsp);
  META_MOCK_SERVICE_METHOD(truncate, TruncateReq, TruncateRsp);
  META_MOCK_SERVICE_METHOD(getRealPath, GetRealPathReq, GetRealPathRsp);
  META_MOCK_SERVICE_METHOD(setAttr, SetAttrReq, SetAttrRsp);
  META_MOCK_SERVICE_METHOD(pruneSession, PruneSessionReq, PruneSessionRsp);
  META_MOCK_SERVICE_METHOD(dropUserCache, DropUserCacheReq, DropUserCacheRsp);
  META_MOCK_SERVICE_METHOD(authenticate, AuthReq, AuthRsp);
  META_MOCK_SERVICE_METHOD(lockDirectory, LockDirectoryReq, LockDirectoryRsp);
  META_MOCK_SERVICE_METHOD(testRpc, TestRpcReq, TestRpcRsp);
  META_MOCK_SERVICE_METHOD(batchStat, BatchStatReq, BatchStatRsp);
  META_MOCK_SERVICE_METHOD(batchStatByPath, BatchStatByPathReq, BatchStatByPathRsp);
  META_MOCK_SERVICE_METHOD(importOriginFile, ImportOriginFileReq, ImportOriginFileRsp);
  META_MOCK_SERVICE_METHOD(batchImportOriginFiles, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  META_MOCK_SERVICE_METHOD(refreshOriginFile, RefreshOriginFileReq, RefreshOriginFileRsp);
  META_MOCK_SERVICE_METHOD(getFileReadPlan, GetFileReadPlanReq, GetFileReadPlanRsp);
  META_MOCK_SERVICE_METHOD(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  META_MOCK_SERVICE_METHOD(listCacheBlocks, ListCacheBlocksReq, ListCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(listRecoverableCachePermits, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  META_MOCK_SERVICE_METHOD(cancelQueuedAdmissions, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);
  META_MOCK_SERVICE_METHOD(listReadyCacheBlocks, ListReadyCacheBlocksReq, ListReadyCacheBlocksRsp);
  META_MOCK_SERVICE_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  META_MOCK_SERVICE_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  META_MOCK_SERVICE_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  META_MOCK_SERVICE_METHOD(updatePrefetchJob, UpdatePrefetchJobReq, UpdatePrefetchJobRsp);
  META_MOCK_SERVICE_METHOD(appendPrefetchPlan, AppendPrefetchPlanReq, AppendPrefetchPlanRsp);
  META_MOCK_SERVICE_METHOD(listPrefetchPlan, ListPrefetchPlanReq, ListPrefetchPlanRsp);
  META_MOCK_SERVICE_METHOD(upsertCachePins, UpsertCachePinsReq, UpsertCachePinsRsp);
  META_MOCK_SERVICE_METHOD(removeCachePins, RemoveCachePinsReq, RemoveCachePinsRsp);
  META_MOCK_SERVICE_METHOD(listCachePinsByOwner, ListCachePinsByOwnerReq, ListCachePinsByOwnerRsp);
  META_MOCK_SERVICE_METHOD(queryCachePins, QueryCachePinsReq, QueryCachePinsRsp);
  META_MOCK_SERVICE_METHOD(updatePrefetchPlanEntries, UpdatePrefetchPlanEntriesReq, UpdatePrefetchPlanEntriesRsp);
  META_MOCK_SERVICE_METHOD(trackPrefetchReady, TrackPrefetchReadyReq, TrackPrefetchReadyRsp);
  META_MOCK_SERVICE_METHOD(advancePrefetchJobState, AdvancePrefetchJobStateReq, AdvancePrefetchJobStateRsp);
  META_MOCK_SERVICE_METHOD(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  META_MOCK_SERVICE_METHOD(convertActiveJobPins, ConvertActiveJobPinsReq, ConvertActiveJobPinsRsp);
};
}  // namespace hf3fs::meta
