#pragma once

#include "common/serde/CallContext.h"
#include "fbs/meta/Service.h"
#include "meta/service/MetaOperator.h"

namespace hf3fs::meta::server {

class MetaSerdeService : public serde::ServiceWrapper<MetaSerdeService, MetaSerde> {
 public:
  MetaSerdeService(MetaOperator &meta)
      : meta_(meta) {}

#define META_SERVICE_METHOD(NAME, REQ, RESP) \
  CoTryTask<RESP> NAME(serde::CallContext &, const REQ &req) { return meta_.NAME(req); }

  META_SERVICE_METHOD(statFs, StatFsReq, StatFsRsp);
  META_SERVICE_METHOD(stat, StatReq, StatRsp);
  META_SERVICE_METHOD(create, CreateReq, CreateRsp);
  META_SERVICE_METHOD(mkdirs, MkdirsReq, MkdirsRsp);
  META_SERVICE_METHOD(symlink, SymlinkReq, SymlinkRsp);
  META_SERVICE_METHOD(hardLink, HardLinkReq, HardLinkRsp);
  META_SERVICE_METHOD(remove, RemoveReq, RemoveRsp);
  META_SERVICE_METHOD(open, OpenReq, OpenRsp);
  META_SERVICE_METHOD(sync, SyncReq, SyncRsp);
  META_SERVICE_METHOD(close, CloseReq, CloseRsp);
  META_SERVICE_METHOD(rename, RenameReq, RenameRsp);
  META_SERVICE_METHOD(list, ListReq, ListRsp);
  META_SERVICE_METHOD(truncate, TruncateReq, TruncateRsp);
  META_SERVICE_METHOD(getRealPath, GetRealPathReq, GetRealPathRsp);
  META_SERVICE_METHOD(setAttr, SetAttrReq, SetAttrRsp);
  META_SERVICE_METHOD(pruneSession, PruneSessionReq, PruneSessionRsp);
  META_SERVICE_METHOD(dropUserCache, DropUserCacheReq, DropUserCacheRsp);
  META_SERVICE_METHOD(authenticate, AuthReq, AuthRsp);
  META_SERVICE_METHOD(lockDirectory, LockDirectoryReq, LockDirectoryRsp);
  META_SERVICE_METHOD(testRpc, TestRpcReq, TestRpcRsp);
  META_SERVICE_METHOD(batchStat, BatchStatReq, BatchStatRsp);
  META_SERVICE_METHOD(batchStatByPath, BatchStatByPathReq, BatchStatByPathRsp);
  META_SERVICE_METHOD(importOriginFile, ImportOriginFileReq, ImportOriginFileRsp);
  META_SERVICE_METHOD(batchImportOriginFiles, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  META_SERVICE_METHOD(refreshOriginFile, RefreshOriginFileReq, RefreshOriginFileRsp);
  META_SERVICE_METHOD(getFileReadPlan, GetFileReadPlanReq, GetFileReadPlanRsp);
  META_SERVICE_METHOD(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  META_SERVICE_METHOD(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  META_SERVICE_METHOD(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  META_SERVICE_METHOD(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
  META_SERVICE_METHOD(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  META_SERVICE_METHOD(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  META_SERVICE_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  META_SERVICE_METHOD(listCacheBlocks, ListCacheBlocksReq, ListCacheBlocksRsp);
  META_SERVICE_METHOD(listRecoverableCachePermits, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  META_SERVICE_METHOD(cancelQueuedAdmissions, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);
  META_SERVICE_METHOD(listReadyCacheBlocks, ListReadyCacheBlocksReq, ListReadyCacheBlocksRsp);
  META_SERVICE_METHOD(updateCacheBlockAccess, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
  META_SERVICE_METHOD(beginEvictCacheBlocks, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
  META_SERVICE_METHOD(listEvictingCacheBlocks, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
  META_SERVICE_METHOD(reportCacheStorageEvents, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
  META_SERVICE_METHOD(listCacheEventDeadLetters, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
  META_SERVICE_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  META_SERVICE_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  META_SERVICE_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  META_SERVICE_METHOD(updatePrefetchJob, UpdatePrefetchJobReq, UpdatePrefetchJobRsp);
  META_SERVICE_METHOD(appendPrefetchPlan, AppendPrefetchPlanReq, AppendPrefetchPlanRsp);
  META_SERVICE_METHOD(listPrefetchPlan, ListPrefetchPlanReq, ListPrefetchPlanRsp);
  META_SERVICE_METHOD(updatePrefetchPlanEntries, UpdatePrefetchPlanEntriesReq, UpdatePrefetchPlanEntriesRsp);
  META_SERVICE_METHOD(trackPrefetchReady, TrackPrefetchReadyReq, TrackPrefetchReadyRsp);
  META_SERVICE_METHOD(advancePrefetchJobState, AdvancePrefetchJobStateReq, AdvancePrefetchJobStateRsp);
  META_SERVICE_METHOD(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  META_SERVICE_METHOD(upsertCachePins, UpsertCachePinsReq, UpsertCachePinsRsp);
  META_SERVICE_METHOD(removeCachePins, RemoveCachePinsReq, RemoveCachePinsRsp);
  META_SERVICE_METHOD(listCachePinsByOwner, ListCachePinsByOwnerReq, ListCachePinsByOwnerRsp);
  META_SERVICE_METHOD(queryCachePins, QueryCachePinsReq, QueryCachePinsRsp);
  META_SERVICE_METHOD(convertActiveJobPins, ConvertActiveJobPinsReq, ConvertActiveJobPinsRsp);
  META_SERVICE_METHOD(reconcileCacheBlocks, ReconcileCacheBlocksReq, ReconcileCacheBlocksRsp);
  META_SERVICE_METHOD(listReconcileCacheBlocks, ListReconcileCacheBlocksReq, ListReconcileCacheBlocksRsp);
  META_SERVICE_METHOD(recoverExpiredCacheLoads, RecoverExpiredCacheLoadsReq, RecoverExpiredCacheLoadsRsp);
  META_SERVICE_METHOD(createWriteStaging, CreateWriteStagingReq, CreateWriteStagingRsp);
  META_SERVICE_METHOD(renewWriteStagingLease, RenewWriteStagingLeaseReq, RenewWriteStagingLeaseRsp);
  META_SERVICE_METHOD(sealWriteStaging, SealWriteStagingReq, SealWriteStagingRsp);
  META_SERVICE_METHOD(recoverExpiredWriteStaging, RecoverExpiredWriteStagingReq, RecoverExpiredWriteStagingRsp);
  META_SERVICE_METHOD(beginMultipartUpload, BeginMultipartUploadReq, BeginMultipartUploadRsp);
  META_SERVICE_METHOD(checkpointUploadPart, CheckpointUploadPartReq, CheckpointUploadPartRsp);
  META_SERVICE_METHOD(mutateMultipartUpload, MutateMultipartUploadReq, MutateMultipartUploadRsp);
  META_SERVICE_METHOD(publishOriginFileFromStaging, PublishOriginFileFromStagingReq, PublishOriginFileFromStagingRsp);
  META_SERVICE_METHOD(listUploadJobs, ListUploadJobsReq, ListUploadJobsRsp);
  META_SERVICE_METHOD(getUploadJob, GetUploadJobReq, GetUploadJobRsp);
  META_SERVICE_METHOD(adminListUploadJobs, AdminListUploadJobsReq, ListUploadJobsRsp);
  META_SERVICE_METHOD(adminMutateUploadJob, AdminMutateUploadJobReq, AdminMutateUploadJobRsp);
#undef META_SERVICE_METHOD

 private:
  MetaOperator &meta_;
};

}  // namespace hf3fs::meta::server
