#pragma once

#include "common/serde/ClientContext.h"
#include "common/serde/MessagePacket.h"
#include "common/utils/Coroutine.h"
#include "fbs/meta/Common.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta {

class IMetaServiceStub {
 public:
  using InterfaceType = IMetaServiceStub;

  virtual ~IMetaServiceStub() = default;

#define IMETA_STUB_METHOD(NAME, REQ, RESP)                             \
  virtual CoTryTask<RESP> NAME(const REQ &req,                         \
                               const net::UserRequestOptions &options, \
                               serde::Timestamp *timestamp = nullptr) = 0

  IMETA_STUB_METHOD(statFs, StatFsReq, StatFsRsp);
  IMETA_STUB_METHOD(stat, StatReq, StatRsp);
  IMETA_STUB_METHOD(create, CreateReq, CreateRsp);
  IMETA_STUB_METHOD(mkdirs, MkdirsReq, MkdirsRsp);
  IMETA_STUB_METHOD(symlink, SymlinkReq, SymlinkRsp);
  IMETA_STUB_METHOD(hardLink, HardLinkReq, HardLinkRsp);
  IMETA_STUB_METHOD(remove, RemoveReq, RemoveRsp);
  IMETA_STUB_METHOD(open, OpenReq, OpenRsp);
  IMETA_STUB_METHOD(sync, SyncReq, SyncRsp);
  IMETA_STUB_METHOD(close, CloseReq, CloseRsp);
  IMETA_STUB_METHOD(rename, RenameReq, RenameRsp);
  IMETA_STUB_METHOD(list, ListReq, ListRsp);
  IMETA_STUB_METHOD(truncate, TruncateReq, TruncateRsp);
  IMETA_STUB_METHOD(getRealPath, GetRealPathReq, GetRealPathRsp);
  IMETA_STUB_METHOD(setAttr, SetAttrReq, SetAttrRsp);
  IMETA_STUB_METHOD(pruneSession, PruneSessionReq, PruneSessionRsp);
  IMETA_STUB_METHOD(dropUserCache, DropUserCacheReq, DropUserCacheRsp);
  IMETA_STUB_METHOD(authenticate, AuthReq, AuthRsp);
  IMETA_STUB_METHOD(lockDirectory, LockDirectoryReq, LockDirectoryRsp);
  IMETA_STUB_METHOD(testRpc, TestRpcReq, TestRpcRsp);
  IMETA_STUB_METHOD(batchStat, BatchStatReq, BatchStatRsp);
  IMETA_STUB_METHOD(batchStatByPath, BatchStatByPathReq, BatchStatByPathRsp);
  IMETA_STUB_METHOD(importOriginFile, ImportOriginFileReq, ImportOriginFileRsp);
  IMETA_STUB_METHOD(batchImportOriginFiles, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  IMETA_STUB_METHOD(refreshOriginFile, RefreshOriginFileReq, RefreshOriginFileRsp);
  IMETA_STUB_METHOD(getFileReadPlan, GetFileReadPlanReq, GetFileReadPlanRsp);
  IMETA_STUB_METHOD(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  IMETA_STUB_METHOD(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  IMETA_STUB_METHOD(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  IMETA_STUB_METHOD(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
  IMETA_STUB_METHOD(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  IMETA_STUB_METHOD(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  IMETA_STUB_METHOD(getCacheStatus, GetCacheStatusReq, GetCacheStatusRsp);
  IMETA_STUB_METHOD(listCacheBlocks, ListCacheBlocksReq, ListCacheBlocksRsp);
  IMETA_STUB_METHOD(listRecoverableCachePermits, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  IMETA_STUB_METHOD(cancelQueuedAdmissions, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);
  IMETA_STUB_METHOD(updateCacheBlockAccess, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
  IMETA_STUB_METHOD(beginEvictCacheBlocks, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
  IMETA_STUB_METHOD(listEvictingCacheBlocks, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
  IMETA_STUB_METHOD(listReadyCacheBlocks, ListReadyCacheBlocksReq, ListReadyCacheBlocksRsp);
  IMETA_STUB_METHOD(reportCacheStorageEvents, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
  IMETA_STUB_METHOD(listCacheEventDeadLetters, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
  IMETA_STUB_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  IMETA_STUB_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
  IMETA_STUB_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  IMETA_STUB_METHOD(updatePrefetchJob, UpdatePrefetchJobReq, UpdatePrefetchJobRsp);
  IMETA_STUB_METHOD(appendPrefetchPlan, AppendPrefetchPlanReq, AppendPrefetchPlanRsp);
  IMETA_STUB_METHOD(listPrefetchPlan, ListPrefetchPlanReq, ListPrefetchPlanRsp);
  IMETA_STUB_METHOD(upsertCachePins, UpsertCachePinsReq, UpsertCachePinsRsp);
  IMETA_STUB_METHOD(removeCachePins, RemoveCachePinsReq, RemoveCachePinsRsp);
  IMETA_STUB_METHOD(listCachePinsByOwner, ListCachePinsByOwnerReq, ListCachePinsByOwnerRsp);
  IMETA_STUB_METHOD(queryCachePins, QueryCachePinsReq, QueryCachePinsRsp);
  IMETA_STUB_METHOD(updatePrefetchPlanEntries, UpdatePrefetchPlanEntriesReq, UpdatePrefetchPlanEntriesRsp);
  IMETA_STUB_METHOD(trackPrefetchReady, TrackPrefetchReadyReq, TrackPrefetchReadyRsp);
  IMETA_STUB_METHOD(advancePrefetchJobState, AdvancePrefetchJobStateReq, AdvancePrefetchJobStateRsp);
  IMETA_STUB_METHOD(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  IMETA_STUB_METHOD(convertActiveJobPins, ConvertActiveJobPinsReq, ConvertActiveJobPinsRsp);
  IMETA_STUB_METHOD(reconcileCacheBlocks, ReconcileCacheBlocksReq, ReconcileCacheBlocksRsp);
  IMETA_STUB_METHOD(listReconcileCacheBlocks, ListReconcileCacheBlocksReq, ListReconcileCacheBlocksRsp);
  IMETA_STUB_METHOD(recoverExpiredCacheLoads, RecoverExpiredCacheLoadsReq, RecoverExpiredCacheLoadsRsp);
  IMETA_STUB_METHOD(createWriteStaging, CreateWriteStagingReq, CreateWriteStagingRsp);
  IMETA_STUB_METHOD(renewWriteStagingLease, RenewWriteStagingLeaseReq, RenewWriteStagingLeaseRsp);
  IMETA_STUB_METHOD(sealWriteStaging, SealWriteStagingReq, SealWriteStagingRsp);
  IMETA_STUB_METHOD(recoverExpiredWriteStaging, RecoverExpiredWriteStagingReq, RecoverExpiredWriteStagingRsp);
  IMETA_STUB_METHOD(beginMultipartUpload, BeginMultipartUploadReq, BeginMultipartUploadRsp);
  IMETA_STUB_METHOD(checkpointUploadPart, CheckpointUploadPartReq, CheckpointUploadPartRsp);
  IMETA_STUB_METHOD(mutateMultipartUpload, MutateMultipartUploadReq, MutateMultipartUploadRsp);
  IMETA_STUB_METHOD(publishOriginFileFromStaging, PublishOriginFileFromStagingReq, PublishOriginFileFromStagingRsp);
  IMETA_STUB_METHOD(listUploadJobs, ListUploadJobsReq, ListUploadJobsRsp);
  IMETA_STUB_METHOD(getUploadJob, GetUploadJobReq, GetUploadJobRsp);
  IMETA_STUB_METHOD(adminListUploadJobs, AdminListUploadJobsReq, ListUploadJobsRsp);
  IMETA_STUB_METHOD(adminMutateUploadJob, AdminMutateUploadJobReq, AdminMutateUploadJobRsp);

#undef IMETA_STUB_METHOD
};

}  // namespace hf3fs::meta
