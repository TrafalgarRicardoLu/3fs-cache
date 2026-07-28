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

#undef IMETA_STUB_METHOD
};

}  // namespace hf3fs::meta
