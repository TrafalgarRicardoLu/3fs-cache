#pragma once

#include "common/serde/CallContext.h"
#include "fbs/storage/Service.h"
#include "storage/service/StorageOperator.h"

namespace hf3fs::storage {

class StorageService : public serde::ServiceWrapper<StorageService, storage::StorageSerde> {
 public:
  StorageService(StorageOperator &storageOperator)
      : storageOperator_(storageOperator) {}

  CoTryTask<BatchReadRsp> batchRead(serde::CallContext &ctx, const BatchReadReq &req) {
    reportReadQueueLatency(ctx);
    if (UNLIKELY(req.payloads.empty())) co_return BatchReadRsp{.tag = req.tag};
    ServiceRequestContext requestCtx{"batchRead", req.tag, req.retryCount, req.userInfo, req.debugFlags};
    co_return co_await storageOperator_.batchRead(requestCtx, req, ctx);
  }

  CoTryTask<WriteRsp> write(serde::CallContext &ctx, const WriteReq &req) {
    reportUpdateQueueLatency(ctx);
    ServiceRequestContext requestCtx{"write", req.tag, req.retryCount, req.userInfo, req.debugFlags};
    co_return co_await storageOperator_.write(requestCtx, req, ctx.transport()->ibSocket());
  }

  CoTryTask<UpdateRsp> update(serde::CallContext &ctx, const UpdateReq &req) {
    reportUpdateQueueLatency(ctx);
    ServiceRequestContext requestCtx{"update", req.tag, req.retryCount, req.userInfo, req.debugFlags};
    co_return co_await storageOperator_.update(requestCtx, req, ctx.transport()->ibSocket());
  }

  CoTryTask<QueryLastChunkRsp> queryLastChunk(serde::CallContext &ctx, const QueryLastChunkReq &req) {
    reportDefaultQueueLatency(ctx);
    if (UNLIKELY(req.payloads.empty())) co_return QueryLastChunkRsp{};
    ServiceRequestContext requestCtx{"queryLastChunk", req.tag, req.retryCount, req.userInfo, req.debugFlags};
    co_return co_await storageOperator_.queryLastChunk(requestCtx, req);
  }

  CoTryTask<TruncateChunksRsp> truncateChunks(serde::CallContext &ctx, const TruncateChunksReq &req) {
    reportDefaultQueueLatency(ctx);
    if (UNLIKELY(req.payloads.empty())) co_return TruncateChunksRsp{};
    ServiceRequestContext requestCtx{"truncateChunks",
                                     req.payloads.front().tag,
                                     req.payloads.front().retryCount,
                                     req.userInfo,
                                     req.debugFlags};
    co_return co_await storageOperator_.truncateChunks(requestCtx, req);
  }

  CoTryTask<RemoveChunksRsp> removeChunks(serde::CallContext &ctx, const RemoveChunksReq &req) {
    reportDefaultQueueLatency(ctx);
    if (UNLIKELY(req.payloads.empty())) co_return RemoveChunksRsp{};
    ServiceRequestContext requestCtx{"removeChunks",
                                     req.payloads.front().tag,
                                     req.payloads.front().retryCount,
                                     req.userInfo,
                                     req.debugFlags};
    co_return co_await storageOperator_.removeChunks(requestCtx, req);
  }

  CoTryTask<TargetSyncInfo> syncStart(serde::CallContext &ctx, const SyncStartReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.syncStart(req);
  }

  CoTryTask<SyncDoneRsp> syncDone(serde::CallContext &ctx, const SyncDoneReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.syncDone(req);
  }

  CoTryTask<SpaceInfoRsp> spaceInfo(serde::CallContext &ctx, const SpaceInfoReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.spaceInfo(req);
  }

  CoTryTask<CreateTargetRsp> createTarget(serde::CallContext &ctx, const CreateTargetReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.createTarget(req);
  }

  CoTryTask<OfflineTargetRsp> offlineTarget(serde::CallContext &ctx, const OfflineTargetReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.offlineTarget(req);
  }

  CoTryTask<RemoveTargetRsp> removeTarget(serde::CallContext &ctx, const RemoveTargetReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.removeTarget(req);
  }

  CoTryTask<QueryChunkRsp> queryChunk(serde::CallContext &ctx, const QueryChunkReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.queryChunk(req);
  }

  CoTryTask<GetAllChunkMetadataRsp> getAllChunkMetadata(serde::CallContext &ctx, const GetAllChunkMetadataReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.getAllChunkMetadata(req);
  }

  CoTryTask<ReplaceCacheChunksRsp> replaceCacheChunks(serde::CallContext &ctx, const ReplaceCacheChunksReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.replaceCacheChunks(req);
  }

  CoTryTask<RetireCacheChunkGenerationsRsp> retireCacheChunkGenerations(serde::CallContext &ctx,
                                                                        const RetireCacheChunkGenerationsReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.retireCacheChunkGenerations(req);
  }

  CoTryTask<QueryCacheChunkGenerationsRsp> queryCacheChunkGenerations(serde::CallContext &ctx,
                                                                      const QueryCacheChunkGenerationsReq &req) {
    reportDefaultQueueLatency(ctx);
    return storageOperator_.queryCacheChunkGenerations(req);
  }

#define PHASE2_STORAGE_SERVICE_METHOD(NAME, REQ, RSP)            \
  CoTryTask<RSP> NAME(serde::CallContext &ctx, const REQ &req) { \
    reportDefaultQueueLatency(ctx);                              \
    return storageOperator_.NAME(req);                           \
  }
  PHASE2_STORAGE_SERVICE_METHOD(queryCacheSpace, QueryCacheSpaceReq, QueryCacheSpaceRsp);
  PHASE2_STORAGE_SERVICE_METHOD(prepareCachePermits, PrepareCachePermitsReq, PrepareCachePermitsRsp);
  PHASE2_STORAGE_SERVICE_METHOD(renewCachePermits, RenewCachePermitsReq, RenewCachePermitsRsp);
  PHASE2_STORAGE_SERVICE_METHOD(releaseCachePermits, ReleaseCachePermitsReq, ReleaseCachePermitsRsp);
  PHASE2_STORAGE_SERVICE_METHOD(queryCachePermits, QueryCachePermitsReq, QueryCachePermitsRsp);
  PHASE2_STORAGE_SERVICE_METHOD(retireCacheReplicas, RetireCacheReplicasReq, RetireCacheReplicasRsp);
  PHASE2_STORAGE_SERVICE_METHOD(coordinateCacheRetires, CoordinateCacheRetiresReq, CoordinateCacheRetiresRsp);
#undef PHASE2_STORAGE_SERVICE_METHOD

 private:
  void reportReadQueueLatency(serde::CallContext &ctx);
  void reportUpdateQueueLatency(serde::CallContext &ctx);
  void reportDefaultQueueLatency(serde::CallContext &ctx);

 private:
  StorageOperator &storageOperator_;
};

}  // namespace hf3fs::storage
