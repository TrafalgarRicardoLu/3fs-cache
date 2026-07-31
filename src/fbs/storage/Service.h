#pragma once

#include "common/serde/Service.h"
#include "fbs/storage/Cache.h"
#include "fbs/storage/Common.h"

namespace hf3fs::storage {

SERDE_SERVICE(StorageSerde, 3) {
  SERDE_SERVICE_METHOD(batchRead, 1, BatchReadReq, BatchReadRsp);
  SERDE_SERVICE_METHOD(write, 2, WriteReq, WriteRsp);
  SERDE_SERVICE_METHOD(update, 3, UpdateReq, UpdateRsp);
  SERDE_SERVICE_METHOD(queryLastChunk, 5, QueryLastChunkReq, QueryLastChunkRsp);
  SERDE_SERVICE_METHOD(truncateChunks, 6, TruncateChunksReq, TruncateChunksRsp);
  SERDE_SERVICE_METHOD(removeChunks, 7, RemoveChunksReq, RemoveChunksRsp);
  SERDE_SERVICE_METHOD(syncStart, 8, SyncStartReq, TargetSyncInfo);
  SERDE_SERVICE_METHOD(syncDone, 9, SyncDoneReq, SyncDoneRsp);
  SERDE_SERVICE_METHOD(spaceInfo, 10, SpaceInfoReq, SpaceInfoRsp);
  SERDE_SERVICE_METHOD(createTarget, 11, CreateTargetReq, CreateTargetRsp);
  SERDE_SERVICE_METHOD(queryChunk, 12, QueryChunkReq, QueryChunkRsp);
  SERDE_SERVICE_METHOD(getAllChunkMetadata, 13, GetAllChunkMetadataReq, GetAllChunkMetadataRsp);
  SERDE_SERVICE_METHOD(offlineTarget, 16, OfflineTargetReq, OfflineTargetRsp);
  SERDE_SERVICE_METHOD(removeTarget, 17, RemoveTargetReq, RemoveTargetRsp);
  SERDE_SERVICE_METHOD(replaceCacheChunks, 18, ReplaceCacheChunksReq, ReplaceCacheChunksRsp);
  SERDE_SERVICE_METHOD(retireCacheChunkGenerations, 19, RetireCacheChunkGenerationsReq, RetireCacheChunkGenerationsRsp);
  SERDE_SERVICE_METHOD(queryCacheChunkGenerations, 20, QueryCacheChunkGenerationsReq, QueryCacheChunkGenerationsRsp);
  SERDE_SERVICE_METHOD(queryCacheSpace, 21, QueryCacheSpaceReq, QueryCacheSpaceRsp);
  SERDE_SERVICE_METHOD(prepareCachePermits, 22, PrepareCachePermitsReq, PrepareCachePermitsRsp);
  SERDE_SERVICE_METHOD(renewCachePermits, 23, RenewCachePermitsReq, RenewCachePermitsRsp);
  SERDE_SERVICE_METHOD(releaseCachePermits, 24, ReleaseCachePermitsReq, ReleaseCachePermitsRsp);
  SERDE_SERVICE_METHOD(queryCachePermits, 25, QueryCachePermitsReq, QueryCachePermitsRsp);
  SERDE_SERVICE_METHOD(retireCacheReplicas, 26, RetireCacheReplicasReq, RetireCacheReplicasRsp);
  SERDE_SERVICE_METHOD(coordinateCacheRetires, 27, CoordinateCacheRetiresReq, CoordinateCacheRetiresRsp);
};

}  // namespace hf3fs::storage
