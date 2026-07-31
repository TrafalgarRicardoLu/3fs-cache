#pragma once

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "fbs/cache/Common.h"
#include "fbs/core/user/User.h"
#include "fbs/storage/Common.h"

namespace hf3fs::storage {

inline constexpr size_t kMaxCacheStorageBatchItems = 1000;

struct CacheChunkKey {
  SERDE_STRUCT_FIELD(vChainId, VersionedChainId{});
  SERDE_STRUCT_FIELD(chunkId, ChunkId{});

 public:
  Result<Void> valid() const {
    if (vChainId.chainId == ChainId{}) return makeError(StatusCode::kInvalidArg, "chainId not set");
    return Void{};
  }
};

struct ReplaceCacheChunkItem {
  SERDE_STRUCT_FIELD(key, CacheChunkKey{});
  SERDE_STRUCT_FIELD(cacheGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());
  SERDE_STRUCT_FIELD(data, std::vector<uint8_t>{});
  SERDE_STRUCT_FIELD(chunkSize, uint32_t{});
  SERDE_STRUCT_FIELD(checksumType, ChecksumType::NONE);

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (operationId == Uuid::zero()) return makeError(StatusCode::kInvalidArg, "operationId not set");
    if (cacheGeneration.toUnderType() == 0) return makeError(StatusCode::kInvalidArg, "cacheGeneration not set");
    if (data.empty() || chunkSize < data.size()) return makeError(StatusCode::kInvalidArg, "invalid cache chunk size");
    if (checksumType == ChecksumType::NONE) return makeError(StatusCode::kInvalidArg, "checksumType not set");
    return Void{};
  }
};

struct ReplaceCacheChunksReq {
  SERDE_STRUCT_FIELD(userInfo, flat::UserInfo{});
  SERDE_STRUCT_FIELD(items, std::vector<ReplaceCacheChunkItem>{});

 public:
  Result<Void> valid() const {
    if (items.size() > kMaxCacheStorageBatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many cache chunks");
    return Void{};
  }
};

struct CacheChunkGenerationInfo {
  SERDE_STRUCT_FIELD(cacheGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(retired, false);
  SERDE_STRUCT_FIELD(length, uint64_t{0});
  SERDE_STRUCT_FIELD(checksum, ChecksumInfo{});
};

struct ReplaceCacheChunksRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheChunkGenerationInfo>>{});
};

struct RetireCacheChunkItem {
  SERDE_STRUCT_FIELD(key, CacheChunkKey{});
  SERDE_STRUCT_FIELD(expectedGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (operationId == Uuid::zero()) return makeError(StatusCode::kInvalidArg, "operationId not set");
    if (expectedGeneration.toUnderType() == 0) return makeError(StatusCode::kInvalidArg, "expectedGeneration not set");
    return Void{};
  }
};

struct RetireCacheChunkGenerationsReq {
  SERDE_STRUCT_FIELD(userInfo, flat::UserInfo{});
  SERDE_STRUCT_FIELD(items, std::vector<RetireCacheChunkItem>{});

 public:
  Result<Void> valid() const {
    if (items.size() > kMaxCacheStorageBatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many cache chunks");
    return Void{};
  }
};

struct RetireCacheChunkGenerationsRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheChunkGenerationInfo>>{});
};

struct QueryCacheChunkGenerationsReq {
  SERDE_STRUCT_FIELD(userInfo, flat::UserInfo{});
  SERDE_STRUCT_FIELD(keys, std::vector<CacheChunkKey>{});

 public:
  Result<Void> valid() const {
    if (keys.size() > kMaxCacheStorageBatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many cache chunks");
    return Void{};
  }
};

struct QueryCacheChunkGenerationsRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheChunkGenerationInfo>>{});
};

struct CacheSpaceInfo {
  SERDE_STRUCT_FIELD(physicalDiskId, PhysicalDiskId{});
  SERDE_STRUCT_FIELD(role, StorageRole::INVALID);
  SERDE_STRUCT_FIELD(targets, std::vector<TargetId>{});
  SERDE_STRUCT_FIELD(capacityBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(physicalUsedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(allocatableBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(reservedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(enforcedHighWatermark, double{0});
  SERDE_STRUCT_FIELD(sampledAtNs, uint64_t{0});
};

struct CacheFootprintQuery {
  SERDE_STRUCT_FIELD(targetId, TargetId{});
  SERDE_STRUCT_FIELD(chunkSize, uint32_t{0});
  SERDE_STRUCT_FIELD(payloadLength, uint64_t{0});

 public:
  Result<Void> valid() const {
    if (targetId == TargetId{} || chunkSize == 0 || payloadLength == 0 || payloadLength > chunkSize)
      return makeError(StatusCode::kInvalidArg, "invalid cache footprint query");
    return Void{};
  }
};

struct CacheFootprintInfo {
  SERDE_STRUCT_FIELD(targetId, TargetId{});
  SERDE_STRUCT_FIELD(physicalDiskId, PhysicalDiskId{});
  SERDE_STRUCT_FIELD(footprintBytes, uint64_t{0});
};

struct QueryCacheSpaceReq {
  SERDE_STRUCT_FIELD(targetIds, std::vector<TargetId>{});
  SERDE_STRUCT_FIELD(footprints, std::vector<CacheFootprintQuery>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (targetIds.size() > cache::kMaxPhase2BatchItems || footprints.size() > cache::kMaxPhase2BatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many cache space queries");
    for (auto targetId : targetIds)
      if (targetId == TargetId{}) return makeError(StatusCode::kInvalidArg, "targetId not set");
    for (const auto &footprint : footprints) RETURN_ON_ERROR(footprint.valid());
    return Void{};
  }
};
struct QueryCacheSpaceRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheSpaceInfo>>{});
  SERDE_STRUCT_FIELD(footprintResults, std::vector<Result<CacheFootprintInfo>>{});
};

struct CachePermitRequestItem {
  SERDE_STRUCT_FIELD(permit, PermitIdentity{});
  SERDE_STRUCT_FIELD(expiresAtNs, uint64_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(permit.valid());
    if (expiresAtNs == 0) return makeError(StatusCode::kInvalidArg, "permit expiry not set");
    return Void{};
  }
};
struct CachePermitResult {
  SERDE_STRUCT_FIELD(permit, PermitIdentity{});
  SERDE_STRUCT_FIELD(state, cache::CachePermitState::INVALID);
  SERDE_STRUCT_FIELD(expiresAtNs, uint64_t{0});
};

#define STORAGE_PHASE2_BOUNDED_REQ(NAME, FIELD, ITEM)                           \
  struct NAME##Req {                                                            \
    SERDE_STRUCT_FIELD(userInfo, flat::UserInfo{});                             \
    SERDE_STRUCT_FIELD(FIELD, std::vector<ITEM>{});                             \
    SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});                      \
                                                                                \
   public:                                                                      \
    Result<Void> valid() const {                                                \
      if (FIELD.size() > cache::kMaxPhase2BatchItems)                           \
        return makeError(CacheCode::kRequestTooLarge, "cache batch too large"); \
      for (const auto &item : FIELD) RETURN_ON_ERROR(item.valid());             \
      return Void{};                                                            \
    }                                                                           \
  }

STORAGE_PHASE2_BOUNDED_REQ(PrepareCachePermits, items, CachePermitRequestItem);
STORAGE_PHASE2_BOUNDED_REQ(RenewCachePermits, items, CachePermitRequestItem);
STORAGE_PHASE2_BOUNDED_REQ(ReleaseCachePermits, permits, PermitIdentity);
STORAGE_PHASE2_BOUNDED_REQ(QueryCachePermits, permits, PermitIdentity);
struct PrepareCachePermitsRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CachePermitResult>>{});
};
struct RenewCachePermitsRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CachePermitResult>>{});
};
struct ReleaseCachePermitsRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<Void>>{});
};
struct QueryCachePermitsRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CachePermitResult>>{});
};

struct RetireCacheReplicaItem {
  SERDE_STRUCT_FIELD(key, CacheChunkKey{});
  SERDE_STRUCT_FIELD(expectedGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(placement, PlacementIdentity{});
  SERDE_STRUCT_FIELD(evictionEpoch, cache::EvictionEpoch{});
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    RETURN_ON_ERROR(placement.valid());
    if (expectedGeneration == cache::CacheGeneration{} || evictionEpoch == cache::EvictionEpoch{} ||
        operationId == Uuid::zero())
      return makeError(StatusCode::kInvalidArg, "invalid replica retire identity");
    return Void{};
  }
};
STORAGE_PHASE2_BOUNDED_REQ(RetireCacheReplicas, items, RetireCacheReplicaItem);
struct RetireCacheReplicaResult {
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());
  SERDE_STRUCT_FIELD(durableRetired, false);
};
struct RetireCacheReplicasRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<RetireCacheReplicaResult>>{});
};

struct CoordinateCacheRetireItem {
  SERDE_STRUCT_FIELD(key, CacheChunkKey{});
  SERDE_STRUCT_FIELD(expectedGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(placement, PlacementIdentity{});
  SERDE_STRUCT_FIELD(evictionEpoch, cache::EvictionEpoch{});
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    RETURN_ON_ERROR(placement.valid());
    if (expectedGeneration == cache::CacheGeneration{} || evictionEpoch == cache::EvictionEpoch{} ||
        operationId == Uuid::zero())
      return makeError(StatusCode::kInvalidArg, "invalid coordinated retire identity");
    return Void{};
  }
};
STORAGE_PHASE2_BOUNDED_REQ(CoordinateCacheRetires, items, CoordinateCacheRetireItem);
struct CoordinateCacheRetireResult {
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());
  SERDE_STRUCT_FIELD(allReplicasDurableRetired, false);
};
struct CoordinateCacheRetiresRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CoordinateCacheRetireResult>>{});
};

#undef STORAGE_PHASE2_BOUNDED_REQ

}  // namespace hf3fs::storage
