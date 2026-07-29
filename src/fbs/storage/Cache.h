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

}  // namespace hf3fs::storage
