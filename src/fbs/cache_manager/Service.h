#pragma once

#include "common/serde/Service.h"
#include "fbs/cache_manager/Common.h"

namespace hf3fs::cache_manager {

struct EnsureCachedReq {
  SERDE_STRUCT_FIELD(service, ServiceIdentity{});
  SERDE_STRUCT_FIELD(inode, meta::InodeId{});
  SERDE_STRUCT_FIELD(beginBlock, cache::CacheBlockIndex{});
  SERDE_STRUCT_FIELD(blockCount, uint32_t{0});
  SERDE_STRUCT_FIELD(reason, EnsureReason::FOREGROUND_MISS);
  SERDE_STRUCT_FIELD(priority, int32_t{0});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(service.valid());
    if (blockCount == 0) return makeError(StatusCode::kInvalidArg, "blockCount is zero");
    if (blockCount > kMaxCacheManagerBlocks) return makeError(CacheCode::kRequestTooLarge, "blockCount too large");
    return Void{};
  }
};
struct EnsureCachedRsp {
  SERDE_STRUCT_FIELD(status, EnsureCachedStatus::BYPASSED);
};

struct ReportCacheBlockInvalidReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(openSessionId, Uuid::zero());
  SERDE_STRUCT_FIELD(inode, meta::InodeId{});
  SERDE_STRUCT_FIELD(block, cache::CacheBlockIndex{});
  SERDE_STRUCT_FIELD(expectedReady, cache::ReadyIdentity{});
  SERDE_STRUCT_FIELD(observedGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(reason, InvalidReason::NOT_FOUND);
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (openSessionId == Uuid::zero()) return makeError(StatusCode::kInvalidArg, "openSessionId not set");
    return expectedReady.valid();
  }
};
struct ReportCacheBlockInvalidRsp {
  SERDE_STRUCT_FIELD(attachedCleanup, false);
};

struct AdminCleanupCacheBlocksReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(inode, meta::InodeId{});
  SERDE_STRUCT_FIELD(beginBlock, cache::CacheBlockIndex{});
  SERDE_STRUCT_FIELD(blockCount, uint32_t{0});
  SERDE_STRUCT_FIELD(expectedReady, std::optional<cache::ReadyIdentity>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (blockCount == 0) return makeError(StatusCode::kInvalidArg, "blockCount is zero");
    if (blockCount > kMaxCacheManagerBlocks) return makeError(CacheCode::kRequestTooLarge, "blockCount too large");
    return Void{};
  }
};
struct AdminCleanupBlockResult {
  SERDE_STRUCT_FIELD(block, cache::CacheBlockIndex{});
  SERDE_STRUCT_FIELD(status, CleanupBlockStatus::NOT_FOUND);
};
struct AdminCleanupCacheBlocksRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<AdminCleanupBlockResult>>{});
};

struct GetCacheStatusReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(inode, std::optional<meta::InodeId>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const { return Void{}; }
};
struct GetCacheStatusRsp {
  SERDE_STRUCT_FIELD(logicalCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(usedCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(queued, uint64_t{0});
  SERDE_STRUCT_FIELD(loading, uint64_t{0});
  SERDE_STRUCT_FIELD(ready, uint64_t{0});
  SERDE_STRUCT_FIELD(cleaning, uint64_t{0});
};

SERDE_SERVICE(CacheManagerSerde, 1) {
  SERDE_SERVICE_METHOD(ensureCached, 1, EnsureCachedReq, EnsureCachedRsp);
  SERDE_SERVICE_METHOD(reportCacheBlockInvalid, 2, ReportCacheBlockInvalidReq, ReportCacheBlockInvalidRsp);
  SERDE_SERVICE_METHOD(adminCleanupCacheBlocks, 3, AdminCleanupCacheBlocksReq, AdminCleanupCacheBlocksRsp);
  SERDE_SERVICE_METHOD(getCacheStatus, 4, GetCacheStatusReq, GetCacheStatusRsp);
};

}  // namespace hf3fs::cache_manager
