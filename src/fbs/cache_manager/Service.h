#pragma once

#include <limits>

#include "common/serde/Service.h"
#include "fbs/cache_manager/Common.h"
#include "fbs/storage/StorageIdentity.h"

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
    if (priority < 0) return makeError(StatusCode::kInvalidArg, "priority is negative");
    if (blockCount == 0) return makeError(StatusCode::kInvalidArg, "blockCount is zero");
    if (blockCount > kMaxCacheManagerBlocks) return makeError(CacheCode::kRequestTooLarge, "blockCount too large");
    return Void{};
  }
};
struct EnsureCachedRsp {
  SERDE_STRUCT_FIELD(status, EnsureCachedStatus::BYPASSED);
  SERDE_STRUCT_FIELD(bypassReason, BypassReason::NONE);
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
    RETURN_ON_ERROR(expectedReady.valid());
    if (observedGeneration == cache::CacheGeneration{})
      return makeError(StatusCode::kInvalidArg, "observedGeneration not set");
    return Void{};
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
    if (beginBlock.toUnderType() > std::numeric_limits<uint32_t>::max() - blockCount)
      return makeError(StatusCode::kInvalidArg, "block range overflow");
    if (expectedReady) RETURN_ON_ERROR(expectedReady->valid());
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
  SERDE_STRUCT_FIELD(inflightBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(lastBypassReason, BypassReason::NONE);
  SERDE_STRUCT_FIELD(phase3Enabled, false);
  SERDE_STRUCT_FIELD(activeJobs, uint64_t{0});
  SERDE_STRUCT_FIELD(failedJobs, uint64_t{0});
  SERDE_STRUCT_FIELD(phase3PlannedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(phase3ReadyBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(pinnedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(lastJobError, String{});
};

struct CacheAccessReportItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(generation, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(clientObservedTimeNs, uint64_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (generation == cache::CacheGeneration{}) return makeError(StatusCode::kInvalidArg, "generation not set");
    return Void{};
  }
};
struct ReportCacheAccessReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(items, std::vector<CacheAccessReportItem>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (items.size() > cache::kMaxPhase2BatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many access reports");
    for (const auto &item : items) RETURN_ON_ERROR(item.valid());
    return Void{};
  }
};
struct CacheAccessReportResult {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(status, AccessReportStatus::DROPPED);
};
struct ReportCacheAccessRsp {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheAccessReportResult>>{});
};

struct GetPhase2CacheStatusReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const { return Void{}; }
};
struct Phase2DiskStatus {
  SERDE_STRUCT_FIELD(physicalDiskId, storage::PhysicalDiskId{});
  SERDE_STRUCT_FIELD(role, storage::StorageRole::INVALID);
  SERDE_STRUCT_FIELD(capacityBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(physicalUsedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(allocatableBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(reservedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(snapshotAgeNs, uint64_t{0});
  SERDE_STRUCT_FIELD(admissionPaused, false);
  SERDE_STRUCT_FIELD(pauseReason, String{});
  SERDE_STRUCT_FIELD(eventPrepared, uint64_t{0});
  SERDE_STRUCT_FIELD(eventDeliverable, uint64_t{0});
  SERDE_STRUCT_FIELD(eventAcknowledgedSequence, uint64_t{0});
  SERDE_STRUCT_FIELD(activeGenerations, uint64_t{0});
  SERDE_STRUCT_FIELD(enforcedHighWatermark, double{0});
  SERDE_STRUCT_FIELD(permitStoreHealthy, false);
  SERDE_STRUCT_FIELD(eventJournalWritable, false);
};
struct GetPhase2CacheStatusRsp {
  SERDE_STRUCT_FIELD(enabled, false);
  SERDE_STRUCT_FIELD(managerEpoch, Uuid::zero());
  SERDE_STRUCT_FIELD(admissionPolicy, String{});
  SERDE_STRUCT_FIELD(evictionPolicy, String{});
  SERDE_STRUCT_FIELD(disks, std::vector<Phase2DiskStatus>{});
  SERDE_STRUCT_FIELD(evicting, uint64_t{0});
  SERDE_STRUCT_FIELD(eventBacklog, uint64_t{0});
  SERDE_STRUCT_FIELD(deadLetters, uint64_t{0});
  SERDE_STRUCT_FIELD(capacityHighWatermark, double{0});
  SERDE_STRUCT_FIELD(capacityLowWatermark, double{0});
  SERDE_STRUCT_FIELD(snapshotMaxAgeNs, uint64_t{0});
  SERDE_STRUCT_FIELD(permitTtlNs, uint64_t{0});
};

struct CreatePrefetchJobReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(spec, cache::PrefetchJobSpec{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(spec.valid());
    if (spec.ownerUid != user.uid) return makeError(StatusCode::kInvalidArg, "prefetch owner does not match caller");
    return Void{};
  }
};
struct CreatePrefetchJobRsp {
  SERDE_STRUCT_FIELD(job, cache::PrefetchJobRecord{});
};

struct GetPrefetchJobReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(jobId, cache::PrefetchJobId{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (jobId == cache::PrefetchJobId{}) return makeError(StatusCode::kInvalidArg, "prefetch job id not set");
    return Void{};
  }
};
struct GetPrefetchJobRsp {
  SERDE_STRUCT_FIELD(job, cache::PrefetchJobRecord{});
};

struct ListPrefetchJobsReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(after, std::optional<cache::PrefetchJobId>{});
  SERDE_STRUCT_FIELD(limit, uint32_t{100});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (after && *after == cache::PrefetchJobId{}) return makeError(StatusCode::kInvalidArg, "invalid job cursor");
    if (limit == 0 || limit > cache::kMaxPhase2BatchItems)
      return makeError(CacheCode::kRequestTooLarge, "invalid prefetch job page limit");
    return Void{};
  }
};
struct ListPrefetchJobsRsp {
  SERDE_STRUCT_FIELD(jobs, std::vector<cache::PrefetchJobRecord>{});
  SERDE_STRUCT_FIELD(more, false);
};

struct CancelPrefetchJobReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(jobId, cache::PrefetchJobId{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (jobId == cache::PrefetchJobId{}) return makeError(StatusCode::kInvalidArg, "prefetch job id not set");
    return Void{};
  }
};
struct CancelPrefetchJobRsp {
  SERDE_STRUCT_FIELD(job, cache::PrefetchJobRecord{});
};

struct PinDatasetReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(pinId, cache::PinOwnerId{});
  SERDE_STRUCT_FIELD(sources, std::vector<cache::DatasetSource>{});
  SERDE_STRUCT_FIELD(prefetchMissing, false);
  SERDE_STRUCT_FIELD(priority, uint32_t{});
  SERDE_STRUCT_FIELD(ttlMs, uint64_t{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const;
};
struct PinDatasetRsp {
  SERDE_STRUCT_FIELD(pinId, cache::PinOwnerId{});
  SERDE_STRUCT_FIELD(plannedBytes, uint64_t{});
};

struct UnpinDatasetReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(pinId, cache::PinOwnerId{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (pinId == cache::PinOwnerId{}) return makeError(StatusCode::kInvalidArg, "pin id not set");
    return Void{};
  }
};
struct UnpinDatasetRsp {
  SERDE_STRUCT_FIELD(removedBlocks, uint64_t{});
};

struct GetPinStatusReq {
  SERDE_STRUCT_FIELD(user, flat::UserInfo{});
  SERDE_STRUCT_FIELD(pinId, cache::PinOwnerId{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (pinId == cache::PinOwnerId{}) return makeError(StatusCode::kInvalidArg, "pin id not set");
    return Void{};
  }
};
struct GetPinStatusRsp {
  SERDE_STRUCT_FIELD(pinId, cache::PinOwnerId{});
  SERDE_STRUCT_FIELD(plannedBytes, uint64_t{});
  SERDE_STRUCT_FIELD(pinnedBytes, uint64_t{});
  SERDE_STRUCT_FIELD(readyBytes, uint64_t{});
  SERDE_STRUCT_FIELD(expiresAtMs, uint64_t{});
};

SERDE_SERVICE(CacheManagerSerde, 1) {
  SERDE_SERVICE_METHOD(ensureCached, 1, EnsureCachedReq, EnsureCachedRsp);
  SERDE_SERVICE_METHOD(reportCacheBlockInvalid, 2, ReportCacheBlockInvalidReq, ReportCacheBlockInvalidRsp);
  SERDE_SERVICE_METHOD(adminCleanupCacheBlocks, 3, AdminCleanupCacheBlocksReq, AdminCleanupCacheBlocksRsp);
  SERDE_SERVICE_METHOD(getCacheStatus, 4, GetCacheStatusReq, GetCacheStatusRsp);
  SERDE_SERVICE_METHOD(reportCacheAccess, 5, ReportCacheAccessReq, ReportCacheAccessRsp);
  SERDE_SERVICE_METHOD(getPhase2CacheStatus, 6, GetPhase2CacheStatusReq, GetPhase2CacheStatusRsp);
  SERDE_SERVICE_METHOD(createPrefetchJob, 7, CreatePrefetchJobReq, CreatePrefetchJobRsp);
  SERDE_SERVICE_METHOD(getPrefetchJob, 8, GetPrefetchJobReq, GetPrefetchJobRsp);
  SERDE_SERVICE_METHOD(listPrefetchJobs, 9, ListPrefetchJobsReq, ListPrefetchJobsRsp);
  SERDE_SERVICE_METHOD(cancelPrefetchJob, 10, CancelPrefetchJobReq, CancelPrefetchJobRsp);
  SERDE_SERVICE_METHOD(pinDataset, 11, PinDatasetReq, PinDatasetRsp);
  SERDE_SERVICE_METHOD(unpinDataset, 12, UnpinDatasetReq, UnpinDatasetRsp);
  SERDE_SERVICE_METHOD(getPinStatus, 13, GetPinStatusReq, GetPinStatusRsp);
};

}  // namespace hf3fs::cache_manager
