#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <variant>

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "common/utils/StrongType.h"
#include "common/utils/Uuid.h"
#include "fbs/core/user/User.h"

namespace hf3fs::cache {

inline constexpr uint32_t kCacheSchemaVersion = 2;
inline constexpr uint32_t kCacheProtocolVersion = 2;
inline constexpr uint32_t kCachePhase3ProtocolVersion = 3;
inline constexpr size_t kMaxPhase2BatchItems = 1000;
inline constexpr size_t kMaxDatasetSources = 64;
inline constexpr size_t kMaxDatasetPaths = 4096;
inline constexpr size_t kMaxDatasetPathLength = 4096;
inline constexpr size_t kMaxPathListBytes = 1U << 20;
inline constexpr size_t kMaxJobSourceBytes = 64U << 10;
inline constexpr size_t kMaxOriginNameLength = 1024;
inline constexpr uint32_t kMaxJobParallelLoads = 1024;
inline constexpr uint32_t kReadyRatioScaleBps = 10000;
inline constexpr uint64_t kMaxPinTtlMs = 365ULL * 24 * 60 * 60 * 1000;

inline Result<Void> checkPhase2Capability(uint32_t protocolVersion, bool enabled) {
  if (protocolVersion != kCacheProtocolVersion) {
    return makeError(CacheCode::kUpgradeRequired, "incompatible cache protocol version");
  }
  if (!enabled) return makeError(CacheCode::kFeatureDisabled, "cache phase two is disabled");
  return Void{};
}

inline Result<Void> checkPhase3Capability(uint32_t protocolVersion, bool enabled) {
  if (protocolVersion != kCachePhase3ProtocolVersion) {
    return makeError(CacheCode::kUpgradeRequired, "incompatible cache phase three protocol version");
  }
  if (!enabled) return makeError(CacheCode::kFeatureDisabled, "cache phase three is disabled");
  return Void{};
}

STRONG_TYPEDEF(uint32_t, OriginId);
STRONG_TYPEDEF(uint32_t, CacheBlockIndex);
STRONG_TYPEDEF(uint64_t, CacheGeneration);
STRONG_TYPEDEF(uint64_t, CleanupEpoch);
STRONG_TYPEDEF(uint64_t, EvictionEpoch);
STRONG_TYPEDEF(Uuid, PrefetchJobId);
STRONG_TYPEDEF(Uuid, PinOwnerId);

enum class EvictionReason : uint8_t {
  INVALID = 0,
  CAPACITY_WATERMARK = 1,
  LOCAL_SAFETY = 2,
};

enum class CacheStorageEventType : uint8_t {
  DELETED = 0,
  EMERGENCY_EVICTED = 1,
  LOST = 2,
  CORRUPTED = 3,
};

enum class CacheStorageEventState : uint8_t {
  INVALID = 0,
  PREPARED = 1,
  DELIVERABLE = 2,
};

enum class CachePermitState : uint8_t {
  INVALID = 0,
  RESERVED = 1,
  PINNED = 2,
};

enum class CacheEnqueueOutcome : uint8_t {
  INVALID = 0,
  CREATED = 1,
  QUEUED = 2,
  LOADING = 3,
  READY = 4,
};

enum class PrefetchPlanEntryState : uint8_t {
  INVALID = 0,
  PLANNED = 1,
  ADMITTED = 2,
  ATTACHED = 3,
  READY = 4,
  FAILED = 5,
  CANCELLED = 6,
};

enum class PinOwnerKind : uint8_t {
  INVALID = 0,
  ACTIVE_JOB = 1,
  EXPLICIT_PIN = 2,
  POST_READY = 3,
};

enum class PrefetchJobState : uint8_t {
  INVALID = 0,
  PENDING = 1,
  PLANNING = 2,
  LOADING = 3,
  PARTIAL_READY = 4,
  READY = 5,
  FAILED = 6,
  CANCELLED = 7,
};

enum class DatasetSourceType : uint8_t {
  NAMESPACE_PATH = 0,
  PATH_LIST = 1,
  MANIFEST_PATH = 2,
  S3_PREFIX = 3,
};

struct NamespacePathSource {
  SERDE_STRUCT_FIELD(path, std::string{});
  SERDE_STRUCT_FIELD(recursive, true);

 public:
  Result<Void> valid() const;
  bool operator==(const NamespacePathSource &) const = default;
};

struct PathListSource {
  SERDE_STRUCT_FIELD(paths, std::vector<std::string>{});

 public:
  Result<Void> valid() const;
  bool operator==(const PathListSource &) const = default;
};

struct ManifestPathSource {
  SERDE_STRUCT_FIELD(path, std::string{});

 public:
  Result<Void> valid() const;
  bool operator==(const ManifestPathSource &) const = default;
};

struct S3PrefixSource {
  SERDE_STRUCT_FIELD(originId, OriginId{});
  SERDE_STRUCT_FIELD(bucket, std::string{});
  SERDE_STRUCT_FIELD(prefix, std::string{});
  SERDE_STRUCT_FIELD(destinationRoot, std::string{});

 public:
  Result<Void> valid() const;
  bool operator==(const S3PrefixSource &) const = default;
};

struct DatasetSource {
  using Source = std::variant<NamespacePathSource, PathListSource, ManifestPathSource, S3PrefixSource>;
  SERDE_STRUCT_FIELD(source, Source{});

 public:
  DatasetSourceType type() const;
  Result<Void> valid() const;
  bool operator==(const DatasetSource &) const = default;
};

Result<Void> validateDatasetSources(const std::vector<DatasetSource> &sources);

struct PrefetchJobSpec {
  SERDE_STRUCT_FIELD(jobId, PrefetchJobId{});
  // Authentication tokens never enter the persistent job specification.
  SERDE_STRUCT_FIELD(ownerUid, flat::Uid{});
  SERDE_STRUCT_FIELD(sources, std::vector<DatasetSource>{});
  SERDE_STRUCT_FIELD(priority, uint32_t{});
  SERDE_STRUCT_FIELD(maxParallelLoads, uint32_t{1});
  // Zero means unlimited.
  SERDE_STRUCT_FIELD(bandwidthLimitBytesPerSec, uint64_t{});
  SERDE_STRUCT_FIELD(requiredReadyBps, kReadyRatioScaleBps);
  SERDE_STRUCT_FIELD(pinAfterReady, false);
  SERDE_STRUCT_FIELD(pinTtlMs, uint64_t{});

 public:
  Result<Void> valid() const;
  bool operator==(const PrefetchJobSpec &) const = default;
};

struct PrefetchJobRecord {
  SERDE_STRUCT_FIELD(spec, PrefetchJobSpec{});
  SERDE_STRUCT_FIELD(state, PrefetchJobState::INVALID);
  SERDE_STRUCT_FIELD(stateVersion, uint64_t{});
  SERDE_STRUCT_FIELD(createdAtMs, uint64_t{});
  SERDE_STRUCT_FIELD(updatedAtMs, uint64_t{});
  SERDE_STRUCT_FIELD(plannedBytes, uint64_t{});
  SERDE_STRUCT_FIELD(readyBytes, uint64_t{});
  SERDE_STRUCT_FIELD(failedBytes, uint64_t{});
  SERDE_STRUCT_FIELD(plannedBlocks, uint64_t{});
  SERDE_STRUCT_FIELD(readyBlocks, uint64_t{});
  SERDE_STRUCT_FIELD(failedBlocks, uint64_t{});
  SERDE_STRUCT_FIELD(cancelEpoch, uint64_t{});
  SERDE_STRUCT_FIELD(error, std::string{});
  SERDE_STRUCT_FIELD(plannerSourceIndex, uint32_t{});
  SERDE_STRUCT_FIELD(plannerCursor, std::string{});
  SERDE_STRUCT_FIELD(planningComplete, false);

 public:
  Result<Void> valid() const;
  bool operator==(const PrefetchJobRecord &) const = default;
};

Result<EvictionEpoch> nextEvictionEpoch(EvictionEpoch current);

enum class VersionSelectorType : uint8_t {
  VERSION_ID,
  STRONG_ETAG,
};

struct VersionSelector {
  SERDE_STRUCT_FIELD(type, VersionSelectorType::VERSION_ID);
  SERDE_STRUCT_FIELD(value, std::string{});

 public:
  Result<Void> valid() const;
  bool operator==(const VersionSelector &) const = default;
};

struct ObjectRef {
  SERDE_STRUCT_FIELD(originId, OriginId{});
  SERDE_STRUCT_FIELD(bucket, std::string{});
  SERDE_STRUCT_FIELD(key, std::string{});

 public:
  Result<Void> valid() const;
  bool operator==(const ObjectRef &) const = default;
};

struct ImmutableObjectIdentity {
  SERDE_STRUCT_FIELD(originId, OriginId{});
  SERDE_STRUCT_FIELD(bucket, std::string{});
  SERDE_STRUCT_FIELD(key, std::string{});
  SERDE_STRUCT_FIELD(version, VersionSelector{});

 public:
  Result<Void> valid() const;
  bool operator==(const ImmutableObjectIdentity &) const = default;
};

struct CacheBlockKey {
  // InodeId intentionally stays wire-neutral here so cache-fbs does not depend on meta-fbs.
  SERDE_STRUCT_FIELD(inode, uint64_t{});
  SERDE_STRUCT_FIELD(block, CacheBlockIndex{});

 public:
  Result<Void> valid() const;
  bool operator==(const CacheBlockKey &) const = default;
};

struct ReadyIdentity {
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{});
  SERDE_STRUCT_FIELD(cacheGeneration, CacheGeneration{});
  SERDE_STRUCT_FIELD(checksumType, uint8_t{});
  SERDE_STRUCT_FIELD(checksumValue, uint32_t{});
  SERDE_STRUCT_FIELD(blockLength, uint64_t{});

 public:
  Result<Void> valid() const;
  bool operator==(const ReadyIdentity &) const = default;
};

struct PrefetchPlanEntry {
  SERDE_STRUCT_FIELD(jobId, PrefetchJobId{});
  SERDE_STRUCT_FIELD(key, CacheBlockKey{});
  SERDE_STRUCT_FIELD(blockLength, uint64_t{});
  SERDE_STRUCT_FIELD(priority, uint32_t{});
  SERDE_STRUCT_FIELD(state, PrefetchPlanEntryState::INVALID);
  SERDE_STRUCT_FIELD(admissionAttemptId, Uuid::zero());
  // Set once when READY is first observed. It fences current readiness while
  // Job counters retain monotonic achieved readiness.
  SERDE_STRUCT_FIELD(ready, std::optional<ReadyIdentity>{});

 public:
  Result<Void> valid() const;
  bool operator==(const PrefetchPlanEntry &) const = default;
};

struct PinOwner {
  SERDE_STRUCT_FIELD(kind, PinOwnerKind::INVALID);
  SERDE_STRUCT_FIELD(id, PinOwnerId{});

 public:
  Result<Void> valid() const;
  bool operator==(const PinOwner &) const = default;
};

struct PinRecord {
  SERDE_STRUCT_FIELD(key, CacheBlockKey{});
  SERDE_STRUCT_FIELD(owner, PinOwner{});
  SERDE_STRUCT_FIELD(createdAtMs, uint64_t{});
  SERDE_STRUCT_FIELD(expiresAtMs, uint64_t{});
  SERDE_STRUCT_FIELD(cacheGeneration, CacheGeneration{});

 public:
  Result<Void> valid() const;
  bool operator==(const PinRecord &) const = default;
};

struct ByteRange {
  SERDE_STRUCT_FIELD(offset, uint64_t{});
  SERDE_STRUCT_FIELD(length, uint64_t{});

 public:
  Result<uint64_t> end() const;
  bool empty() const { return length == 0; }
  bool operator==(const ByteRange &) const = default;
};

enum class CacheBlockState : uint8_t {
  NONE,
  QUEUED,
  LOADING,
  READY,
  CLEANING,
  FAILED,
  INVALID,
  EVICTING,
};

enum class ChargeKind : uint8_t {
  NONE,
  RESERVED,
  COMMITTED,
};

enum class CleanupTerminalState : uint8_t {
  NONE,
  FAILED,
  REENQUEUE,
};

}  // namespace hf3fs::cache
