#pragma once

#include <variant>

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "fbs/cache/Common.h"
#include "fbs/core/user/User.h"
#include "fbs/meta/Common.h"

namespace hf3fs::cache_manager {

inline constexpr uint32_t kMaxCacheManagerBlocks = 1000;
inline constexpr size_t kMaxDatasetSources = 64;
inline constexpr size_t kMaxDatasetPaths = 4096;
inline constexpr size_t kMaxDatasetPathLength = 4096;
inline constexpr size_t kMaxPathListBytes = 1U << 20;
inline constexpr size_t kMaxJobSourceBytes = 4U << 20;
inline constexpr size_t kMaxOriginNameLength = 1024;
inline constexpr uint32_t kMaxJobParallelLoads = 1024;
inline constexpr uint32_t kReadyRatioScaleBps = 10000;
inline constexpr uint64_t kMaxPinTtlMs = 365ULL * 24 * 60 * 60 * 1000;

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
  SERDE_STRUCT_FIELD(originId, cache::OriginId{});
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

struct PrefetchJobSpec {
  SERDE_STRUCT_FIELD(jobId, cache::PrefetchJobId{});
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

 public:
  Result<Void> valid() const;
  bool operator==(const PrefetchJobRecord &) const = default;
};

struct ServiceIdentity {
  SERDE_STRUCT_FIELD(name, String{});
  SERDE_STRUCT_FIELD(token, String{});

 public:
  Result<Void> valid() const {
    if (name.empty() || token.empty()) return makeError(StatusCode::kInvalidArg, "invalid service identity");
    return Void{};
  }
  std::string serdeToReadable() const { return std::string{name} + "@SECRET TOKEN"; }
};

enum class EnsureReason : uint8_t {
  FOREGROUND_MISS,
  PREFETCH,
  RECOVERY,
};

enum class EnsureCachedStatus : uint8_t {
  ACCEPTED,
  ATTACHED,
  BYPASSED,
};

enum class BypassReason : uint8_t {
  NONE,
  FEATURE_DISABLED,
  ADMISSION_DISABLED,
  EMPTY_RANGE,
  CAPACITY,
  POLICY,
  UNAVAILABLE,
};

enum class InvalidReason : uint8_t {
  NOT_FOUND,
  GENERATION_MISMATCH,
  CHECKSUM_MISMATCH,
};

enum class CleanupBlockStatus : uint8_t {
  CLEANED,
  RETRYING,
  NOT_FOUND,
  CONFLICT,
};

enum class AccessReportStatus : uint8_t {
  ACCEPTED,
  STALE_GENERATION,
  DROPPED,
};

}  // namespace hf3fs::cache_manager
