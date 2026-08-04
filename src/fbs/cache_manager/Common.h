#pragma once

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "fbs/cache/Common.h"
#include "fbs/core/user/User.h"
#include "fbs/meta/Common.h"

namespace hf3fs::cache_manager {

inline constexpr uint32_t kMaxCacheManagerBlocks = 1000;
using cache::DatasetSource;
using cache::DatasetSourceType;
using cache::ManifestPathSource;
using cache::NamespacePathSource;
using cache::PathListSource;
using cache::PrefetchJobRecord;
using cache::PrefetchJobSpec;
using cache::PrefetchJobState;
using cache::S3PrefixSource;
inline constexpr auto kMaxDatasetSources = cache::kMaxDatasetSources;
inline constexpr auto kMaxDatasetPaths = cache::kMaxDatasetPaths;
inline constexpr auto kMaxDatasetPathLength = cache::kMaxDatasetPathLength;
inline constexpr auto kMaxPathListBytes = cache::kMaxPathListBytes;
inline constexpr auto kMaxJobSourceBytes = cache::kMaxJobSourceBytes;
inline constexpr auto kMaxOriginNameLength = cache::kMaxOriginNameLength;
inline constexpr auto kMaxJobParallelLoads = cache::kMaxJobParallelLoads;
inline constexpr auto kReadyRatioScaleBps = cache::kReadyRatioScaleBps;
inline constexpr auto kMaxPinTtlMs = cache::kMaxPinTtlMs;

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
