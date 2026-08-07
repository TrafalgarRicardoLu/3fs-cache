#include "fbs/cache/Common.h"

#include <type_traits>

#include "common/utils/MagicEnum.hpp"
#include "common/utils/Path.h"
#include "common/utils/Status.h"
#include "common/utils/StatusCode.h"
#include "fmt/format.h"

namespace hf3fs::cache {
namespace {

Result<Void> validAbsoluteNamespacePath(std::string_view value) {
  if (value.empty() || value.size() > kMaxDatasetPathLength) {
    return makeError(StatusCode::kInvalidArg, "invalid dataset path length");
  }
  if (value.find('\0') != std::string_view::npos) {
    return makeError(StatusCode::kInvalidArg, "dataset path contains NUL");
  }
  Path path{std::string{value}};
  if (!path.is_absolute()) {
    return makeError(StatusCode::kInvalidArg, "dataset path is not absolute");
  }
  for (const auto &component : path) {
    if (component == "..") {
      return makeError(StatusCode::kInvalidArg, "dataset path escapes its root");
    }
  }
  return Void{};
}

Result<Void> validOriginComponent(std::string_view value, std::string_view name) {
  if (value.empty() || value.size() > kMaxOriginNameLength) {
    return makeError(StatusCode::kInvalidArg, fmt::format("invalid {} length", name));
  }
  if (value.find('\0') != std::string_view::npos) {
    return makeError(StatusCode::kInvalidArg, fmt::format("{} contains NUL", name));
  }
  return Void{};
}

Result<size_t> sourcePayloadBytes(const DatasetSource &source) {
  return std::visit(
      [](const auto &value) -> Result<size_t> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, NamespacePathSource> || std::is_same_v<T, ManifestPathSource>) {
          return value.path.size();
        } else if constexpr (std::is_same_v<T, PathListSource>) {
          size_t total = 0;
          for (const auto &path : value.paths) {
            if (path.size() > std::numeric_limits<size_t>::max() - total) {
              return makeError(CacheCode::kRequestTooLarge, "dataset path bytes overflow");
            }
            total += path.size();
          }
          return total;
        } else {
          size_t total = value.bucket.size();
          if (value.prefix.size() > std::numeric_limits<size_t>::max() - total) {
            return makeError(CacheCode::kRequestTooLarge, "origin source bytes overflow");
          }
          total += value.prefix.size();
          if (value.destinationRoot.size() > std::numeric_limits<size_t>::max() - total) {
            return makeError(CacheCode::kRequestTooLarge, "origin source bytes overflow");
          }
          return total + value.destinationRoot.size();
        }
      },
      source.source);
}

}  // namespace

Result<EvictionEpoch> nextEvictionEpoch(EvictionEpoch current) {
  if (current == EvictionEpoch{std::numeric_limits<uint64_t>::max()}) {
    return makeError(CacheCode::kStateConflict, "cache eviction epoch overflow");
  }
  return EvictionEpoch{current.toUnderType() + 1};
}

Result<Void> VersionSelector::valid() const {
  if (!magic_enum::enum_contains(type)) {
    return makeError(StatusCode::kInvalidArg, fmt::format("invalid version selector type {}", static_cast<int>(type)));
  }
  if (value.empty()) {
    return makeError(StatusCode::kInvalidArg, "empty object version selector");
  }
  if (type == VersionSelectorType::STRONG_ETAG && value.starts_with("W/")) {
    return makeError(StatusCode::kInvalidArg, "weak ETag is not a valid version selector");
  }
  return Void{};
}

Result<Void> ObjectRef::valid() const {
  if (originId == OriginId{}) {
    return makeError(StatusCode::kInvalidArg, "empty origin id");
  }
  RETURN_ON_ERROR(validOriginComponent(bucket, "object bucket"));
  RETURN_ON_ERROR(validOriginComponent(key, "object key"));
  return Void{};
}

Result<Void> ImmutableObjectIdentity::valid() const {
  RETURN_ON_ERROR((ObjectRef{originId, bucket, key}.valid()));
  return version.valid();
}

Result<Void> CompletedUploadPart::valid() const {
  if (partNumber == 0 || partNumber > kMaxUploadParts) {
    return makeError(StatusCode::kInvalidArg, "invalid completed upload part number");
  }
  if (etag.empty() || etag.size() > kMaxCompletedPartTagBytes || etag.find('\0') != std::string::npos ||
      checksum.size() > kMaxCompletedPartTagBytes || checksum.find('\0') != std::string::npos) {
    return makeError(StatusCode::kInvalidArg, "invalid completed upload part tag");
  }
  return Void{};
}

Result<Void> UploadJobRecord::valid() const {
  if (jobId == UploadJobId{} || stagingInode == 0) {
    return makeError(StatusCode::kInvalidArg, "empty upload job or staging inode identity");
  }
  RETURN_ON_ERROR(validAbsoluteNamespacePath(path));
  RETURN_ON_ERROR(destination.valid());
  if (!magic_enum::enum_contains(state) || state == UploadJobState::INVALID || stateVersion == 0 || createdAtMs == 0 ||
      updatedAtMs < createdAtMs || nextPartNumber == 0 || nextPartNumber > kMaxUploadParts + 1 ||
      parts.size() > kMaxUploadParts || multipartId.size() > kMaxMultipartUploadIdBytes ||
      multipartId.find('\0') != std::string::npos || error.size() > kMaxUploadErrorBytes ||
      error.find('\0') != std::string::npos) {
    return makeError(StatusCode::kInvalidArg, "invalid upload job state, bounds, or timestamp");
  }
  uint32_t expectedPart = 1;
  uint64_t uploadedBytes = 0;
  for (const auto &part : parts) {
    RETURN_ON_ERROR(part.valid());
    if (part.partNumber != expectedPart++) {
      return makeError(StatusCode::kInvalidArg, "upload parts are not contiguous");
    }
    if (part.size > std::numeric_limits<uint64_t>::max() - uploadedBytes) {
      return makeError(StatusCode::kInvalidArg, "uploaded part bytes overflow");
    }
    uploadedBytes += part.size;
  }
  if (nextPartNumber != expectedPart || uploadedBytes > stagingLength) {
    return makeError(StatusCode::kInvalidArg, "upload part progress exceeds staging snapshot");
  }
  const bool open = state == UploadJobState::OPEN;
  if (open != (writerLeaseId != Uuid::zero()) || open != (writerLeaseExpiresAtMs > updatedAtMs)) {
    return makeError(StatusCode::kInvalidArg, "upload writer lease does not match job state");
  }
  if (open && (!multipartId.empty() || !parts.empty() || completedObject || publishedInode != 0)) {
    return makeError(StatusCode::kInvalidArg, "open upload job has durable upload results");
  }
  const bool needsMultipart =
      state == UploadJobState::UPLOADING || state == UploadJobState::COMPLETING || state == UploadJobState::ABORTING;
  if (needsMultipart && multipartId.empty()) {
    return makeError(StatusCode::kInvalidArg, "active multipart job has no upload id");
  }
  const bool completed = state == UploadJobState::PUBLISHING || state == UploadJobState::PUBLISHED;
  if (completed != completedObject.has_value()) {
    return makeError(StatusCode::kInvalidArg, "upload completion identity does not match job state");
  }
  if (completedObject) {
    RETURN_ON_ERROR(completedObject->valid());
    if (completedObject->originId != destination.originId || completedObject->bucket != destination.bucket ||
        completedObject->key != destination.key || uploadedBytes != stagingLength) {
      return makeError(StatusCode::kInvalidArg, "completed object does not match upload destination or length");
    }
  }
  if ((state == UploadJobState::PUBLISHED) != (publishedInode != 0)) {
    return makeError(StatusCode::kInvalidArg, "published inode does not match upload job state");
  }
  return Void{};
}

Result<Void> ReconcileProgress::valid() const {
  if (runId == ReconcileRunId{} || !magic_enum::enum_contains(state) || state == ReconcileRunState::INVALID ||
      startedAtMs == 0 || updatedAtMs < startedAtMs || repaired > scanned || orphaned > scanned || missing > scanned ||
      conflicts > scanned || error.size() > kMaxUploadErrorBytes || error.find('\0') != std::string::npos) {
    return makeError(StatusCode::kInvalidArg, "invalid cache reconcile progress");
  }
  if (state == ReconcileRunState::HEALTHY && (!error.empty() || conflicts != 0)) {
    return makeError(StatusCode::kInvalidArg, "healthy reconcile run has unresolved errors");
  }
  return Void{};
}

Result<Void> CacheBlockKey::valid() const {
  if (inode == 0) {
    return makeError(StatusCode::kInvalidArg, "empty cache block inode");
  }
  return Void{};
}

Result<Void> ReadyIdentity::valid() const {
  if (loadEpoch == 0 || cacheGeneration == CacheGeneration{}) {
    return makeError(StatusCode::kInvalidArg, "empty ready identity epoch or generation");
  }
  if (blockLength == 0) {
    return makeError(StatusCode::kInvalidArg, "empty ready block");
  }
  return Void{};
}

Result<Void> PrefetchPlanEntry::valid() const {
  if (jobId == PrefetchJobId{}) {
    return makeError(StatusCode::kInvalidArg, "empty prefetch job id");
  }
  RETURN_ON_ERROR(key.valid());
  if (blockLength == 0) {
    return makeError(StatusCode::kInvalidArg, "empty prefetch plan block");
  }
  if (!magic_enum::enum_contains(state) || state == PrefetchPlanEntryState::INVALID) {
    return makeError(StatusCode::kInvalidArg, "invalid prefetch plan entry state");
  }
  if (state == PrefetchPlanEntryState::PLANNED && admissionAttemptId != Uuid::zero()) {
    return makeError(StatusCode::kInvalidArg, "planned entry has admission identity");
  }
  if ((state == PrefetchPlanEntryState::ADMITTED || state == PrefetchPlanEntryState::ATTACHED) &&
      admissionAttemptId == Uuid::zero()) {
    return makeError(StatusCode::kInvalidArg, "admitted entry is missing admission identity");
  }
  if (state == PrefetchPlanEntryState::READY) {
    if (!ready.has_value()) return makeError(StatusCode::kInvalidArg, "ready plan entry has no generation fence");
    RETURN_ON_ERROR(ready->valid());
    if (ready->blockLength != blockLength) {
      return makeError(StatusCode::kInvalidArg, "ready plan entry block length changed");
    }
  } else if (ready.has_value()) {
    return makeError(StatusCode::kInvalidArg, "non-ready plan entry has a generation fence");
  }
  return Void{};
}

Result<Void> PinOwner::valid() const {
  if (!magic_enum::enum_contains(kind) || kind == PinOwnerKind::INVALID) {
    return makeError(StatusCode::kInvalidArg, "invalid cache pin owner kind");
  }
  if (id == PinOwnerId{}) {
    return makeError(StatusCode::kInvalidArg, "empty cache pin owner id");
  }
  return Void{};
}

Result<Void> PinRecord::valid() const {
  RETURN_ON_ERROR(key.valid());
  RETURN_ON_ERROR(owner.valid());
  if (createdAtMs == 0 || expiresAtMs <= createdAtMs) {
    return makeError(StatusCode::kInvalidArg, "invalid cache pin lifetime");
  }
  return Void{};
}

Result<Void> NamespacePathSource::valid() const { return validAbsoluteNamespacePath(path); }

Result<Void> PathListSource::valid() const {
  if (paths.empty() || paths.size() > kMaxDatasetPaths) {
    return makeError(CacheCode::kRequestTooLarge, "invalid dataset path count");
  }
  size_t totalBytes = 0;
  for (const auto &path : paths) {
    RETURN_ON_ERROR(validAbsoluteNamespacePath(path));
    if (path.size() > kMaxPathListBytes - totalBytes) {
      return makeError(CacheCode::kRequestTooLarge, "dataset path list bytes too large");
    }
    totalBytes += path.size();
  }
  return Void{};
}

Result<Void> ManifestPathSource::valid() const { return validAbsoluteNamespacePath(path); }

Result<Void> S3PrefixSource::valid() const {
  if (originId == OriginId{}) {
    return makeError(StatusCode::kInvalidArg, "empty origin id");
  }
  RETURN_ON_ERROR(validOriginComponent(bucket, "bucket"));
  RETURN_ON_ERROR(validOriginComponent(prefix, "prefix"));
  return validAbsoluteNamespacePath(destinationRoot);
}

DatasetSourceType DatasetSource::type() const { return static_cast<DatasetSourceType>(source.index()); }

Result<Void> DatasetSource::valid() const {
  if (source.valueless_by_exception()) {
    return makeError(StatusCode::kInvalidArg, "dataset source has no value");
  }
  return std::visit([](const auto &value) { return value.valid(); }, source);
}

Result<Void> validateDatasetSources(const std::vector<DatasetSource> &sources) {
  if (sources.empty() || sources.size() > kMaxDatasetSources) {
    return makeError(CacheCode::kRequestTooLarge, "invalid dataset source count");
  }
  size_t sourceBytes = 0;
  for (const auto &source : sources) {
    RETURN_ON_ERROR(source.valid());
    auto bytes = sourcePayloadBytes(source);
    RETURN_ON_ERROR(bytes);
    if (*bytes > kMaxJobSourceBytes - sourceBytes) {
      return makeError(CacheCode::kRequestTooLarge, "prefetch job source bytes too large");
    }
    sourceBytes += *bytes;
  }
  return Void{};
}

Result<Void> PrefetchJobSpec::valid() const {
  if (jobId == PrefetchJobId{}) {
    return makeError(StatusCode::kInvalidArg, "empty prefetch job id");
  }
  RETURN_ON_ERROR(validateDatasetSources(sources));
  if (priority > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return makeError(StatusCode::kInvalidArg, "prefetch priority is out of range");
  }
  if (maxParallelLoads == 0 || maxParallelLoads > kMaxJobParallelLoads) {
    return makeError(StatusCode::kInvalidArg, "invalid prefetch parallel load limit");
  }
  if (requiredReadyBps == 0 || requiredReadyBps > kReadyRatioScaleBps) {
    return makeError(StatusCode::kInvalidArg, "invalid prefetch ready ratio");
  }
  if (pinAfterReady != (pinTtlMs != 0)) {
    return makeError(StatusCode::kInvalidArg, "pin TTL and pin-after-ready disagree");
  }
  if (pinTtlMs > kMaxPinTtlMs) {
    return makeError(StatusCode::kInvalidArg, "prefetch pin TTL is too large");
  }
  return Void{};
}

Result<Void> PrefetchJobRecord::valid() const {
  RETURN_ON_ERROR(spec.valid());
  if (!magic_enum::enum_contains(state) || state == PrefetchJobState::INVALID) {
    return makeError(StatusCode::kInvalidArg, "invalid prefetch job state");
  }
  if (stateVersion == 0 || createdAtMs == 0 || updatedAtMs < createdAtMs) {
    return makeError(StatusCode::kInvalidArg, "invalid prefetch job version or timestamp");
  }
  if (readyBytes > plannedBytes || failedBytes > plannedBytes || readyBlocks > plannedBlocks ||
      failedBlocks > plannedBlocks) {
    return makeError(StatusCode::kInvalidArg, "prefetch job counters exceed plan");
  }
  if (readyBytes > plannedBytes - failedBytes || readyBlocks > plannedBlocks - failedBlocks) {
    return makeError(StatusCode::kInvalidArg, "prefetch job counters overlap");
  }
  if (state == PrefetchJobState::CANCELLED && cancelEpoch == 0) {
    return makeError(StatusCode::kInvalidArg, "cancelled prefetch job has no cancel epoch");
  }
  if (plannerSourceIndex > spec.sources.size() || plannerCursor.size() > kMaxDatasetPathLength ||
      (planningComplete && plannerSourceIndex != spec.sources.size()) ||
      (!planningComplete && plannerSourceIndex == spec.sources.size())) {
    return makeError(StatusCode::kInvalidArg, "invalid prefetch planner cursor");
  }
  return Void{};
}

Result<uint64_t> ByteRange::end() const {
  if (length > std::numeric_limits<uint64_t>::max() - offset) {
    return makeError(StatusCode::kInvalidArg, "object byte range overflow");
  }
  return offset + length;
}

}  // namespace hf3fs::cache
