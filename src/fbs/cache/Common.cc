#include "fbs/cache/Common.h"

#include "common/utils/MagicEnum.hpp"
#include "common/utils/Status.h"
#include "common/utils/StatusCode.h"
#include "fmt/format.h"

namespace hf3fs::cache {

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
  if (bucket.empty()) {
    return makeError(StatusCode::kInvalidArg, "empty object bucket");
  }
  if (key.empty()) {
    return makeError(StatusCode::kInvalidArg, "empty object key");
  }
  return Void{};
}

Result<Void> ImmutableObjectIdentity::valid() const {
  RETURN_ON_ERROR((ObjectRef{originId, bucket, key}.valid()));
  return version.valid();
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

Result<uint64_t> ByteRange::end() const {
  if (length > std::numeric_limits<uint64_t>::max() - offset) {
    return makeError(StatusCode::kInvalidArg, "object byte range overflow");
  }
  return offset + length;
}

}  // namespace hf3fs::cache
