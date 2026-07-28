#pragma once

#include <cstdint>
#include <limits>
#include <string>

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "common/utils/StrongType.h"

namespace hf3fs::cache {

STRONG_TYPEDEF(uint32_t, OriginId);
STRONG_TYPEDEF(uint32_t, CacheBlockIndex);
STRONG_TYPEDEF(uint64_t, CacheGeneration);
STRONG_TYPEDEF(uint64_t, CleanupEpoch);

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
