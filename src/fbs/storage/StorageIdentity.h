#pragma once

#include <cstdint>

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "common/utils/Uuid.h"

namespace hf3fs::storage {

struct PhysicalDiskId {
  SERDE_STRUCT_FIELD(uuid, Uuid::zero());

 public:
  Result<Void> valid() const;
  bool operator==(const PhysicalDiskId &) const = default;
  bool operator<(const PhysicalDiskId &other) const { return uuid < other.uuid; }
};
static_assert(serde::Serializable<PhysicalDiskId>);

enum class StorageRole : uint8_t {
  INVALID = 0,
  USER_DATA = 1,
  CACHE_ONLY = 2,
};

}  // namespace hf3fs::storage
