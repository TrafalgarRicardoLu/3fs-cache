#pragma once

#include <cstdint>
#include <vector>

#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache::origin {

struct ObjectMetadata {
  ImmutableObjectIdentity identity;
  uint64_t size{0};
};

class ObjectStore {
 public:
  virtual ~ObjectStore() = default;

  virtual CoTryTask<ObjectMetadata> head(const ObjectRef &object) = 0;
  virtual CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) = 0;
};

}  // namespace hf3fs::cache::origin
