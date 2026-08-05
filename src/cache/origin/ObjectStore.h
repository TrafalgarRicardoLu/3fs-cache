#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache::origin {

struct ObjectMetadata {
  ImmutableObjectIdentity identity;
  uint64_t size{0};
};

struct ListObjectsRequest {
  OriginId originId;
  std::string bucket;
  std::string prefix;
  std::string continuation;
  uint32_t maxKeys{1000};

  Result<Void> valid() const;
};

struct ListObjectsPage {
  std::vector<ObjectMetadata> objects;
  std::string nextContinuation;
  bool done{false};
};

class ObjectStore {
 public:
  virtual ~ObjectStore() = default;

  virtual CoTryTask<ObjectMetadata> head(const ObjectRef &object) = 0;
  virtual CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) = 0;
  virtual CoTryTask<ListObjectsPage> listObjects(const ListObjectsRequest &request);
};

}  // namespace hf3fs::cache::origin
