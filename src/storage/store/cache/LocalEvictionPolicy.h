#pragma once

#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "common/utils/Result.h"
#include "fbs/storage/Common.h"

namespace hf3fs::storage {

struct LocalEvictionCandidate {
  ChunkId chunkId;
  CacheChunkDescriptor descriptor;
  uint64_t footprintBytes{0};
};

struct LocalEvictionContext {
  uint64_t bytesToRelease{0};
  uint64_t nowNs{0};
  uint64_t protectionPeriodNs{0};
};

class LocalEvictionPolicy {
 public:
  virtual ~LocalEvictionPolicy() = default;
  virtual Result<std::vector<size_t>> select(std::span<const LocalEvictionCandidate> candidates,
                                             const LocalEvictionContext &context) = 0;
};

Result<std::unique_ptr<LocalEvictionPolicy>> createLocalEvictionPolicy(std::string_view name);

}  // namespace hf3fs::storage
