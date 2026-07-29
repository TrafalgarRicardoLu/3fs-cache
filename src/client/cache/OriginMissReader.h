#pragma once

#include <cstdint>
#include <span>

#include "cache/origin/ObjectStore.h"
#include "client/cache/LocalMissSingleflight.h"
#include "common/utils/Coroutine.h"

namespace hf3fs::client::cache {

struct OriginMissReaderConfig {
  uint64_t maxRangeBytes{256ULL << 20};
  uint64_t maxInflightBytes{256ULL << 20};
  uint32_t maxConcurrentRequests{32};
};

class OriginMissReader {
 public:
  OriginMissReader(hf3fs::cache::origin::ObjectStore &store,
                   LocalMissSingleflight &singleflight,
                   OriginMissReaderConfig config = {});

  CoTryTask<size_t> read(const hf3fs::cache::ImmutableObjectIdentity &object,
                         uint64_t objectSize,
                         uint32_t blockSize,
                         uint64_t offset,
                         std::span<uint8_t> output);

 private:
  hf3fs::cache::origin::ObjectStore &store_;
  LocalMissSingleflight &singleflight_;
  OriginMissReaderConfig config_;
};

}  // namespace hf3fs::client::cache
