#pragma once

#include <cstdint>
#include <vector>

#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::client::cache {

class OriginRangePlanner {
 public:
  static Result<std::vector<hf3fs::cache::ByteRange>> plan(uint64_t objectSize,
                                                           uint32_t blockSize,
                                                           hf3fs::cache::ByteRange request,
                                                           uint64_t maxRangeBytes);
};

}  // namespace hf3fs::client::cache
