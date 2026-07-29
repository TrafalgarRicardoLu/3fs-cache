#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::client::cache {

struct OriginRangeData {
  hf3fs::cache::ByteRange range;
  std::vector<uint8_t> data;
};

class BufferAssembler {
 public:
  static Result<size_t> assemble(hf3fs::cache::ByteRange request,
                                 uint64_t objectSize,
                                 std::span<uint8_t> output,
                                 std::vector<OriginRangeData> ranges);
};

}  // namespace hf3fs::client::cache
