#include "client/cache/OriginRangePlanner.h"

#include <algorithm>
#include <limits>

namespace hf3fs::client::cache {

Result<std::vector<hf3fs::cache::ByteRange>> OriginRangePlanner::plan(uint64_t objectSize,
                                                                      uint32_t blockSize,
                                                                      hf3fs::cache::ByteRange request,
                                                                      uint64_t maxRangeBytes) {
  if (blockSize == 0 || maxRangeBytes == 0) {
    return makeError(StatusCode::kInvalidArg, "origin range planner has a zero limit");
  }
  if (blockSize > maxRangeBytes) {
    return makeError(CacheCode::kRequestTooLarge, "origin block exceeds the maximum range size");
  }
  if (request.empty() || request.offset >= objectSize) return std::vector<hf3fs::cache::ByteRange>{};

  uint64_t requestEnd = objectSize;
  if (request.length <= std::numeric_limits<uint64_t>::max() - request.offset) {
    requestEnd = std::min(objectSize, request.offset + request.length);
  }
  if (requestEnd <= request.offset) return std::vector<hf3fs::cache::ByteRange>{};

  const uint64_t alignedBegin = request.offset - request.offset % blockSize;
  const uint64_t endBase = requestEnd - requestEnd % blockSize;
  uint64_t alignedEnd = requestEnd;
  if (endBase != requestEnd) {
    const auto padding = uint64_t{blockSize} - requestEnd % blockSize;
    alignedEnd = requestEnd <= std::numeric_limits<uint64_t>::max() - padding ? requestEnd + padding : objectSize;
  }
  alignedEnd = std::min(alignedEnd, objectSize);

  const uint64_t blocksPerRange = maxRangeBytes / blockSize;
  const uint64_t fullRangeBytes = blocksPerRange * uint64_t{blockSize};
  std::vector<hf3fs::cache::ByteRange> ranges;
  for (uint64_t begin = alignedBegin; begin < alignedEnd;) {
    const auto length = std::min(fullRangeBytes, alignedEnd - begin);
    ranges.push_back({begin, length});
    begin += length;
  }
  return ranges;
}

}  // namespace hf3fs::client::cache
