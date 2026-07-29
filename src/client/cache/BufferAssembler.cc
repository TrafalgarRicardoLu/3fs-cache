#include "client/cache/BufferAssembler.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace hf3fs::client::cache {

Result<size_t> BufferAssembler::assemble(hf3fs::cache::ByteRange request,
                                         uint64_t objectSize,
                                         std::span<uint8_t> output,
                                         std::vector<OriginRangeData> ranges) {
  if (request.empty() || request.offset >= objectSize) return size_t{0};
  const auto available = objectSize - request.offset;
  const auto resultLength = std::min(request.length, available);
  if (output.size() < resultLength) {
    return makeError(StatusCode::kInvalidArg, "origin read output buffer is too small");
  }
  const auto requestEnd = request.offset + resultLength;

  std::sort(ranges.begin(), ranges.end(), [](const auto &lhs, const auto &rhs) {
    return lhs.range.offset < rhs.range.offset;
  });
  uint64_t covered = request.offset;
  for (const auto &range : ranges) {
    auto rangeEnd = range.range.end();
    RETURN_ON_ERROR(rangeEnd);
    if (range.data.size() != range.range.length || *rangeEnd > objectSize) {
      return makeError(CacheCode::kInvalidResponse, "origin range body does not match its range");
    }
    if (*rangeEnd <= covered || range.range.offset >= requestEnd) continue;
    if (range.range.offset > covered) {
      return makeError(CacheCode::kInvalidResponse, "origin range results contain a gap");
    }
    covered = std::min(requestEnd, *rangeEnd);
  }
  if (covered != requestEnd) {
    return makeError(CacheCode::kInvalidResponse, "origin range results do not cover the read");
  }

  for (const auto &range : ranges) {
    const auto rangeEnd = range.range.offset + range.range.length;
    const auto copyBegin = std::max(request.offset, range.range.offset);
    const auto copyEnd = std::min(requestEnd, rangeEnd);
    if (copyBegin >= copyEnd) continue;
    std::memcpy(output.data() + (copyBegin - request.offset),
                range.data.data() + (copyBegin - range.range.offset),
                copyEnd - copyBegin);
  }
  return static_cast<size_t>(resultLength);
}

}  // namespace hf3fs::client::cache
