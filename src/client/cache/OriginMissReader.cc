#include "client/cache/OriginMissReader.h"

#include <algorithm>
#include <chrono>

#include "cache/metrics/CacheMetrics.h"
#include "client/cache/BufferAssembler.h"
#include "client/cache/OriginRangePlanner.h"

namespace hf3fs::client::cache {

OriginMissReader::OriginMissReader(hf3fs::cache::origin::ObjectStore &store,
                                   LocalMissSingleflight &singleflight,
                                   OriginMissReaderConfig config)
    : store_(store),
      singleflight_(singleflight),
      config_(config) {}

CoTryTask<size_t> OriginMissReader::read(const hf3fs::cache::ImmutableObjectIdentity &object,
                                         uint64_t objectSize,
                                         uint32_t blockSize,
                                         uint64_t offset,
                                         std::span<uint8_t> output) {
  CO_RETURN_ON_ERROR(object.valid());
  if (config_.maxConcurrentRequests == 0 || config_.maxInflightBytes == 0) {
    co_return makeError(StatusCode::kInvalidConfig, "origin miss reader limits must be positive");
  }
  auto request = hf3fs::cache::ByteRange{offset, output.size()};
  auto maxRange = std::min(config_.maxRangeBytes, config_.maxInflightBytes);
  auto planned = OriginRangePlanner::plan(objectSize, blockSize, request, maxRange);
  CO_RETURN_ON_ERROR(planned);

  std::vector<OriginRangeData> results;
  results.reserve(planned->size());
  for (auto range : *planned) {
    auto start = std::chrono::steady_clock::now();
    auto data = co_await singleflight_.run(object, range, [&]() { return store_.getRange(object, range); });
    hf3fs::cache::metrics::recordLatency(
        hf3fs::cache::metrics::Event::CLIENT_ORIGIN_READ,
        std::chrono::steady_clock::now() - start,
        {.originId = object.originId.toUnderType(),
         .reason = data.hasValue() ? "success" : std::string(StatusCode::toString(data.error().code()))});
    CO_RETURN_ON_ERROR(data);
    results.push_back({range, std::move(*data)});
  }
  co_return BufferAssembler::assemble(request, objectSize, output, std::move(results));
}

}  // namespace hf3fs::client::cache
