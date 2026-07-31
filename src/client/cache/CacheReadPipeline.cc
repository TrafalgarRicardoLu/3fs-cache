#include "client/cache/CacheReadPipeline.h"

#include <algorithm>
#include <chrono>

#include "cache/metrics/CacheMetrics.h"
#include "client/cache/BufferAssembler.h"

namespace hf3fs::client::cache {

CoTryTask<size_t> CacheReadPipeline::read(const meta::Inode &inode,
                                          const std::optional<meta::SessionInfo> &session,
                                          uint64_t offset,
                                          std::span<uint8_t> output) {
  co_return co_await read(flat::UserInfo{}, inode, session, offset, output);
}

cache_manager::InvalidReason CacheReadPipeline::invalidReason(const Status &status) {
  if (status.code() == StorageClientCode::kChecksumMismatch || status.code() == StorageCode::kChecksumMismatch) {
    return cache_manager::InvalidReason::CHECKSUM_MISMATCH;
  }
  if (status.code() == CacheCode::kStaleGeneration || status.code() == CacheCode::kGenerationAdvanced) {
    return cache_manager::InvalidReason::GENERATION_MISMATCH;
  }
  return cache_manager::InvalidReason::NOT_FOUND;
}

CoTryTask<size_t> CacheReadPipeline::read(const flat::UserInfo &user,
                                          const meta::Inode &inode,
                                          const std::optional<meta::SessionInfo> &session,
                                          uint64_t offset,
                                          std::span<uint8_t> output) {
  if (!inode.isOriginFile()) {
    co_return makeError(StatusCode::kInvalidArg, "regular files must use the native storage read path");
  }
  if (session.has_value() && !session->valid()) {
    co_return makeError(StatusCode::kInvalidArg, "invalid origin file open session");
  }
  const auto &origin = inode.asOriginFile();
  if (!planner_) {
    auto result = co_await missReader_.read(origin.object, origin.length, origin.layout.chunkSize, offset, output);
    if (result.hasValue()) {
      hf3fs::cache::metrics::recordCount(hf3fs::cache::metrics::Event::CLIENT_MISS_BYTES,
                                         *result,
                                         {.inode = inode.id.u64(), .originId = origin.object.originId.toUnderType()});
      hf3fs::cache::metrics::recordCount(hf3fs::cache::metrics::Event::CLIENT_ORIGIN_BYTES,
                                         *result,
                                         {.inode = inode.id.u64(), .originId = origin.object.originId.toUnderType()});
    }
    co_return result;
  }
  if (!session) co_return makeError(StatusCode::kInvalidArg, "cache read requires an open session");
  if (output.empty() || offset >= origin.length) co_return size_t{0};

  auto planned = co_await planner_->plan(user, inode, *session, offset, output.size());
  CO_RETURN_ON_ERROR(planned);
  const auto requestEnd = offset + std::min<uint64_t>(origin.length - offset, output.size());
  std::vector<OriginRangeData> ranges;
  ranges.reserve(planned->blocks.size());
  for (const auto &block : planned->blocks) {
    const auto blockEnd = block.fileRange.offset + block.fileRange.length;
    const auto segmentBegin = std::max(offset, block.fileRange.offset);
    const auto segmentEnd = std::min(requestEnd, blockEnd);
    if (segmentBegin >= segmentEnd) continue;

    if (block.state == hf3fs::cache::CacheBlockState::READY) {
      auto storageStart = std::chrono::steady_clock::now();
      auto hit = co_await hitReader_->readFullBlock(block, user);
      hf3fs::cache::metrics::recordLatency(
          hf3fs::cache::metrics::Event::CLIENT_STORAGE_READ,
          std::chrono::steady_clock::now() - storageStart,
          {.inode = inode.id.u64(),
           .block = block.key.block.toUnderType(),
           .originId = origin.object.originId.toUnderType(),
           .reason = hit.hasValue() ? "hit" : std::string(StatusCode::toString(hit.error().code()))});
      if (hit.hasValue()) {
        hf3fs::cache::metrics::recordCount(hf3fs::cache::metrics::Event::CLIENT_HIT_BYTES,
                                           segmentEnd - segmentBegin,
                                           {.inode = inode.id.u64(),
                                            .block = block.key.block.toUnderType(),
                                            .originId = origin.object.originId.toUnderType()});
        ranges.push_back({block.fileRange, std::move(*hit)});
        if (accessReporter_ && block.ready) {
          (void)accessReporter_->record(user, block.key, block.ready->cacheGeneration);
        }
        continue;
      }
      if (reporter_) {
        static_cast<void>(co_await reporter_->report(user, *session, block, invalidReason(hit.error())));
      }
    }

    std::vector<uint8_t> data(segmentEnd - segmentBegin);
    auto miss = co_await missReader_.read(origin.object, origin.length, origin.layout.chunkSize, segmentBegin, data);
    CO_RETURN_ON_ERROR(miss);
    if (*miss != data.size()) co_return makeError(CacheCode::kInvalidResponse, "short origin fallback read");
    hf3fs::cache::metrics::recordCount(hf3fs::cache::metrics::Event::CLIENT_MISS_BYTES,
                                       *miss,
                                       {.inode = inode.id.u64(),
                                        .block = block.key.block.toUnderType(),
                                        .originId = origin.object.originId.toUnderType()});
    hf3fs::cache::metrics::recordCount(hf3fs::cache::metrics::Event::CLIENT_ORIGIN_BYTES,
                                       *miss,
                                       {.inode = inode.id.u64(),
                                        .block = block.key.block.toUnderType(),
                                        .originId = origin.object.originId.toUnderType()});
    ranges.push_back({{segmentBegin, data.size()}, std::move(data)});
    if (reporter_) static_cast<void>(co_await reporter_->ensure(inode.id, block.key.block));
  }
  co_return BufferAssembler::assemble({offset, output.size()}, origin.length, output, std::move(ranges));
}

}  // namespace hf3fs::client::cache
