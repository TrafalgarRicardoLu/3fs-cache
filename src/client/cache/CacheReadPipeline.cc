#include "client/cache/CacheReadPipeline.h"

#include <algorithm>

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
    co_return co_await missReader_.read(origin.object, origin.length, origin.layout.chunkSize, offset, output);
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
      auto hit = co_await hitReader_->readFullBlock(block, user);
      if (hit.hasValue()) {
        ranges.push_back({block.fileRange, std::move(*hit)});
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
    ranges.push_back({{segmentBegin, data.size()}, std::move(data)});
    if (reporter_) static_cast<void>(co_await reporter_->ensure(inode.id, block.key.block));
  }
  co_return BufferAssembler::assemble({offset, output.size()}, origin.length, output, std::move(ranges));
}

}  // namespace hf3fs::client::cache
