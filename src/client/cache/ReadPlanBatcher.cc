#include "client/cache/ReadPlanBatcher.h"

#include <algorithm>
#include <iterator>
#include <limits>

namespace hf3fs::client::cache {
namespace {

CoTryTask<std::optional<BatchedReadPlan>> fetchOnce(const meta::Inode &openedInode,
                                                    const meta::GetFileReadPlanReq &request,
                                                    const ReadPlanBatcher::Fetch &fetch) {
  BatchedReadPlan combined;
  combined.inode = openedInode.id;
  combined.object = openedInode.asOriginFile().object;
  if (request.length == 0 || request.offset >= openedInode.fileLength()) {
    co_return combined;
  }

  const auto blockSize = uint64_t{openedInode.fileLayout().chunkSize};
  const auto end = std::min(openedInode.fileLength(), request.offset + request.length);
  for (uint64_t cursor = request.offset; cursor < end;) {
    const auto firstBlock = cursor / blockSize;
    uint64_t batchEnd = end;
    if (firstBlock <= std::numeric_limits<uint64_t>::max() - meta::kMaxCacheBatchItems) {
      const auto endBlock = firstBlock + meta::kMaxCacheBatchItems;
      if (endBlock <= std::numeric_limits<uint64_t>::max() / blockSize) {
        batchEnd = std::min(end, endBlock * blockSize);
      }
    }
    if (batchEnd <= cursor) co_return makeError(StatusCode::kInvalidArg, "read plan batch range overflow");

    auto batchRequest = request;
    batchRequest.offset = cursor;
    batchRequest.length = batchEnd - cursor;
    auto response = co_await fetch(std::move(batchRequest));
    CO_RETURN_ON_ERROR(response);
    if (response->inode != openedInode.id || combined.object != response->object) co_return std::nullopt;
    combined.blocks.insert(combined.blocks.end(),
                           std::make_move_iterator(response->blocks.begin()),
                           std::make_move_iterator(response->blocks.end()));
    cursor = batchEnd;
  }
  co_return combined;
}

}  // namespace

CoTryTask<BatchedReadPlan> ReadPlanBatcher::fetch(const meta::Inode &openedInode,
                                                  meta::GetFileReadPlanReq request,
                                                  Fetch fetch,
                                                  uint32_t maxReplans) {
  CO_RETURN_ON_ERROR(request.valid());
  if (!openedInode.isOriginFile() || request.inode != openedInode.id) {
    co_return makeError(StatusCode::kInvalidArg, "read plan request does not match the opened OriginFile");
  }
  for (uint64_t attempt = 0; attempt <= maxReplans; ++attempt) {
    auto result = co_await fetchOnce(openedInode, request, fetch);
    CO_RETURN_ON_ERROR(result);
    if (result->has_value()) co_return std::move(**result);
  }
  co_return makeError(CacheCode::kVersionMismatch, "read plan identity changed while batching");
}

}  // namespace hf3fs::client::cache
