#include "client/cache/ReadPlanner.h"

#include <algorithm>

namespace hf3fs::client::cache {

CoTryTask<meta::GetFileReadPlanRsp> MetaReadPlanSource::fetch(meta::GetFileReadPlanReq request) {
  co_return co_await client_.getFileReadPlan(std::move(request));
}

CoTryTask<BatchedReadPlan> ReadPlanner::plan(const flat::UserInfo &user,
                                             const meta::Inode &inode,
                                             const meta::SessionInfo &session,
                                             uint64_t offset,
                                             uint64_t length) {
  if (!source_ || !session.valid()) co_return makeError(StatusCode::kInvalidArg, "invalid cache read planner session");
  meta::GetFileReadPlanReq request;
  request.user = user;
  request.client = session.client;
  request.openSessionId = session.session;
  request.inode = inode.id;
  request.offset = offset;
  request.length = length;
  request.cacheProtocolVersion = hf3fs::cache::kCacheProtocolVersion;
  auto result = co_await ReadPlanBatcher::fetch(inode, request, [source = source_](meta::GetFileReadPlanReq batch) {
    return source->fetch(std::move(batch));
  });
  CO_RETURN_ON_ERROR(result);
  CO_RETURN_ON_ERROR(validate(inode, {offset, length}, *result));
  co_return std::move(*result);
}

Result<Void> ReadPlanner::validate(const meta::Inode &inode,
                                   hf3fs::cache::ByteRange request,
                                   const BatchedReadPlan &plan) {
  if (plan.inode != inode.id || plan.object != inode.asOriginFile().object) {
    return makeError(CacheCode::kVersionMismatch, "read plan identity does not match opened inode");
  }
  if (request.empty() || request.offset >= inode.fileLength()) return Void{};
  const auto requestEnd = std::min(inode.fileLength(), request.offset + request.length);
  auto covered = request.offset;
  for (const auto &block : plan.blocks) {
    RETURN_ON_ERROR(block.key.valid());
    auto blockEnd = block.fileRange.end();
    RETURN_ON_ERROR(blockEnd);
    if (inode.fileLayout().chunkSize == 0 || block.key.inode != inode.id.u64() ||
        block.key.block.toUnderType() != block.fileRange.offset / inode.fileLayout().chunkSize ||
        !static_cast<bool>(block.chunkId) || block.chainId == flat::ChainId{} || block.actualBlockLength == 0 ||
        block.actualBlockLength != block.fileRange.length || block.originRange != block.fileRange ||
        *blockEnd > inode.fileLength()) {
      return makeError(CacheCode::kInvalidResponse, "invalid block in cache read plan");
    }
    if (*blockEnd <= covered || block.fileRange.offset >= requestEnd) continue;
    if (block.fileRange.offset > covered) {
      return makeError(CacheCode::kInvalidResponse, "cache read plan contains a gap");
    }
    covered = std::min(requestEnd, *blockEnd);
    if (block.state == hf3fs::cache::CacheBlockState::READY) {
      if (!block.ready) return makeError(CacheCode::kInvalidResponse, "READY block has no identity");
      RETURN_ON_ERROR(block.ready->valid());
      if (block.ready->loadEpoch != block.loadEpoch || block.ready->blockLength != block.actualBlockLength) {
        return makeError(CacheCode::kInvalidResponse, "READY block identity does not match read plan");
      }
    }
  }
  if (covered != requestEnd) return makeError(CacheCode::kInvalidResponse, "cache read plan does not cover request");
  return Void{};
}

}  // namespace hf3fs::client::cache
