#include "client/cache/EnsureCachedReporter.h"

#include "common/net/RequestOptions.h"

namespace hf3fs::client::cache {

net::UserRequestOptions EnsureCachedReporter::options() const {
  net::UserRequestOptions result;
  result.timeout = timeout_;
  result.sendRetryTimes = 0;
  return result;
}

CoTryTask<void> EnsureCachedReporter::ensure(meta::InodeId inode, hf3fs::cache::CacheBlockIndex block) {
  cache_manager::EnsureCachedReq request;
  request.service = service_;
  request.inode = inode;
  request.beginBlock = block;
  request.blockCount = 1;
  request.reason = cache_manager::EnsureReason::FOREGROUND_MISS;
  request.cacheProtocolVersion = hf3fs::cache::kCacheProtocolVersion;
  auto result = co_await stub_.ensureCached(request, options());
  CO_RETURN_ON_ERROR(result);
  co_return Void{};
}

CoTryTask<void> EnsureCachedReporter::report(const flat::UserInfo &user,
                                             const meta::SessionInfo &session,
                                             const meta::ReadBlockPlan &plan,
                                             cache_manager::InvalidReason reason) {
  if (!plan.ready) co_return makeError(CacheCode::kInvalidResponse, "cannot report cache block without READY identity");
  cache_manager::ReportCacheBlockInvalidReq request;
  request.user = user;
  request.openSessionId = session.session;
  request.inode = meta::InodeId{plan.key.inode};
  request.block = plan.key.block;
  request.expectedReady = *plan.ready;
  request.observedGeneration = plan.ready->cacheGeneration;
  request.reason = reason;
  request.cacheProtocolVersion = hf3fs::cache::kCacheProtocolVersion;
  auto result = co_await stub_.reportCacheBlockInvalid(request, options());
  CO_RETURN_ON_ERROR(result);
  co_return Void{};
}

}  // namespace hf3fs::client::cache
