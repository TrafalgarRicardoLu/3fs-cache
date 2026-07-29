#include "client/cache/CacheReadPipeline.h"

namespace hf3fs::client::cache {

CoTryTask<size_t> CacheReadPipeline::read(const meta::Inode &inode,
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
  co_return co_await missReader_.read(origin.object, origin.length, origin.layout.chunkSize, offset, output);
}

}  // namespace hf3fs::client::cache
