#pragma once

#include <optional>
#include <span>

#include "client/cache/OriginMissReader.h"
#include "fbs/meta/Schema.h"

namespace hf3fs::client::cache {

class CacheReadPipeline {
 public:
  explicit CacheReadPipeline(OriginMissReader &missReader)
      : missReader_(missReader) {}

  CoTryTask<size_t> read(const meta::Inode &inode,
                         const std::optional<meta::SessionInfo> &session,
                         uint64_t offset,
                         std::span<uint8_t> output);

 private:
  OriginMissReader &missReader_;
};

}  // namespace hf3fs::client::cache
