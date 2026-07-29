#pragma once

#include <optional>
#include <span>

#include "client/cache/CacheHitReader.h"
#include "client/cache/EnsureCachedReporter.h"
#include "client/cache/OriginMissReader.h"
#include "client/cache/ReadPlanner.h"
#include "fbs/meta/Schema.h"

namespace hf3fs::client::cache {

class CacheReadPipeline {
 public:
  explicit CacheReadPipeline(OriginMissReader &missReader)
      : missReader_(missReader) {}

  CacheReadPipeline(OriginMissReader &missReader,
                    ReadPlanner &planner,
                    ICacheHitReader &hitReader,
                    IEnsureCachedReporter *reporter = nullptr)
      : missReader_(missReader),
        planner_(&planner),
        hitReader_(&hitReader),
        reporter_(reporter) {}

  CoTryTask<size_t> read(const meta::Inode &inode,
                         const std::optional<meta::SessionInfo> &session,
                         uint64_t offset,
                         std::span<uint8_t> output);

  CoTryTask<size_t> read(const flat::UserInfo &user,
                         const meta::Inode &inode,
                         const std::optional<meta::SessionInfo> &session,
                         uint64_t offset,
                         std::span<uint8_t> output);

 private:
  static cache_manager::InvalidReason invalidReason(const Status &status);

  OriginMissReader &missReader_;
  ReadPlanner *planner_{nullptr};
  ICacheHitReader *hitReader_{nullptr};
  IEnsureCachedReporter *reporter_{nullptr};
};

}  // namespace hf3fs::client::cache
