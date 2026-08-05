#pragma once

#include "meta/store/cache/PrefetchPlanStore.h"

namespace hf3fs::meta::server {

struct TrackPrefetchReadyResult {
  cache::PrefetchJobRecord job;
  uint64_t currentReadyBytes{0};
  uint64_t currentReadyBlocks{0};
  bool more{false};
  std::optional<cache::CacheBlockKey> nextAfter;
};

class PrefetchReadyStore {
 public:
  static CoTryTask<TrackPrefetchReadyResult> track(kv::IReadWriteTransaction &txn,
                                                   cache::PrefetchJobId jobId,
                                                   std::optional<cache::CacheBlockKey> after,
                                                   uint32_t limit);
};

}  // namespace hf3fs::meta::server
