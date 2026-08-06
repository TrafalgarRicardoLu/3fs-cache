#pragma once

#include "meta/store/cache/PrefetchJobStore.h"

namespace hf3fs::meta::server {

uint32_t prefetchReadyRatioBps(uint64_t readyBytes, uint64_t plannedBytes);
bool meetsPrefetchReadyRequirement(uint64_t readyBytes, uint64_t plannedBytes, uint32_t requiredReadyBps);

class PrefetchJobStateMachine {
 public:
  static CoTryTask<cache::PrefetchJobRecord> advance(kv::IReadWriteTransaction &txn,
                                                     cache::PrefetchJobId jobId,
                                                     uint64_t expectedStateVersion);
};

}  // namespace hf3fs::meta::server
