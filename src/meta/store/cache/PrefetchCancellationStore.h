#pragma once

#include "meta/store/cache/PrefetchJobStore.h"

namespace hf3fs::meta::server {

class PrefetchCancellationStore {
 public:
  static CoTryTask<cache::PrefetchJobRecord> cancel(kv::IReadWriteTransaction &txn, cache::PrefetchJobId jobId);
};

}  // namespace hf3fs::meta::server
