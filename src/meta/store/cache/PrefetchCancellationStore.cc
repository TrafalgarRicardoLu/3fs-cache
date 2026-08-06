#include "meta/store/cache/PrefetchCancellationStore.h"

#include <limits>

namespace hf3fs::meta::server {

CoTryTask<cache::PrefetchJobRecord> PrefetchCancellationStore::cancel(kv::IReadWriteTransaction &txn,
                                                                      cache::PrefetchJobId jobId) {
  auto loaded = co_await PrefetchJobStore::load(txn, jobId);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch Job not found");
  auto job = std::move(**loaded);
  if (job.state == cache::PrefetchJobState::CANCELLED) co_return job;
  if (job.state == cache::PrefetchJobState::READY || job.state == cache::PrefetchJobState::FAILED) {
    co_return makeError(CacheCode::kStateConflict, "completed prefetch Job cannot be cancelled");
  }
  if (job.stateVersion == std::numeric_limits<uint64_t>::max() ||
      job.updatedAtMs == std::numeric_limits<uint64_t>::max() ||
      job.cancelEpoch == std::numeric_limits<uint64_t>::max()) {
    co_return makeError(CacheCode::kStateConflict, "prefetch Job cancellation fence exhausted");
  }
  auto expected = job.stateVersion;
  job.state = cache::PrefetchJobState::CANCELLED;
  ++job.cancelEpoch;
  ++job.stateVersion;
  ++job.updatedAtMs;
  co_return co_await PrefetchJobStore::update(txn, expected, job);
}

}  // namespace hf3fs::meta::server
