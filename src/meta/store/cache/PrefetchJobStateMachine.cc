#include "meta/store/cache/PrefetchJobStateMachine.h"

#include <algorithm>
#include <limits>

#include "common/utils/Int128.h"

namespace hf3fs::meta::server {

uint32_t prefetchReadyRatioBps(uint64_t readyBytes, uint64_t plannedBytes) {
  if (plannedBytes == 0) return 0;
  auto scaled = static_cast<uint128_t>(readyBytes) * cache::kReadyRatioScaleBps / plannedBytes;
  return static_cast<uint32_t>(std::min<uint128_t>(scaled, cache::kReadyRatioScaleBps));
}

bool meetsPrefetchReadyRequirement(uint64_t readyBytes, uint64_t plannedBytes, uint32_t requiredReadyBps) {
  if (plannedBytes == 0 || requiredReadyBps == 0 || requiredReadyBps > cache::kReadyRatioScaleBps) return false;
  return static_cast<uint128_t>(readyBytes) * cache::kReadyRatioScaleBps >=
         static_cast<uint128_t>(plannedBytes) * requiredReadyBps;
}

CoTryTask<cache::PrefetchJobRecord> PrefetchJobStateMachine::advance(kv::IReadWriteTransaction &txn,
                                                                     cache::PrefetchJobId jobId,
                                                                     uint64_t expectedStateVersion) {
  if (expectedStateVersion == 0) co_return makeError(StatusCode::kInvalidArg, "empty Job state fence");
  auto loaded = co_await PrefetchJobStore::load(txn, jobId);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch Job not found");
  auto job = std::move(**loaded);
  if (job.state == cache::PrefetchJobState::READY ||
      (job.state == cache::PrefetchJobState::FAILED && job.error == "EMPTY_PLAN")) {
    co_return job;
  }
  if (job.state == cache::PrefetchJobState::FAILED || job.state == cache::PrefetchJobState::CANCELLED) {
    co_return makeError(CacheCode::kStateConflict, "terminal prefetch Job cannot advance");
  }
  if (job.stateVersion != expectedStateVersion) {
    co_return makeError(CacheCode::kStateConflict, "prefetch Job state fence changed");
  }
  if (!job.planningComplete) co_return job;

  auto desiredState = cache::PrefetchJobState::LOADING;
  std::string error;
  if (job.plannedBytes == 0) {
    desiredState = cache::PrefetchJobState::FAILED;
    error = "EMPTY_PLAN";
  } else if (meetsPrefetchReadyRequirement(job.readyBytes, job.plannedBytes, job.spec.requiredReadyBps)) {
    desiredState = cache::PrefetchJobState::READY;
  } else if (job.readyBytes != 0) {
    desiredState = cache::PrefetchJobState::PARTIAL_READY;
  }
  if (job.state == desiredState && job.error == error) co_return job;
  if (job.stateVersion == std::numeric_limits<uint64_t>::max() ||
      job.updatedAtMs == std::numeric_limits<uint64_t>::max()) {
    co_return makeError(CacheCode::kStateConflict, "prefetch Job state version exhausted");
  }
  auto expected = job.stateVersion;
  job.state = desiredState;
  job.error = std::move(error);
  ++job.stateVersion;
  ++job.updatedAtMs;
  co_return co_await PrefetchJobStore::update(txn, expected, job);
}

}  // namespace hf3fs::meta::server
