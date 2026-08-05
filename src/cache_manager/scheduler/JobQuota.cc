#include "cache_manager/scheduler/JobQuota.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

#include "common/utils/Int128.h"

namespace hf3fs::cache_manager {
namespace {

constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000;

}  // namespace

Result<Void> JobQuotaConfig::valid() const {
  if (maxParallelLoads == 0 || maxParallelLoads > cache::kMaxJobParallelLoads) {
    return makeError(StatusCode::kInvalidConfig, "invalid Job parallel load quota");
  }
  return Void{};
}

JobQuota::Permit::Permit(std::shared_ptr<SharedState> state, cache::PrefetchJobId jobId)
    : state_(std::move(state)),
      jobId_(jobId) {}

JobQuota::Permit::Permit(Permit &&other) noexcept
    : state_(std::exchange(other.state_, {})),
      jobId_(std::exchange(other.jobId_, {})) {}

JobQuota::Permit &JobQuota::Permit::operator=(Permit &&other) noexcept {
  if (this != &other) {
    release();
    state_ = std::exchange(other.state_, {});
    jobId_ = std::exchange(other.jobId_, {});
  }
  return *this;
}

JobQuota::Permit::~Permit() { release(); }

void JobQuota::Permit::release() {
  if (!state_) return;
  JobQuota::release(state_, jobId_);
  state_.reset();
  jobId_ = {};
}

JobQuota::JobQuota(Clock clock)
    : clock_(std::move(clock)),
      state_(std::make_shared<SharedState>()) {}

Result<Void> JobQuota::registerJob(cache::PrefetchJobId jobId, JobQuotaConfig config) {
  if (jobId == cache::PrefetchJobId{}) return makeError(StatusCode::kInvalidArg, "Job quota ID is not set");
  RETURN_ON_ERROR(config.valid());
  auto lock = std::scoped_lock(state_->mutex);
  auto [found, inserted] =
      state_->jobs.emplace(jobId.toUnderType(), Bucket{config, 0, config.bandwidthLimitBytesPerSec, 0, clock_()});
  if (!inserted && found->second.config != config) {
    return makeError(CacheCode::kStateConflict, "Job quota is already registered with different limits");
  }
  return Void{};
}

void JobQuota::refill(Bucket &bucket, SteadyTime now) {
  if (bucket.config.bandwidthLimitBytesPerSec == 0 || now <= bucket.lastRefill) return;
  const auto elapsed =
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - bucket.lastRefill).count());
  uint128_t numerator =
      static_cast<uint128_t>(elapsed) * bucket.config.bandwidthLimitBytesPerSec + bucket.refillRemainder;
  const auto added = static_cast<uint64_t>(numerator / kNanosecondsPerSecond);
  bucket.refillRemainder = static_cast<uint64_t>(numerator % kNanosecondsPerSecond);
  bucket.tokens =
      std::min(bucket.config.bandwidthLimitBytesPerSec,
               added > bucket.config.bandwidthLimitBytesPerSec - bucket.tokens ? bucket.config.bandwidthLimitBytesPerSec
                                                                               : bucket.tokens + added);
  bucket.lastRefill = now;
}

Result<JobQuota::Permit> JobQuota::tryAcquire(cache::PrefetchJobId jobId,
                                              uint64_t bytes,
                                              const CancellationToken &cancellation) {
  if (cancellation.isCancellationRequested()) throw OperationCancelled();
  if (bytes == 0) return makeError(StatusCode::kInvalidArg, "Job quota request is empty");
  auto lock = std::scoped_lock(state_->mutex);
  auto found = state_->jobs.find(jobId.toUnderType());
  if (found == state_->jobs.end()) return makeError(StatusCode::kInvalidArg, "Job quota is not registered");
  auto &bucket = found->second;
  refill(bucket, clock_());
  if (bucket.inflight >= bucket.config.maxParallelLoads) {
    return makeError(CacheCode::kThrottled, "Job parallel load quota is exhausted");
  }
  if (bucket.config.bandwidthLimitBytesPerSec != 0 && bytes > bucket.tokens) {
    return makeError(CacheCode::kThrottled, "Job bandwidth quota is exhausted");
  }
  if (bucket.config.bandwidthLimitBytesPerSec != 0) bucket.tokens -= bytes;
  ++bucket.inflight;
  return Permit(state_, jobId);
}

Result<Void> JobQuota::removeJob(cache::PrefetchJobId jobId) {
  auto lock = std::scoped_lock(state_->mutex);
  auto found = state_->jobs.find(jobId.toUnderType());
  if (found == state_->jobs.end()) return Void{};
  if (found->second.inflight != 0) return makeError(CacheCode::kStateConflict, "Job quota still has inflight loads");
  state_->jobs.erase(found);
  return Void{};
}

uint32_t JobQuota::inflight(cache::PrefetchJobId jobId) const {
  auto lock = std::scoped_lock(state_->mutex);
  auto found = state_->jobs.find(jobId.toUnderType());
  return found == state_->jobs.end() ? 0 : found->second.inflight;
}

void JobQuota::release(const std::shared_ptr<SharedState> &state, cache::PrefetchJobId jobId) {
  auto lock = std::scoped_lock(state->mutex);
  auto found = state->jobs.find(jobId.toUnderType());
  if (found != state->jobs.end() && found->second.inflight != 0) --found->second.inflight;
}

}  // namespace hf3fs::cache_manager
