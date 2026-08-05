#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>

#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "common/utils/UtcTime.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache_manager {

struct JobQuotaConfig {
  uint32_t maxParallelLoads{1};
  uint64_t bandwidthLimitBytesPerSec{0};

  Result<Void> valid() const;
  bool operator==(const JobQuotaConfig &) const = default;
};

class JobQuota {
 private:
  struct SharedState;

 public:
  using Clock = std::function<SteadyTime()>;

  class Permit {
   public:
    Permit() = default;
    Permit(const Permit &) = delete;
    Permit &operator=(const Permit &) = delete;
    Permit(Permit &&other) noexcept;
    Permit &operator=(Permit &&other) noexcept;
    ~Permit();

   private:
    Permit(std::shared_ptr<SharedState> state, cache::PrefetchJobId jobId);
    void release();

    std::shared_ptr<SharedState> state_;
    cache::PrefetchJobId jobId_;
    friend class JobQuota;
  };

  explicit JobQuota(Clock clock = SteadyClock::now);

  Result<Void> registerJob(cache::PrefetchJobId jobId, JobQuotaConfig config);
  Result<Permit> tryAcquire(cache::PrefetchJobId jobId, uint64_t bytes, const CancellationToken &cancellation = {});
  Result<Void> removeJob(cache::PrefetchJobId jobId);
  uint32_t inflight(cache::PrefetchJobId jobId) const;

 private:
  struct Bucket {
    JobQuotaConfig config;
    uint32_t inflight{0};
    uint64_t tokens{0};
    uint64_t refillRemainder{0};
    SteadyTime lastRefill;
  };

  struct SharedState {
    mutable std::mutex mutex;
    std::map<Uuid, Bucket> jobs;
  };

  static void release(const std::shared_ptr<SharedState> &state, cache::PrefetchJobId jobId);
  static void refill(Bucket &bucket, SteadyTime now);

  Clock clock_;
  std::shared_ptr<SharedState> state_;
};

}  // namespace hf3fs::cache_manager
