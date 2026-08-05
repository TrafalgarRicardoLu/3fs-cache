#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>

#include "cache_manager/scheduler/JobQuota.h"
#include "common/utils/Coroutine.h"
#include "fbs/meta/Service.h"

namespace hf3fs::cache_manager {

enum class JobAdmissionDisposition : uint8_t {
  READY,
  QUEUED,
  LOADING,
  CAPACITY_WAIT,
  RETRYABLE,
  TERMINAL,
};

struct JobAdmissionResult {
  JobAdmissionDisposition disposition{JobAdmissionDisposition::RETRYABLE};
  std::optional<cache::ReadyIdentity> ready;
};

class JobRunnerBackend {
 public:
  using Completion = std::function<void(const Status &)>;

  virtual ~JobRunnerBackend() = default;
  virtual CoTryTask<meta::ListPrefetchPlanRsp> list(cache::PrefetchJobId jobId,
                                                    std::optional<cache::CacheBlockKey> after,
                                                    uint32_t limit) = 0;
  virtual CoTryTask<cache::PrefetchPlanEntry> update(const cache::PrefetchPlanEntry &expected,
                                                     const cache::PrefetchPlanEntry &desired) = 0;
  virtual CoTryTask<JobAdmissionResult> admit(const cache::PrefetchPlanEntry &entry, Completion completion) = 0;
};

struct JobRunnerPageResult {
  uint32_t visited{0};
  uint32_t ready{0};
  uint32_t attached{0};
  uint32_t retryable{0};
  uint32_t failed{0};
  bool throttled{false};
  bool more{false};
  std::optional<cache::CacheBlockKey> nextAfter;
};

class JobRunner {
 public:
  using AttemptFactory = std::function<Uuid()>;

  JobRunner(std::shared_ptr<JobRunnerBackend> backend,
            JobQuota &quota,
            uint32_t pageLimit = cache::kMaxPhase2BatchItems,
            AttemptFactory attemptFactory = Uuid::random);

  CoTryTask<JobRunnerPageResult> runNextPage(const cache::PrefetchJobRecord &job,
                                             std::optional<cache::CacheBlockKey> after = std::nullopt,
                                             const CancellationToken &cancellation = {});

 private:
  struct ClaimKey {
    cache::PrefetchJobId jobId;
    cache::CacheBlockKey block;
  };
  struct ClaimKeyLess {
    bool operator()(const ClaimKey &lhs, const ClaimKey &rhs) const;
  };
  struct SharedClaims {
    std::mutex mutex;
    std::map<ClaimKey, std::shared_ptr<JobQuota::Permit>, ClaimKeyLess> permits;
  };

  Result<std::shared_ptr<JobQuota::Permit>> acquire(const cache::PrefetchPlanEntry &entry,
                                                    const CancellationToken &cancellation);
  void release(const cache::PrefetchPlanEntry &entry);
  JobRunnerBackend::Completion completion(const cache::PrefetchPlanEntry &entry);

  std::shared_ptr<JobRunnerBackend> backend_;
  JobQuota &quota_;
  uint32_t pageLimit_;
  AttemptFactory attemptFactory_;
  std::shared_ptr<SharedClaims> claims_{std::make_shared<SharedClaims>()};
};

}  // namespace hf3fs::cache_manager
