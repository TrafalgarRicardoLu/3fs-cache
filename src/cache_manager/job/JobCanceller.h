#pragma once

#include <mutex>

#include "cache_manager/loader/CacheLoader.h"
#include "cache_manager/scheduler/HintCoalescer.h"
#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

class JobCancellerBackend {
 public:
  virtual ~JobCancellerBackend() = default;
  virtual CoTryTask<cache::PrefetchJobRecord> cancelJob(cache::PrefetchJobId jobId) = 0;
  virtual CoTryTask<meta::ListPrefetchPlanRsp> list(cache::PrefetchJobId jobId,
                                                    std::optional<cache::CacheBlockKey> after,
                                                    uint32_t limit) = 0;
  virtual CoTryTask<cache::PrefetchPlanEntry> update(const cache::PrefetchPlanEntry &expected,
                                                     const cache::PrefetchPlanEntry &desired) = 0;
  virtual CoTryTask<void> cancelQueued(const cache::CacheBlockKey &key, const storage::PermitIdentity &permit) = 0;
};

class MetaJobCancellerBackend final : public JobCancellerBackend {
 public:
  MetaJobCancellerBackend(std::shared_ptr<meta::client::MetaClient> metaClient,
                          meta::CacheServiceIdentity service,
                          std::shared_ptr<CacheManagerBackend> cacheBackend)
      : metaClient_(std::move(metaClient)),
        service_(std::move(service)),
        cacheBackend_(std::move(cacheBackend)) {}

  CoTryTask<cache::PrefetchJobRecord> cancelJob(cache::PrefetchJobId jobId) override;
  CoTryTask<meta::ListPrefetchPlanRsp> list(cache::PrefetchJobId jobId,
                                            std::optional<cache::CacheBlockKey> after,
                                            uint32_t limit) override;
  CoTryTask<cache::PrefetchPlanEntry> update(const cache::PrefetchPlanEntry &expected,
                                             const cache::PrefetchPlanEntry &desired) override;
  CoTryTask<void> cancelQueued(const cache::CacheBlockKey &key, const storage::PermitIdentity &permit) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  meta::CacheServiceIdentity service_;
  std::shared_ptr<CacheManagerBackend> cacheBackend_;
};

struct JobCancellationResult {
  cache::PrefetchJobRecord job;
  uint64_t cancelledEntries{0};
  uint64_t cancelledQueuedLoads{0};
};

class JobCanceller {
 public:
  JobCanceller(std::shared_ptr<JobCancellerBackend> backend, HintCoalescer &hints, uint32_t pageLimit)
      : backend_(std::move(backend)),
        hints_(hints),
        pageLimit_(pageLimit) {}

  CoTryTask<JobCancellationResult> cancel(cache::PrefetchJobId jobId, const CancellationToken &cancellation = {});

 private:
  struct PendingPermit {
    cache::PrefetchJobId jobId;
    cache::CacheBlockKey key;
    storage::PermitIdentity permit;
  };

  std::optional<storage::PermitIdentity> pending(cache::PrefetchJobId jobId, const cache::CacheBlockKey &key) const;
  void remember(cache::PrefetchJobId jobId, const cache::CacheBlockKey &key, const storage::PermitIdentity &permit);
  void forget(cache::PrefetchJobId jobId, const cache::CacheBlockKey &key);

  std::shared_ptr<JobCancellerBackend> backend_;
  HintCoalescer &hints_;
  uint32_t pageLimit_;
  mutable std::mutex mutex_;
  std::vector<PendingPermit> pendingPermits_;
};

}  // namespace hf3fs::cache_manager
