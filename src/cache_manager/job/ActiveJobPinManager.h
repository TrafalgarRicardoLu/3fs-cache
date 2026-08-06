#pragma once

#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

class ActiveJobPinBackend {
 public:
  virtual ~ActiveJobPinBackend() = default;
  virtual CoTryTask<meta::ListPrefetchJobsRsp> listJobs(std::optional<cache::PrefetchJobId> after, uint32_t limit) = 0;
  virtual CoTryTask<meta::ListPrefetchPlanRsp> listPlan(cache::PrefetchJobId jobId,
                                                        std::optional<cache::CacheBlockKey> after,
                                                        uint32_t limit) = 0;
  virtual CoTryTask<void> upsert(std::vector<cache::PinRecord> pins) = 0;
  virtual CoTryTask<void> remove(cache::PinOwner owner) = 0;
};

class MetaActiveJobPinBackend final : public ActiveJobPinBackend {
 public:
  MetaActiveJobPinBackend(std::shared_ptr<meta::client::MetaClient> metaClient, meta::CacheServiceIdentity service)
      : metaClient_(std::move(metaClient)),
        service_(std::move(service)) {}

  CoTryTask<meta::ListPrefetchJobsRsp> listJobs(std::optional<cache::PrefetchJobId> after, uint32_t limit) override;
  CoTryTask<meta::ListPrefetchPlanRsp> listPlan(cache::PrefetchJobId jobId,
                                                std::optional<cache::CacheBlockKey> after,
                                                uint32_t limit) override;
  CoTryTask<void> upsert(std::vector<cache::PinRecord> pins) override;
  CoTryTask<void> remove(cache::PinOwner owner) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  meta::CacheServiceIdentity service_;
};

class ActiveJobPinManager {
 public:
  using WallClockMs = std::function<uint64_t()>;

  ActiveJobPinManager(std::shared_ptr<ActiveJobPinBackend> backend,
                      uint32_t jobPageLimit,
                      uint32_t planPageLimit,
                      uint64_t ttlMs,
                      WallClockMs wallClockMs = {});

  CoTryTask<void> runOnce();

 private:
  static cache::PinOwner owner(cache::PrefetchJobId jobId);
  static bool retain(const cache::PrefetchJobRecord &job);
  CoTryTask<void> reconcile(const cache::PrefetchJobRecord &job, uint64_t expiresAtMs);

  std::shared_ptr<ActiveJobPinBackend> backend_;
  uint32_t jobPageLimit_;
  uint32_t planPageLimit_;
  uint64_t ttlMs_;
  WallClockMs wallClockMs_;
};

}  // namespace hf3fs::cache_manager
