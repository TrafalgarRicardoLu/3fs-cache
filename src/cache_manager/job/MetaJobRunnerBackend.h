#pragma once

#include "cache_manager/job/JobRunner.h"
#include "cache_manager/service/EnsureCached.h"
#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

class MetaJobRunnerBackend final : public JobRunnerBackend {
 public:
  MetaJobRunnerBackend(std::shared_ptr<meta::client::MetaClient> metaClient,
                       meta::CacheServiceIdentity service,
                       EnsureCached &admission)
      : metaClient_(std::move(metaClient)),
        service_(std::move(service)),
        admission_(admission) {}

  CoTryTask<meta::ListPrefetchPlanRsp> list(cache::PrefetchJobId jobId,
                                            std::optional<cache::CacheBlockKey> after,
                                            uint32_t limit) override;
  CoTryTask<cache::PrefetchPlanEntry> update(const cache::PrefetchPlanEntry &expected,
                                             const cache::PrefetchPlanEntry &desired) override;
  CoTryTask<JobAdmissionResult> admit(const cache::PrefetchPlanEntry &entry, Completion completion) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  meta::CacheServiceIdentity service_;
  EnsureCached &admission_;
};

}  // namespace hf3fs::cache_manager
