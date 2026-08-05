#pragma once

#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

class JobTrackerBackend {
 public:
  virtual ~JobTrackerBackend() = default;
  virtual CoTryTask<meta::TrackPrefetchReadyRsp> track(cache::PrefetchJobId jobId,
                                                       std::optional<cache::CacheBlockKey> after,
                                                       uint32_t limit) = 0;
};

class MetaJobTrackerBackend final : public JobTrackerBackend {
 public:
  MetaJobTrackerBackend(std::shared_ptr<meta::client::MetaClient> metaClient, meta::CacheServiceIdentity service)
      : metaClient_(std::move(metaClient)),
        service_(std::move(service)) {}

  CoTryTask<meta::TrackPrefetchReadyRsp> track(cache::PrefetchJobId jobId,
                                               std::optional<cache::CacheBlockKey> after,
                                               uint32_t limit) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  meta::CacheServiceIdentity service_;
};

struct JobTrackerResult {
  cache::PrefetchJobRecord job;
  uint64_t currentReadyBytes{0};
  uint64_t currentReadyBlocks{0};
};

class JobTracker {
 public:
  JobTracker(std::shared_ptr<JobTrackerBackend> backend, uint32_t pageLimit)
      : backend_(std::move(backend)),
        pageLimit_(pageLimit) {}

  CoTryTask<JobTrackerResult> run(const cache::PrefetchJobRecord &job, const CancellationToken &cancellation = {});

 private:
  std::shared_ptr<JobTrackerBackend> backend_;
  uint32_t pageLimit_;
};

}  // namespace hf3fs::cache_manager
