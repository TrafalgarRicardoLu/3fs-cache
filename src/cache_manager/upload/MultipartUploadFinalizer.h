#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#include "cache/origin/ObjectStore.h"
#include "common/utils/Coroutine.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta::client {
class MetaClient;
}

namespace hf3fs::cache_manager {

struct MultipartUploadFinalizerConfig {
  uint32_t maxRetries{3};
  std::chrono::milliseconds initialBackoff{100};
  std::chrono::milliseconds maxBackoff{2000};

  Result<Void> valid() const;
};

class MultipartUploadFinalizerBackend {
 public:
  virtual ~MultipartUploadFinalizerBackend() = default;
  virtual CoTryTask<cache::origin::ObjectMetadata> complete(cache::origin::CompleteMultipartUploadRequest request) = 0;
  virtual CoTryTask<cache::origin::ObjectMetadata> headCompleted(cache::origin::HeadCompletedUploadRequest request) = 0;
  virtual CoTryTask<Void> abort(cache::origin::AbortMultipartUploadRequest request) = 0;
  virtual CoTryTask<cache::UploadJobRecord> mutate(cache::UploadJobRecord job,
                                                   meta::MultipartUploadMutation mutation,
                                                   std::optional<cache::ImmutableObjectIdentity> completed,
                                                   std::string error) = 0;
  virtual CoTryTask<Void> backoff(std::chrono::milliseconds delay) = 0;
  virtual bool cancelled() const = 0;
};

class MultipartUploadFinalizer {
 public:
  MultipartUploadFinalizer(std::shared_ptr<MultipartUploadFinalizerBackend> backend,
                           MultipartUploadFinalizerConfig config = {});

  CoTryTask<cache::UploadJobRecord> complete(cache::UploadJobRecord job);
  CoTryTask<cache::UploadJobRecord> cancel(cache::UploadJobRecord job, std::string reason);

 private:
  CoTryTask<cache::UploadJobRecord> mutateWithRetry(
      cache::UploadJobRecord job,
      meta::MultipartUploadMutation mutation,
      std::optional<cache::ImmutableObjectIdentity> completed = std::nullopt,
      std::string error = {});
  CoTryTask<cache::origin::ObjectMetadata> recoverCompleted(const cache::UploadJobRecord &job);
  CoTryTask<Void> waitBeforeRetry(uint32_t retry);
  Result<Void> checkCancelled() const;
  CoTryTask<Void> markTerminalFailure(cache::UploadJobRecord job, const Status &failure);

  std::shared_ptr<MultipartUploadFinalizerBackend> backend_;
  MultipartUploadFinalizerConfig config_;
};

class RealMultipartUploadFinalizerBackend final : public MultipartUploadFinalizerBackend {
 public:
  RealMultipartUploadFinalizerBackend(std::shared_ptr<meta::client::MetaClient> metaClient,
                                      std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                      std::string serviceName,
                                      std::string serviceToken);

  CoTryTask<cache::origin::ObjectMetadata> complete(cache::origin::CompleteMultipartUploadRequest request) final;
  CoTryTask<cache::origin::ObjectMetadata> headCompleted(cache::origin::HeadCompletedUploadRequest request) final;
  CoTryTask<Void> abort(cache::origin::AbortMultipartUploadRequest request) final;
  CoTryTask<cache::UploadJobRecord> mutate(cache::UploadJobRecord job,
                                           meta::MultipartUploadMutation mutation,
                                           std::optional<cache::ImmutableObjectIdentity> completed,
                                           std::string error) final;
  CoTryTask<Void> backoff(std::chrono::milliseconds delay) final;
  bool cancelled() const final;
  void cancel();

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::shared_ptr<cache::origin::ObjectStore> objectStore_;
  std::string serviceName_;
  std::string serviceToken_;
  std::atomic_bool cancelled_{false};
};

}  // namespace hf3fs::cache_manager
