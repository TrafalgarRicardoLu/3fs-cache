#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include "common/utils/Coroutine.h"
#include "common/utils/Duration.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta::client {
class MetaClient;
}

namespace hf3fs::client::cache {

struct UploadJobWaiterConfig {
  Duration pollInterval{100_ms};

  Result<Void> valid() const;
};

class UploadJobWaiterBackend {
 public:
  virtual ~UploadJobWaiterBackend() = default;
  virtual CoTryTask<hf3fs::cache::UploadJobRecord> get(flat::UserInfo user, hf3fs::cache::UploadJobId jobId) = 0;
  virtual CoTryTask<Void> wait(Duration delay) = 0;
  virtual std::chrono::steady_clock::time_point now() const = 0;
};

class MetaUploadJobWaiterBackend final : public UploadJobWaiterBackend {
 public:
  explicit MetaUploadJobWaiterBackend(std::shared_ptr<meta::client::MetaClient> metaClient)
      : metaClient_(std::move(metaClient)) {}

  CoTryTask<hf3fs::cache::UploadJobRecord> get(flat::UserInfo user, hf3fs::cache::UploadJobId jobId) final;
  CoTryTask<Void> wait(Duration delay) final;
  std::chrono::steady_clock::time_point now() const final;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
};

class UploadJobWaiter {
 public:
  using Cancelled = std::function<bool()>;

  UploadJobWaiter(std::shared_ptr<UploadJobWaiterBackend> backend, UploadJobWaiterConfig config = {});

  CoTryTask<hf3fs::cache::UploadJobRecord> query(flat::UserInfo user, hf3fs::cache::UploadJobId jobId);
  CoTryTask<hf3fs::cache::UploadJobRecord> awaitTerminal(flat::UserInfo user,
                                                         hf3fs::cache::UploadJobId jobId,
                                                         std::chrono::steady_clock::time_point deadline,
                                                         Cancelled cancelled = {});

 private:
  static bool retryable(const Status &status);
  static Result<hf3fs::cache::UploadJobRecord> terminalResult(hf3fs::cache::UploadJobRecord job);

  std::shared_ptr<UploadJobWaiterBackend> backend_;
  UploadJobWaiterConfig config_;
};

}  // namespace hf3fs::client::cache
