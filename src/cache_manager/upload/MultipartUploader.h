#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "cache/origin/ObjectStore.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::client {
class ICommonMgmtdClient;
}
namespace hf3fs::meta::client {
class MetaClient;
}
namespace hf3fs::storage::client {
class StorageClient;
}

namespace hf3fs::cache_manager {

struct MultipartUploaderConfig {
  uint64_t partSize{8U << 20};
  uint32_t maxRetries{3};
  std::chrono::milliseconds initialBackoff{100};
  std::chrono::milliseconds maxBackoff{2000};

  Result<Void> valid() const;
};

class MultipartUploaderBackend {
 public:
  virtual ~MultipartUploaderBackend() = default;

  virtual CoTryTask<std::vector<uint8_t>> readStaging(uint64_t inode, cache::ByteRange range) = 0;
  virtual CoTryTask<cache::origin::MultipartUpload> createMultipartUpload(const cache::ObjectRef &destination) = 0;
  virtual CoTryTask<cache::origin::UploadPartResult> uploadPart(cache::origin::UploadPartRequest request) = 0;
  virtual CoTryTask<cache::UploadJobRecord> beginMultipartUpload(cache::UploadJobId jobId,
                                                                 uint64_t expectedStateVersion,
                                                                 std::string multipartId) = 0;
  virtual CoTryTask<cache::UploadJobRecord> checkpointUploadPart(cache::UploadJobId jobId,
                                                                 uint64_t expectedStateVersion,
                                                                 std::string multipartId,
                                                                 cache::CompletedUploadPart part) = 0;
  virtual CoTryTask<Void> backoff(std::chrono::milliseconds delay) = 0;
  virtual bool cancelled() const = 0;
};

class MultipartUploader {
 public:
  MultipartUploader(std::shared_ptr<MultipartUploaderBackend> backend, MultipartUploaderConfig config = {});

  CoTryTask<cache::UploadJobRecord> upload(cache::UploadJobRecord job);

  static Result<Void> validateProgress(const cache::UploadJobRecord &job, uint64_t partSize);
  static std::string checksum(std::span<const uint8_t> data);

 private:
  CoTryTask<cache::origin::UploadPartResult> uploadPartWithRetry(cache::origin::UploadPartRequest request);
  CoTryTask<cache::UploadJobRecord> beginWithRetry(const cache::UploadJobRecord &job, std::string multipartId);
  CoTryTask<cache::UploadJobRecord> checkpointWithRetry(const cache::UploadJobRecord &job,
                                                        cache::CompletedUploadPart part);
  CoTryTask<Void> waitBeforeRetry(uint32_t retry);
  Result<Void> checkCancelled() const;

  std::shared_ptr<MultipartUploaderBackend> backend_;
  MultipartUploaderConfig config_;
};

class RealMultipartUploaderBackend final : public MultipartUploaderBackend {
 public:
  RealMultipartUploaderBackend(std::shared_ptr<meta::client::MetaClient> metaClient,
                               std::shared_ptr<storage::client::StorageClient> storageClient,
                               std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                               std::shared_ptr<cache::origin::ObjectStore> objectStore,
                               std::string serviceName,
                               std::string serviceToken,
                               flat::UserInfo user);

  CoTryTask<std::vector<uint8_t>> readStaging(uint64_t inode, cache::ByteRange range) final;
  CoTryTask<cache::origin::MultipartUpload> createMultipartUpload(const cache::ObjectRef &destination) final;
  CoTryTask<cache::origin::UploadPartResult> uploadPart(cache::origin::UploadPartRequest request) final;
  CoTryTask<cache::UploadJobRecord> beginMultipartUpload(cache::UploadJobId jobId,
                                                         uint64_t expectedStateVersion,
                                                         std::string multipartId) final;
  CoTryTask<cache::UploadJobRecord> checkpointUploadPart(cache::UploadJobId jobId,
                                                         uint64_t expectedStateVersion,
                                                         std::string multipartId,
                                                         cache::CompletedUploadPart part) final;
  CoTryTask<Void> backoff(std::chrono::milliseconds delay) final;
  bool cancelled() const final;
  void cancel();

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::shared_ptr<storage::client::StorageClient> storageClient_;
  std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient_;
  std::shared_ptr<cache::origin::ObjectStore> objectStore_;
  std::string serviceName_;
  std::string serviceToken_;
  flat::UserInfo user_;
  std::atomic_bool cancelled_{false};
};

}  // namespace hf3fs::cache_manager
