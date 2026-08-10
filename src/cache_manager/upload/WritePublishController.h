#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "cache_manager/upload/MultipartUploadFinalizer.h"
#include "cache_manager/upload/MultipartUploader.h"
#include "common/utils/Coroutine.h"
#include "fbs/meta/Service.h"

namespace hf3fs::cache_manager {

struct UploadJobPage {
  std::vector<cache::UploadJobRecord> jobs;
  bool more{false};
};

struct WritePublishControllerConfig {
  uint32_t pageSize{100};
  uint32_t globalConcurrency{4};
  uint32_t perOwnerConcurrency{2};
  uint32_t perOriginConcurrency{2};
  flat::ChainTableId cacheTableId{};
  uint32_t cacheBlockSize{4U << 20};
  uint32_t cacheStripeSize{1};
  uint32_t publishedPrefetchPriority{static_cast<uint32_t>(std::numeric_limits<int32_t>::max())};
  MultipartUploaderConfig uploader;
  MultipartUploadFinalizerConfig finalizer;

  Result<Void> valid() const;
};

struct WritePublishRunResult {
  uint32_t scanned{0};
  uint32_t scheduled{0};
  uint32_t completed{0};
  uint32_t failed{0};
  bool stopped{false};
};

class WritePublishControllerBackend {
 public:
  virtual ~WritePublishControllerBackend() = default;
  virtual CoTryTask<UploadJobPage> list(std::optional<cache::UploadJobId> after,
                                        uint32_t limit,
                                        bool includeTerminal) = 0;
  virtual CoTryTask<UploadJobPage> listExpiredOpen(std::optional<meta::UploadOpenLeaseCursor> after,
                                                   uint64_t expiresBeforeMs,
                                                   uint32_t limit) = 0;
  virtual CoTryTask<cache::UploadJobRecord> recoverOpen(cache::UploadJobRecord job) = 0;
  virtual CoTryTask<cache::UploadJobRecord> finalizeCancelled(cache::UploadJobRecord job) = 0;
  virtual CoTryTask<cache::UploadJobRecord> upload(cache::UploadJobRecord job) = 0;
  virtual CoTryTask<cache::UploadJobRecord> complete(cache::UploadJobRecord job) = 0;
  virtual CoTryTask<cache::UploadJobRecord> publish(cache::UploadJobRecord job) = 0;
  virtual CoTryTask<cache::UploadJobRecord> warm(cache::UploadJobRecord job) = 0;
  virtual CoTryTask<cache::UploadJobRecord> abort(cache::UploadJobRecord job) = 0;
  virtual void stop() = 0;
};

class RealWritePublishControllerBackend final : public WritePublishControllerBackend {
 public:
  RealWritePublishControllerBackend(std::shared_ptr<meta::client::MetaClient> metaClient,
                                    std::shared_ptr<storage::client::StorageClient> storageClient,
                                    std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                                    std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                    meta::CacheServiceIdentity service,
                                    WritePublishControllerConfig config);

  CoTryTask<UploadJobPage> list(std::optional<cache::UploadJobId> after, uint32_t limit, bool includeTerminal) final;
  CoTryTask<UploadJobPage> listExpiredOpen(std::optional<meta::UploadOpenLeaseCursor> after,
                                           uint64_t expiresBeforeMs,
                                           uint32_t limit) final;
  CoTryTask<cache::UploadJobRecord> recoverOpen(cache::UploadJobRecord job) final;
  CoTryTask<cache::UploadJobRecord> finalizeCancelled(cache::UploadJobRecord job) final;
  CoTryTask<cache::UploadJobRecord> upload(cache::UploadJobRecord job) final;
  CoTryTask<cache::UploadJobRecord> complete(cache::UploadJobRecord job) final;
  CoTryTask<cache::UploadJobRecord> publish(cache::UploadJobRecord job) final;
  CoTryTask<cache::UploadJobRecord> warm(cache::UploadJobRecord job) final;
  CoTryTask<cache::UploadJobRecord> abort(cache::UploadJobRecord job) final;
  void stop() final;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::shared_ptr<storage::client::StorageClient> storageClient_;
  std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient_;
  std::shared_ptr<cache::origin::ObjectStore> objectStore_;
  meta::CacheServiceIdentity service_;
  WritePublishControllerConfig config_;
  std::atomic<bool> stopping_{false};
  std::mutex activeMutex_;
  std::map<Uuid, std::shared_ptr<RealMultipartUploaderBackend>> uploaders_;
  std::map<Uuid, std::shared_ptr<RealMultipartUploadFinalizerBackend>> finalizers_;
};

class WritePublishController {
 public:
  WritePublishController(std::shared_ptr<WritePublishControllerBackend> backend, WritePublishControllerConfig config);
  ~WritePublishController();

  CoTryTask<WritePublishRunResult> recover();
  CoTryTask<WritePublishRunResult> runOnce();
  void stop();

 private:
  static bool actionable(const cache::UploadJobRecord &job);
  static bool terminal(cache::UploadJobState state);
  CoTryTask<std::vector<cache::UploadJobRecord>> scan(bool includeTerminal);
  CoTryTask<std::vector<cache::UploadJobRecord>> scanExpiredOpen();
  CoTryTask<void> migrateActiveIndex();
  CoTryTask<WritePublishRunResult> run(bool includeTerminal);
  std::vector<cache::UploadJobRecord> select(std::vector<cache::UploadJobRecord> jobs);
  CoTryTask<cache::UploadJobRecord> advance(cache::UploadJobRecord job);

  std::shared_ptr<WritePublishControllerBackend> backend_;
  WritePublishControllerConfig config_;
  std::atomic<bool> stopping_{false};
  size_t roundRobinOffset_{0};
  std::optional<cache::UploadJobId> activeScanAfter_;
};

}  // namespace hf3fs::cache_manager
