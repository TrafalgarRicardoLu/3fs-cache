#pragma once

#include <condition_variable>
#include <cstdint>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <memory>
#include <mutex>
#include <string>

#include "cache/origin/ObjectStore.h"
#include "cache/origin/s3/S3RequestExecutor.h"
#include "common/utils/Duration.h"

namespace hf3fs::cache::origin::s3 {

struct S3ObjectStoreConfig {
  uint32_t ioThreads{4};
  uint32_t maxConcurrentRequests{32};
  uint64_t maxInflightBytes{256ULL << 20};
  uint32_t maxRetries{3};
  Duration retryDelay{100_ms};
  Duration totalTimeout{30_s};
};

struct AwsS3ClientConfig {
  std::string region;
  std::string endpoint;
  bool useTls{true};
  bool pathStyle{false};
  uint32_t maxConnections{32};
  Duration connectTimeout{5_s};
  Duration requestTimeout{30_s};
  std::string accessKey;
  std::string secretKey;
  std::string sessionToken;
};

class S3ObjectStore : public ObjectStore {
 public:
  S3ObjectStore(S3ObjectStoreConfig config, std::unique_ptr<S3RequestExecutor> executor);

  static Result<std::shared_ptr<S3ObjectStore>> createAws(S3ObjectStoreConfig config,
                                                          const AwsS3ClientConfig &clientConfig);

  CoTryTask<ObjectMetadata> head(const ObjectRef &object) override;
  CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) override;
  CoTryTask<ListObjectsPage> listObjects(const ListObjectsRequest &request) override;

 private:
  class Permit;

  Result<ObjectMetadata> headSync(const ObjectRef &object);
  Result<std::vector<uint8_t>> getRangeSync(const ImmutableObjectIdentity &object, ByteRange range);
  Result<ListObjectsPage> listObjectsSync(const ListObjectsRequest &request);
  Result<Permit> acquire(uint64_t bytes);
  void release(uint64_t bytes);

  S3ObjectStoreConfig config_;
  std::unique_ptr<S3RequestExecutor> executor_;
  folly::IOThreadPoolExecutor ioExecutor_;
  std::mutex mutex_;
  std::condition_variable condition_;
  uint32_t activeRequests_{0};
  uint64_t inflightBytes_{0};
};

}  // namespace hf3fs::cache::origin::s3
