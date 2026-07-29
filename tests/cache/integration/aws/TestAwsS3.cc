#include <algorithm>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#if __has_include(<aws/core/client/AWSAuthSigner.h>)
#include <aws/core/client/AWSAuthSigner.h>
#else
#include <aws/core/auth/AWSAuthSigner.h>
#endif
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <cstdlib>
#include <folly/experimental/coro/BlockingWait.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

#include "cache/origin/s3/S3ObjectStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::integration::aws {
namespace {

std::string environment(const char *name) {
  auto *value = std::getenv(name);
  return value == nullptr ? std::string{} : std::string{value};
}

bool environmentFlag(const char *name, bool defaultValue = false) {
  auto value = environment(name);
  if (value.empty()) return defaultValue;
  return value == "1" || value == "true" || value == "TRUE" || value == "on";
}

struct CreatedObject {
  std::string bucket;
  std::string key;
  std::optional<std::string> versionId;
};

class AwsS3Qualification : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    enabled_ = environmentFlag("HF3FS_CACHE_AWS_QUALIFICATION");
    if (!enabled_) return;

    region_ = environment("HF3FS_CACHE_AWS_REGION");
    versionedBucket_ = environment("HF3FS_CACHE_AWS_VERSIONED_BUCKET");
    etagBucket_ = environment("HF3FS_CACHE_AWS_ETAG_BUCKET");
    logPath_ = environment("HF3FS_CACHE_AWS_QUALIFICATION_LOG");
    auto accessKey = environment("HF3FS_CACHE_AWS_ACCESS_KEY");
    auto secretKey = environment("HF3FS_CACHE_AWS_SECRET_KEY");
    auto sessionToken = environment("HF3FS_CACHE_AWS_SESSION_TOKEN");
    if (region_.empty() || versionedBucket_.empty() || etagBucket_.empty() || logPath_.empty()) {
      configurationError_ = "region, both qualification buckets, and a qualification log path are required";
      return;
    }
    if (accessKey.empty() != secretKey.empty() || (!sessionToken.empty() && accessKey.empty())) {
      configurationError_ = "invalid externally supplied AWS credential fields";
      return;
    }

    origin::s3::AwsS3ClientConfig clientConfig;
    clientConfig.region = region_;
    clientConfig.endpoint = environment("HF3FS_CACHE_AWS_ENDPOINT");
    clientConfig.useTls = environmentFlag("HF3FS_CACHE_AWS_USE_TLS", true);
    clientConfig.pathStyle = environmentFlag("HF3FS_CACHE_AWS_PATH_STYLE");
    clientConfig.accessKey = accessKey;
    clientConfig.secretKey = secretKey;
    clientConfig.sessionToken = sessionToken;
    origin::s3::S3ObjectStoreConfig storeConfig;
    storeConfig.ioThreads = 2;
    storeConfig.maxRetries = 2;
    storeConfig.retryDelay = 100_ms;
    auto store = origin::s3::S3ObjectStore::createAws(storeConfig, clientConfig);
    if (store.hasError()) {
      configurationError_ = store.error().describe();
      return;
    }
    store_ = *store;

    Aws::Client::ClientConfiguration awsConfig;
    awsConfig.region = region_.c_str();
    if (!clientConfig.endpoint.empty()) awsConfig.endpointOverride = clientConfig.endpoint.c_str();
    awsConfig.scheme = clientConfig.useTls ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials;
    if (accessKey.empty()) {
      credentials = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    } else {
      credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(accessKey.c_str(),
                                                                              secretKey.c_str(),
                                                                              sessionToken.c_str());
    }
    client_ = std::make_unique<Aws::S3::S3Client>(credentials,
                                                  awsConfig,
                                                  Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                  !clientConfig.pathStyle);
    prefix_ = "hf3fs-cache-qualification/" + std::to_string(static_cast<uint64_t>(getpid())) + "/";
    log_ = std::make_unique<std::ofstream>(logPath_, std::ios::trunc);
    if (!*log_) configurationError_ = "cannot open AWS qualification log path";
  }

  static void TearDownTestSuite() {
    if (!enabled_) return;
    size_t cleanupFailures = 0;
    if (client_ != nullptr) {
      for (const auto &object : created_) {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(object.bucket.c_str());
        request.SetKey(object.key.c_str());
        if (object.versionId) request.SetVersionId(object.versionId->c_str());
        if (!client_->DeleteObject(request).IsSuccess()) ++cleanupFailures;
      }
    }
    if (log_ != nullptr && *log_) {
      *log_ << "cleanup_objects=" << created_.size() << '\n';
      *log_ << "cleanup_failures=" << cleanupFailures << '\n';
      *log_ << "qualification_complete=" << (cleanupFailures == 0 && passedScenarios_ == 2 ? "true" : "false") << '\n';
      log_->flush();
    }
    client_.reset();
    store_.reset();
    log_.reset();
  }

  static Result<std::string> put(const std::string &bucket, const std::string &key, const std::vector<uint8_t> &data) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket.c_str());
    request.SetKey(key.c_str());
    auto body = Aws::MakeShared<Aws::StringStream>("hf3fs-cache-aws-qualification");
    if (!data.empty()) {
      body->write(reinterpret_cast<const char *>(data.data()), static_cast<std::streamsize>(data.size()));
    }
    request.SetBody(body);
    auto result = client_->PutObject(request);
    if (!result.IsSuccess()) return makeError(CacheCode::kUnavailable, "AWS qualification PUT failed");
    return std::string{result.GetResult().GetVersionId().c_str()};
  }

  static void record(std::string_view scenario, std::string_view result) {
    if (result == "passed") ++passedScenarios_;
    if (log_ != nullptr && *log_) *log_ << scenario << '=' << result << '\n';
  }

  static bool enabled_;
  static std::string configurationError_;
  static std::string region_;
  static std::string versionedBucket_;
  static std::string etagBucket_;
  static std::string logPath_;
  static std::string prefix_;
  static std::shared_ptr<origin::s3::S3ObjectStore> store_;
  static std::unique_ptr<Aws::S3::S3Client> client_;
  static std::unique_ptr<std::ofstream> log_;
  static std::vector<CreatedObject> created_;
  static size_t passedScenarios_;
};

bool AwsS3Qualification::enabled_{false};
std::string AwsS3Qualification::configurationError_;
std::string AwsS3Qualification::region_;
std::string AwsS3Qualification::versionedBucket_;
std::string AwsS3Qualification::etagBucket_;
std::string AwsS3Qualification::logPath_;
std::string AwsS3Qualification::prefix_;
std::shared_ptr<origin::s3::S3ObjectStore> AwsS3Qualification::store_;
std::unique_ptr<Aws::S3::S3Client> AwsS3Qualification::client_;
std::unique_ptr<std::ofstream> AwsS3Qualification::log_;
std::vector<CreatedObject> AwsS3Qualification::created_;
size_t AwsS3Qualification::passedScenarios_{0};

TEST_F(AwsS3Qualification, VersionedBucketPreservesPinnedVersion) {
  if (!enabled_) GTEST_SKIP() << "AWS S3 qualification is not explicitly enabled";
  ASSERT_TRUE(configurationError_.empty()) << configurationError_;
  auto key = prefix_ + "versioned";
  auto initial = std::vector<uint8_t>{1, 2, 3, 4};
  auto firstVersion = put(versionedBucket_, key, initial);
  ASSERT_OK(firstVersion);
  ASSERT_FALSE(firstVersion->empty()) << "qualification bucket must have versioning enabled";
  created_.push_back({versionedBucket_, key, *firstVersion});

  auto metadata = folly::coro::blockingWait(store_->head({OriginId{1}, versionedBucket_, key}));
  ASSERT_OK(metadata);
  ASSERT_EQ(metadata->identity.version.type, VersionSelectorType::VERSION_ID);
  ASSERT_EQ(metadata->identity.version.value, *firstVersion);

  auto current = std::vector<uint8_t>{9, 8, 7, 6};
  auto secondVersion = put(versionedBucket_, key, current);
  ASSERT_OK(secondVersion);
  ASSERT_FALSE(secondVersion->empty());
  created_.push_back({versionedBucket_, key, *secondVersion});

  auto pinned = folly::coro::blockingWait(store_->getRange(metadata->identity, {0, initial.size()}));
  ASSERT_OK(pinned);
  ASSERT_EQ(*pinned, initial);
  record("version_id", "passed");
}

TEST_F(AwsS3Qualification, UnversionedBucketRejectsStaleEtag) {
  if (!enabled_) GTEST_SKIP() << "AWS S3 qualification is not explicitly enabled";
  ASSERT_TRUE(configurationError_.empty()) << configurationError_;
  auto key = prefix_ + "etag";
  auto initial = std::vector<uint8_t>{4, 3, 2, 1};
  auto version = put(etagBucket_, key, initial);
  ASSERT_OK(version);
  ASSERT_TRUE(version->empty()) << "If-Match qualification bucket must have versioning disabled";
  created_.push_back({etagBucket_, key, std::nullopt});

  auto metadata = folly::coro::blockingWait(store_->head({OriginId{2}, etagBucket_, key}));
  ASSERT_OK(metadata);
  ASSERT_EQ(metadata->identity.version.type, VersionSelectorType::STRONG_ETAG);
  auto original = folly::coro::blockingWait(store_->getRange(metadata->identity, {0, initial.size()}));
  ASSERT_OK(original);
  ASSERT_EQ(*original, initial);

  ASSERT_OK(put(etagBucket_, key, {8, 8, 8, 8}));
  auto stale = folly::coro::blockingWait(store_->getRange(metadata->identity, {0, initial.size()}));
  ASSERT_ERROR(stale, CacheCode::kVersionMismatch);
  record("if_match", "passed");
}

}  // namespace
}  // namespace hf3fs::cache::integration::aws
