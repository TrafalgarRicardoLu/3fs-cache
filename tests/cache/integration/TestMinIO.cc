#include <algorithm>
#include <atomic>
#include <aws/core/auth/AWSCredentialsProvider.h>
#if __has_include(<aws/core/client/AWSAuthSigner.h>)
#include <aws/core/client/AWSAuthSigner.h>
#else
#include <aws/core/auth/AWSAuthSigner.h>
#endif
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <cstdlib>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "cache/origin/s3/S3ObjectStore.h"
#include "cache_manager/access/AccessAggregator.h"
#include "cache_manager/admission/CapacityGate.h"
#include "cache_manager/admission/SecondMissAdmissionPolicy.h"
#include "cache_manager/eviction/LRUEvictionPolicy.h"
#include "client/cache/CacheReadPipeline.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::integration {
namespace {

constexpr uint32_t kBlockSize = 4;

std::string environment(const char *name) {
  auto *value = std::getenv(name);
  return value == nullptr ? std::string{} : std::string{value};
}

bool environmentFlag(const char *name) {
  auto value = environment(name);
  return value == "1" || value == "true" || value == "TRUE" || value == "on";
}

std::vector<uint8_t> sequence(size_t size, uint8_t base = 0) {
  std::vector<uint8_t> result(size);
  for (size_t i = 0; i < size; ++i) result[i] = static_cast<uint8_t>(base + i);
  return result;
}

class CountingObjectStore final : public origin::ObjectStore {
 public:
  explicit CountingObjectStore(std::shared_ptr<origin::ObjectStore> delegate)
      : delegate_(std::move(delegate)) {}

  CoTryTask<origin::ObjectMetadata> head(const ObjectRef &object) final {
    ++headRequests;
    co_return co_await delegate_->head(object);
  }

  CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) final {
    ++rangeRequests;
    co_return co_await delegate_->getRange(object, range);
  }

  void resetRangeRequests() { rangeRequests.store(0); }

  std::atomic<size_t> headRequests{0};
  std::atomic<size_t> rangeRequests{0};

 private:
  std::shared_ptr<origin::ObjectStore> delegate_;
};

using CachedBlocks = std::map<CacheBlockIndex, std::vector<uint8_t>>;

class HarnessPlanSource final : public client::cache::IReadPlanSource {
 public:
  HarnessPlanSource(meta::Inode inode, const CachedBlocks &blocks)
      : inode_(std::move(inode)),
        blocks_(blocks) {}

  CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) final {
    meta::GetFileReadPlanRsp response;
    response.inode = inode_.id;
    response.object = inode_.asOriginFile().object;
    auto requestEnd = std::min(inode_.fileLength(), request.offset + request.length);
    for (auto index = request.offset / kBlockSize; index * kBlockSize < requestEnd; ++index) {
      auto begin = index * kBlockSize;
      auto length = std::min<uint64_t>(kBlockSize, inode_.fileLength() - begin);
      auto blockIndex = CacheBlockIndex{static_cast<uint32_t>(index)};
      meta::ReadBlockPlan block;
      block.key = {inode_.id.u64(), blockIndex};
      block.fileRange = {begin, length};
      block.originRange = block.fileRange;
      block.chunkId = meta::ChunkId(inode_.id, 0, static_cast<uint32_t>(index));
      block.chainId = flat::ChainId{1};
      block.actualBlockLength = length;
      auto cached = blocks_.find(blockIndex);
      if (cached == blocks_.end()) {
        block.state = CacheBlockState::LOADING;
      } else {
        block.state = CacheBlockState::READY;
        block.loadEpoch = 1;
        auto checksum =
            storage::ChecksumInfo::create(storage::ChecksumType::CRC32C, cached->second.data(), cached->second.size());
        block.ready = ReadyIdentity{block.loadEpoch,
                                    CacheGeneration{1},
                                    static_cast<uint8_t>(checksum.type),
                                    checksum.value,
                                    length};
      }
      response.blocks.push_back(std::move(block));
    }
    co_return response;
  }

 private:
  meta::Inode inode_;
  const CachedBlocks &blocks_;
};

class HarnessHitReader final : public client::cache::ICacheHitReader {
 public:
  explicit HarnessHitReader(const CachedBlocks &blocks)
      : blocks_(blocks) {}

  CoTryTask<std::vector<uint8_t>> readFullBlock(const meta::ReadBlockPlan &plan, const flat::UserInfo &) final {
    auto block = blocks_.find(plan.key.block);
    if (block == blocks_.end()) co_return makeError(StorageClientCode::kChunkNotFound);
    co_return block->second;
  }

 private:
  const CachedBlocks &blocks_;
};

meta::Inode originInode(meta::InodeId inode, uint64_t length, ImmutableObjectIdentity identity) {
  return meta::Inode{
      inode,
      meta::InodeData{
          meta::OriginFile{length, meta::Layout::newEmpty(flat::ChainTableId{1}, kBlockSize, 1), std::move(identity)}}};
}

class MinIOIntegration : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    endpoint_ = environment("HF3FS_CACHE_MINIO_ENDPOINT");
    accessKey_ = environment("HF3FS_CACHE_MINIO_ACCESS_KEY");
    secretKey_ = environment("HF3FS_CACHE_MINIO_SECRET_KEY");
    if (endpoint_.empty() || accessKey_.empty() || secretKey_.empty()) {
      configurationError_ =
          "HF3FS_CACHE_MINIO_ENDPOINT, HF3FS_CACHE_MINIO_ACCESS_KEY and HF3FS_CACHE_MINIO_SECRET_KEY are required";
      return;
    }

    origin::s3::AwsS3ClientConfig clientConfig;
    clientConfig.region = environment("HF3FS_CACHE_MINIO_REGION");
    if (clientConfig.region.empty()) clientConfig.region = "us-east-1";
    clientConfig.endpoint = endpoint_;
    clientConfig.useTls = environmentFlag("HF3FS_CACHE_MINIO_USE_TLS");
    clientConfig.pathStyle = true;
    clientConfig.accessKey = accessKey_;
    clientConfig.secretKey = secretKey_;
    origin::s3::S3ObjectStoreConfig storeConfig;
    storeConfig.ioThreads = 2;
    storeConfig.maxRetries = 2;
    storeConfig.retryDelay = 10_ms;
    auto created = origin::s3::S3ObjectStore::createAws(storeConfig, clientConfig);
    if (created.hasError()) {
      configurationError_ = created.error().describe();
      return;
    }
    store_ = *created;
    countingStore_ = std::make_unique<CountingObjectStore>(store_);

    Aws::Client::ClientConfiguration awsConfig;
    awsConfig.region = clientConfig.region.c_str();
    awsConfig.endpointOverride = endpoint_.c_str();
    awsConfig.scheme = clientConfig.useTls ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    auto credentials =
        std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(accessKey_.c_str(), secretKey_.c_str());
    client_ = std::make_unique<Aws::S3::S3Client>(credentials,
                                                  awsConfig,
                                                  Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                  false);
    bucket_ = "hf3fs-cache-it-" + std::to_string(static_cast<uint64_t>(getpid()));
    Aws::S3::Model::CreateBucketRequest create;
    create.SetBucket(bucket_.c_str());
    auto createdBucket = client_->CreateBucket(create);
    if (!createdBucket.IsSuccess()) {
      configurationError_ =
          std::string{"failed to create isolated MinIO bucket: "} + createdBucket.GetError().GetMessage().c_str();
      return;
    }

    objects_ = {{"empty", {}},
                {"one", sequence(1)},
                {"block-minus-one", sequence(kBlockSize - 1)},
                {"block", sequence(kBlockSize)},
                {"block-plus-one", sequence(kBlockSize + 1)},
                {"multi", sequence(kBlockSize * 3 + 1)}};
    for (const auto &[key, data] : objects_) {
      if (!put(key, data)) return;
    }
  }

  static void TearDownTestSuite() {
    if (client_ != nullptr && !bucket_.empty()) {
      for (const auto &[key, data] : objects_) {
        static_cast<void>(data);
        Aws::S3::Model::DeleteObjectRequest remove;
        remove.SetBucket(bucket_.c_str());
        remove.SetKey(key.c_str());
        client_->DeleteObject(remove);
      }
      Aws::S3::Model::DeleteBucketRequest removeBucket;
      removeBucket.SetBucket(bucket_.c_str());
      client_->DeleteBucket(removeBucket);
    }
    client_.reset();
    countingStore_.reset();
    store_.reset();
  }

  static bool put(const std::string &key, const std::vector<uint8_t> &data) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());
    auto body = Aws::MakeShared<Aws::StringStream>("hf3fs-cache-minio-integration");
    if (!data.empty()) {
      body->write(reinterpret_cast<const char *>(data.data()), static_cast<std::streamsize>(data.size()));
    }
    request.SetBody(body);
    auto result = client_->PutObject(request);
    if (!result.IsSuccess()) {
      configurationError_ =
          std::string{"failed to upload MinIO fixture object: "} + result.GetError().GetMessage().c_str();
      return false;
    }
    return true;
  }

  static std::string endpoint_;
  static std::string accessKey_;
  static std::string secretKey_;
  static std::string bucket_;
  static std::string configurationError_;
  static std::map<std::string, std::vector<uint8_t>> objects_;
  static std::shared_ptr<origin::s3::S3ObjectStore> store_;
  static std::unique_ptr<CountingObjectStore> countingStore_;
  static std::unique_ptr<Aws::S3::S3Client> client_;
};

std::string MinIOIntegration::endpoint_;
std::string MinIOIntegration::accessKey_;
std::string MinIOIntegration::secretKey_;
std::string MinIOIntegration::bucket_;
std::string MinIOIntegration::configurationError_;
std::map<std::string, std::vector<uint8_t>> MinIOIntegration::objects_;
std::shared_ptr<origin::s3::S3ObjectStore> MinIOIntegration::store_;
std::unique_ptr<CountingObjectStore> MinIOIntegration::countingStore_;
std::unique_ptr<Aws::S3::S3Client> MinIOIntegration::client_;

TEST_F(MinIOIntegration, ReadsFixedObjectBoundaryMatrix) {
  ASSERT_TRUE(configurationError_.empty()) << configurationError_;
  for (const auto &[key, expected] : objects_) {
    auto metadata = folly::coro::blockingWait(countingStore_->head({OriginId{1}, bucket_, key}));
    ASSERT_OK(metadata);
    ASSERT_EQ(metadata->size, expected.size());
    if (expected.empty()) continue;
    auto actual = folly::coro::blockingWait(countingStore_->getRange(metadata->identity, {0, expected.size()}));
    ASSERT_OK(actual);
    ASSERT_EQ(*actual, expected);
  }
}

TEST_F(MinIOIntegration, ColdFillWarmMixedRefreshCapacityAndCleanup) {
  ASSERT_TRUE(configurationError_.empty()) << configurationError_;
  const auto &initial = objects_.at("multi");
  auto metadata = folly::coro::blockingWait(countingStore_->head({OriginId{1}, bucket_, "multi"}));
  ASSERT_OK(metadata);
  auto inode = originInode(meta::InodeId{101}, metadata->size, metadata->identity);
  CachedBlocks blocks;
  auto source = std::make_shared<HarnessPlanSource>(inode, blocks);
  client::cache::ReadPlanner planner(source);
  HarnessHitReader hitReader(blocks);
  client::cache::LocalMissSingleflight singleflight;
  client::cache::OriginMissReader missReader(*countingStore_, singleflight, {.maxRangeBytes = kBlockSize});
  client::cache::CacheReadPipeline pipeline(missReader, planner, hitReader);
  auto session = meta::SessionInfo{ClientId::random(), Uuid::random()};

  countingStore_->resetRangeRequests();
  std::vector<uint8_t> output(initial.size());
  auto cold = folly::coro::blockingWait(pipeline.read(flat::UserInfo{}, inode, session, 0, output));
  ASSERT_OK(cold);
  ASSERT_EQ(output, initial);
  ASSERT_GT(countingStore_->rangeRequests.load(), size_t{0});

  cache_manager::SecondMissAdmissionPolicy admission(1'000'000, 1024);
  cache::CacheBlockKey firstKey{inode.id.u64(), CacheBlockIndex{0}};
  EXPECT_EQ(admission.evaluate({firstKey, 100}).action, cache_manager::AdmissionAction::BYPASS);
  EXPECT_EQ(admission.evaluate({firstKey, 101}).action, cache_manager::AdmissionAction::ADMIT);

  for (uint32_t index = 0; uint64_t{index} * kBlockSize < initial.size(); ++index) {
    auto offset = uint64_t{index} * kBlockSize;
    auto length = std::min<uint64_t>(kBlockSize, initial.size() - offset);
    auto loaded = folly::coro::blockingWait(countingStore_->getRange(metadata->identity, {offset, length}));
    ASSERT_OK(loaded);
    blocks.emplace(CacheBlockIndex{index}, std::move(*loaded));
  }

  countingStore_->resetRangeRequests();
  std::fill(output.begin(), output.end(), 0);
  auto warm = folly::coro::blockingWait(pipeline.read(flat::UserInfo{}, inode, session, 0, output));
  ASSERT_OK(warm);
  ASSERT_EQ(output, initial);
  ASSERT_EQ(countingStore_->rangeRequests.load(), size_t{0});

  cache_manager::AccessAggregator access(16);
  ASSERT_TRUE(access.record({firstKey, CacheGeneration{1}, 1000}, 1100));
  ASSERT_EQ(access.take(16).size(), size_t{1});

  blocks.erase(CacheBlockIndex{1});
  countingStore_->resetRangeRequests();
  auto mixed = folly::coro::blockingWait(pipeline.read(flat::UserInfo{}, inode, session, 0, output));
  ASSERT_OK(mixed);
  ASSERT_EQ(output, initial);
  ASSERT_EQ(countingStore_->rangeRequests.load(), size_t{1});

  cache_manager::CapacityGate capacity({1, kBlockSize}, {{OriginId{1}, {1, kBlockSize}}});
  ASSERT_ERROR(capacity.tryAcquire(OriginId{1}, kBlockSize + 1), CacheCode::kCapacityExceeded);

  auto mismatched = metadata->identity;
  mismatched.version = {VersionSelectorType::STRONG_ETAG, metadata->identity.version.value + "-stale"};
  auto mismatch = folly::coro::blockingWait(countingStore_->getRange(mismatched, {0, 1}));
  ASSERT_ERROR(mismatch, CacheCode::kVersionMismatch);

  auto refreshedData = sequence(initial.size(), 31);
  ASSERT_TRUE(put("multi", refreshedData)) << configurationError_;
  auto refreshed = folly::coro::blockingWait(countingStore_->head({OriginId{1}, bucket_, "multi"}));
  ASSERT_OK(refreshed);
  ASSERT_NE(refreshed->identity, metadata->identity);
  auto refreshedInode = originInode(meta::InodeId{102}, refreshed->size, refreshed->identity);
  CachedBlocks refreshedBlocks;
  auto refreshedSource = std::make_shared<HarnessPlanSource>(refreshedInode, refreshedBlocks);
  client::cache::ReadPlanner refreshedPlanner(refreshedSource);
  HarnessHitReader refreshedHitReader(refreshedBlocks);
  client::cache::CacheReadPipeline refreshedPipeline(missReader, refreshedPlanner, refreshedHitReader);
  std::vector<uint8_t> refreshedOutput(refreshedData.size());
  ASSERT_OK(
      folly::coro::blockingWait(refreshedPipeline.read(flat::UserInfo{}, refreshedInode, session, 0, refreshedOutput)));
  ASSERT_EQ(refreshedOutput, refreshedData);

  for (uint32_t index = 0; uint64_t{index} * kBlockSize < refreshedData.size(); ++index) {
    auto offset = uint64_t{index} * kBlockSize;
    auto length = std::min<uint64_t>(kBlockSize, refreshedData.size() - offset);
    auto loaded = folly::coro::blockingWait(countingStore_->getRange(refreshed->identity, {offset, length}));
    ASSERT_OK(loaded);
    refreshedBlocks.emplace(CacheBlockIndex{index}, std::move(*loaded));
  }
  countingStore_->resetRangeRequests();
  ASSERT_OK(
      folly::coro::blockingWait(refreshedPipeline.read(flat::UserInfo{}, refreshedInode, session, 0, refreshedOutput)));
  ASSERT_EQ(countingStore_->rangeRequests.load(), size_t{0});

  storage::PhysicalDiskId disk{Uuid::random()};
  cache_manager::EvictionCandidate candidate{firstKey,
                                             ReadyIdentity{1, CacheGeneration{1}, 1, 1, kBlockSize},
                                             flat::ChainId{1},
                                             kBlockSize,
                                             UtcTime::fromMicroseconds(1),
                                             UtcTime::fromMicroseconds(2),
                                             {{disk, kBlockSize}}};
  cache_manager::LRUEvictionPolicy eviction;
  auto selected = eviction.select(std::span<const cache_manager::EvictionCandidate>{&candidate, 1},
                                  {{{disk, kBlockSize}}, UtcTime::fromMicroseconds(3), 0_ns});
  ASSERT_OK(selected);
  ASSERT_EQ(*selected, std::vector<size_t>{0});
  refreshedBlocks.clear();
  countingStore_->resetRangeRequests();
  ASSERT_OK(
      folly::coro::blockingWait(refreshedPipeline.read(flat::UserInfo{}, refreshedInode, session, 0, refreshedOutput)));
  ASSERT_GT(countingStore_->rangeRequests.load(), size_t{0});
  objects_.at("multi") = std::move(refreshedData);
}

}  // namespace
}  // namespace hf3fs::cache::integration
