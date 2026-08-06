#include <gtest/gtest.h>

#include "common/serde/Serde.h"
#include "fbs/cache_manager/Service.h"
#include "fbs/meta/Service.h"
#include "fbs/storage/Service.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::test {
namespace {

static_assert(meta::MetaSerde<>::importOriginFileMethodId == 22);
static_assert(meta::MetaSerde<>::listCacheBlocksMethodId == 33);
static_assert(storage::StorageSerde<>::replaceCacheChunksMethodId == 18);
static_assert(storage::StorageSerde<>::queryCacheChunkGenerationsMethodId == 20);
static_assert(cache_manager::CacheManagerSerde<>::ensureCachedMethodId == 1);
static_assert(cache_manager::CacheManagerSerde<>::getCacheStatusMethodId == 4);
static_assert(meta::MetaSerde<>::updateCacheBlockAccessMethodId == 34);
static_assert(meta::MetaSerde<>::listCacheEventDeadLettersMethodId == 38);
static_assert(storage::StorageSerde<>::queryCacheSpaceMethodId == 21);
static_assert(storage::StorageSerde<>::coordinateCacheRetiresMethodId == 27);
static_assert(cache_manager::CacheManagerSerde<>::reportCacheAccessMethodId == 5);
static_assert(cache_manager::CacheManagerSerde<>::getPhase2CacheStatusMethodId == 6);
static_assert(cache_manager::CacheManagerSerde<>::createPrefetchJobMethodId == 7);
static_assert(cache_manager::CacheManagerSerde<>::getPinStatusMethodId == 13);
static_assert(meta::MetaSerde<>::createPrefetchJobMethodId == 42);
static_assert(meta::MetaSerde<>::removeCachePinsMethodId == 49);
static_assert(meta::MetaSerde<>::listCachePinsByOwnerMethodId == 51);
static_assert(meta::MetaSerde<>::queryCachePinsMethodId == 52);
static_assert(meta::MetaSerde<>::updatePrefetchPlanEntriesMethodId == 53);
static_assert(meta::MetaSerde<>::trackPrefetchReadyMethodId == 54);
static_assert(meta::MetaSerde<>::advancePrefetchJobStateMethodId == 55);
static_assert(meta::MetaSerde<>::cancelPrefetchJobMethodId == 56);

struct LegacyPhase2DiskStatus {
  SERDE_STRUCT_FIELD(physicalDiskId, storage::PhysicalDiskId{});
  SERDE_STRUCT_FIELD(role, storage::StorageRole::INVALID);
  SERDE_STRUCT_FIELD(capacityBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(physicalUsedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(allocatableBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(reservedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(snapshotAgeNs, uint64_t{0});
  SERDE_STRUCT_FIELD(admissionPaused, false);
  SERDE_STRUCT_FIELD(pauseReason, String{});
  SERDE_STRUCT_FIELD(eventPrepared, uint64_t{0});
  SERDE_STRUCT_FIELD(eventDeliverable, uint64_t{0});
  SERDE_STRUCT_FIELD(eventAcknowledgedSequence, uint64_t{0});
};

struct LegacyPhase2CacheStatus {
  SERDE_STRUCT_FIELD(enabled, false);
  SERDE_STRUCT_FIELD(managerEpoch, Uuid::zero());
  SERDE_STRUCT_FIELD(admissionPolicy, String{});
  SERDE_STRUCT_FIELD(evictionPolicy, String{});
  SERDE_STRUCT_FIELD(disks, std::vector<LegacyPhase2DiskStatus>{});
  SERDE_STRUCT_FIELD(evicting, uint64_t{0});
  SERDE_STRUCT_FIELD(eventBacklog, uint64_t{0});
  SERDE_STRUCT_FIELD(deadLetters, uint64_t{0});
};

struct LegacyCacheManagerStatus {
  SERDE_STRUCT_FIELD(logicalCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(usedCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(queued, uint64_t{0});
  SERDE_STRUCT_FIELD(loading, uint64_t{0});
  SERDE_STRUCT_FIELD(ready, uint64_t{0});
  SERDE_STRUCT_FIELD(cleaning, uint64_t{0});
  SERDE_STRUCT_FIELD(inflightBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(lastBypassReason, cache_manager::BypassReason::NONE);
};

struct LegacyCacheSpaceInfo {
  SERDE_STRUCT_FIELD(physicalDiskId, storage::PhysicalDiskId{});
  SERDE_STRUCT_FIELD(role, storage::StorageRole::INVALID);
  SERDE_STRUCT_FIELD(targets, std::vector<flat::TargetId>{});
  SERDE_STRUCT_FIELD(capacityBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(physicalUsedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(allocatableBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(reservedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(enforcedHighWatermark, double{0});
  SERDE_STRUCT_FIELD(sampledAtNs, uint64_t{0});
  SERDE_STRUCT_FIELD(eventPrepared, uint64_t{0});
  SERDE_STRUCT_FIELD(eventDeliverable, uint64_t{0});
  SERDE_STRUCT_FIELD(eventAcknowledgedSequence, uint64_t{0});
};

TEST(ServiceContracts, RejectsOversizedMetadataBatch) {
  meta::BatchImportOriginFilesReq request;
  request.entries.resize(meta::kMaxCacheBatchItems + 1);
  EXPECT_TRUE(request.valid().hasError());

  meta::EnqueueCacheBlocksReq enqueue;
  enqueue.items.resize(meta::kMaxCacheBatchItems + 1);
  EXPECT_TRUE(enqueue.valid().hasError());
}

TEST(ServiceContracts, RejectsOversizedStorageBatch) {
  storage::QueryCacheChunkGenerationsReq request;
  request.keys.resize(storage::kMaxCacheStorageBatchItems + 1);
  EXPECT_TRUE(request.valid().hasError());
}

TEST(ServiceContracts, CacheManagerSerdeRoundTrip) {
  cache_manager::EnsureCachedReq original;
  original.service = cache_manager::ServiceIdentity{"cache-manager", "token"};
  original.inode = meta::InodeId{42};
  original.beginBlock = CacheBlockIndex{3};
  original.blockCount = 10;
  original.cacheProtocolVersion = kCacheProtocolVersion;

  cache_manager::EnsureCachedReq decoded;
  ASSERT_FALSE(serde::deserialize(decoded, serde::serialize(original)).hasError());
  EXPECT_EQ(decoded.inode, original.inode);
  EXPECT_EQ(decoded.beginBlock, original.beginBlock);
  EXPECT_EQ(decoded.blockCount, original.blockCount);
}

TEST(ServiceContracts, Phase2ContractsRoundTrip) {
  cache_manager::ReportCacheAccessReq original;
  original.items.push_back({CacheBlockKey{42, CacheBlockIndex{3}}, CacheGeneration{7}, 123});
  original.cacheProtocolVersion = kCacheProtocolVersion;

  cache_manager::ReportCacheAccessReq decoded;
  ASSERT_TRUE(serde::deserialize(decoded, serde::serialize(original)));
  ASSERT_EQ(decoded.items.size(), 1);
  EXPECT_EQ(decoded.items.front().key, original.items.front().key);
  EXPECT_EQ(decoded.items.front().generation, CacheGeneration{7});
}

TEST(ServiceContracts, Phase3StatusExtensionsAreBackwardCompatible) {
  LegacyCacheManagerStatus legacy;
  legacy.ready = 7;
  cache_manager::GetCacheStatusRsp current;
  ASSERT_OK(serde::deserialize(current, serde::serialize(legacy)));
  EXPECT_EQ(current.ready, 7u);
  EXPECT_FALSE(current.phase3Enabled);
  EXPECT_EQ(current.activeJobs, 0u);
  EXPECT_EQ(current.pinnedBytes, 0u);

  current.phase3Enabled = true;
  current.activeJobs = 2;
  current.pinnedBytes = 4096;
  LegacyCacheManagerStatus oldReader;
  ASSERT_OK(serde::deserialize(oldReader, serde::serialize(current)));
  EXPECT_EQ(oldReader.ready, 7u);
}

TEST(ServiceContracts, Phase3ContractsRoundTripAndBoundPages) {
  cache_manager::CreatePrefetchJobReq create;
  create.user.uid = flat::Uid{1000};
  create.spec.jobId = PrefetchJobId{Uuid::from(1, 2)};
  create.spec.ownerUid = create.user.uid;
  DatasetSource source;
  source.source = NamespacePathSource{"/dataset", true};
  create.spec.sources.push_back(source);
  create.cacheProtocolVersion = kCachePhase3ProtocolVersion;
  ASSERT_OK(create.valid());

  cache_manager::CreatePrefetchJobReq decoded;
  ASSERT_OK(serde::deserialize(decoded, serde::serialize(create)));
  EXPECT_EQ(decoded.spec, create.spec);

  meta::ListPrefetchPlanReq page;
  page.service = {"cache-manager", "token"};
  page.jobId = create.spec.jobId;
  page.limit = meta::kMaxCacheBatchItems + 1;
  ASSERT_ERROR(page.valid(), CacheCode::kRequestTooLarge);

  ASSERT_ERROR(checkPhase3Capability(kCacheProtocolVersion, true), CacheCode::kUpgradeRequired);
  ASSERT_ERROR(checkPhase3Capability(kCachePhase3ProtocolVersion, false), CacheCode::kFeatureDisabled);
}

TEST(ServiceContracts, Phase2StatusRoundTripIncludesOperationalGates) {
  cache_manager::GetPhase2CacheStatusRsp original;
  original.enabled = true;
  original.managerEpoch = Uuid::random();
  original.admissionPolicy = "second_miss";
  original.evictionPolicy = "lru";
  original.capacityHighWatermark = 0.9;
  original.capacityLowWatermark = 0.8;
  original.snapshotMaxAgeNs = 15'000'000'000;
  original.permitTtlNs = 60'000'000'000;
  cache_manager::Phase2DiskStatus disk;
  disk.physicalDiskId.uuid = Uuid::random();
  disk.role = storage::StorageRole::CACHE_ONLY;
  disk.activeGenerations = 3;
  disk.enforcedHighWatermark = 0.9;
  disk.permitStoreHealthy = true;
  disk.eventJournalWritable = true;
  disk.admissionPaused = true;
  disk.pauseReason = "capacity_watermark";
  original.disks.push_back(disk);

  cache_manager::GetPhase2CacheStatusRsp decoded;
  ASSERT_OK(serde::deserialize(decoded, serde::serialize(original)));
  EXPECT_EQ(decoded.managerEpoch, original.managerEpoch);
  EXPECT_EQ(decoded.capacityHighWatermark, 0.9);
  ASSERT_EQ(decoded.disks.size(), size_t{1});
  EXPECT_EQ(decoded.disks.front().activeGenerations, uint64_t{3});
  EXPECT_TRUE(decoded.disks.front().permitStoreHealthy);
  EXPECT_TRUE(decoded.disks.front().eventJournalWritable);
  EXPECT_EQ(decoded.disks.front().pauseReason, "capacity_watermark");
}

TEST(ServiceContracts, Phase2StatusExtensionsAreBackwardCompatible) {
  LegacyPhase2CacheStatus legacy;
  legacy.enabled = true;
  legacy.managerEpoch = Uuid::random();
  legacy.disks.push_back({});
  legacy.disks.front().capacityBytes = 1024;

  cache_manager::GetPhase2CacheStatusRsp current;
  ASSERT_OK(serde::deserialize(current, serde::serialize(legacy)));
  EXPECT_EQ(current.managerEpoch, legacy.managerEpoch);
  ASSERT_EQ(current.disks.size(), size_t{1});
  EXPECT_EQ(current.disks.front().capacityBytes, uint64_t{1024});
  EXPECT_EQ(current.disks.front().activeGenerations, uint64_t{0});
  EXPECT_EQ(current.capacityHighWatermark, 0.0);

  current.capacityHighWatermark = 0.9;
  current.disks.front().activeGenerations = 2;
  LegacyPhase2CacheStatus oldReader;
  ASSERT_OK(serde::deserialize(oldReader, serde::serialize(current)));
  EXPECT_EQ(oldReader.managerEpoch, current.managerEpoch);
  ASSERT_EQ(oldReader.disks.size(), size_t{1});
  EXPECT_EQ(oldReader.disks.front().capacityBytes, uint64_t{1024});
}

TEST(ServiceContracts, CacheSpaceExtensionsAreBackwardCompatible) {
  LegacyCacheSpaceInfo legacy;
  legacy.physicalDiskId.uuid = Uuid::random();
  legacy.role = storage::StorageRole::CACHE_ONLY;
  legacy.capacityBytes = 4096;

  storage::CacheSpaceInfo current;
  ASSERT_OK(serde::deserialize(current, serde::serialize(legacy)));
  EXPECT_EQ(current.physicalDiskId, legacy.physicalDiskId);
  EXPECT_EQ(current.capacityBytes, uint64_t{4096});
  EXPECT_EQ(current.activeGenerations, uint64_t{0});

  current.activeGenerations = 3;
  LegacyCacheSpaceInfo oldReader;
  ASSERT_OK(serde::deserialize(oldReader, serde::serialize(current)));
  EXPECT_EQ(oldReader.physicalDiskId, current.physicalDiskId);
  EXPECT_EQ(oldReader.capacityBytes, uint64_t{4096});
}

TEST(ServiceContracts, RejectsOversizedPhase2Batches) {
  cache_manager::ReportCacheAccessReq access;
  access.items.resize(kMaxPhase2BatchItems + 1);
  ASSERT_ERROR(access.valid(), CacheCode::kRequestTooLarge);

  meta::BeginEvictCacheBlocksReq evict;
  evict.service = meta::CacheServiceIdentity{"cache-manager", "token"};
  evict.items.resize(kMaxPhase2BatchItems + 1);
  ASSERT_ERROR(evict.valid(), CacheCode::kRequestTooLarge);

  storage::PrepareCachePermitsReq permits;
  permits.items.resize(kMaxPhase2BatchItems + 1);
  ASSERT_ERROR(permits.valid(), CacheCode::kRequestTooLarge);
}

TEST(ServiceContracts, Phase2CapabilityRejectsOldOrDisabledComponents) {
  ASSERT_ERROR(checkPhase2Capability(kCacheProtocolVersion - 1, true), CacheCode::kUpgradeRequired);
  ASSERT_ERROR(checkPhase2Capability(kCacheProtocolVersion, false), CacheCode::kFeatureDisabled);
  EXPECT_TRUE(checkPhase2Capability(kCacheProtocolVersion, true));
}

}  // namespace
}  // namespace hf3fs::cache::test
