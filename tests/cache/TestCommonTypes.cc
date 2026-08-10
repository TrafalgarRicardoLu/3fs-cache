#include <cerrno>
#include <gtest/gtest.h>
#include <limits>

#include "common/serde/Serde.h"
#include "fbs/cache/Common.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::test {
namespace {

TEST(CacheCommonTypes, ObjectIdentityIncludesObjectLocation) {
  auto version = VersionSelector{VersionSelectorType::STRONG_ETAG, "etag"};
  ImmutableObjectIdentity first{OriginId{1}, "bucket", "first", version};
  ImmutableObjectIdentity second{OriginId{1}, "bucket", "second", version};

  ASSERT_TRUE(first.valid());
  ASSERT_TRUE(second.valid());
  EXPECT_NE(first, second);
  ImmutableObjectIdentity decoded;
  auto result = serde::deserialize(decoded, serde::serialize(first));
  ASSERT_FALSE(result.hasError());
  EXPECT_EQ(decoded, first);
}

TEST(CacheCommonTypes, BlockAndReadyIdentityRoundTrip) {
  CacheBlockKey block{42, CacheBlockIndex{7}};
  ReadyIdentity ready{3, CacheGeneration{9}, 1, 1234, 4096};

  ASSERT_FALSE(block.valid().hasError());
  ASSERT_FALSE(ready.valid().hasError());
  CacheBlockKey decodedBlock;
  ReadyIdentity decodedReady;
  ASSERT_FALSE(serde::deserialize(decodedBlock, serde::serialize(block)).hasError());
  ASSERT_FALSE(serde::deserialize(decodedReady, serde::serialize(ready)).hasError());
  EXPECT_EQ(decodedBlock, block);
  EXPECT_EQ(decodedReady, ready);
}

TEST(CacheCommonTypes, GenerationUsesUnsignedOrdering) {
  EXPECT_LT(CacheGeneration{1}, CacheGeneration{2});
  EXPECT_LT(CacheGeneration{std::numeric_limits<uint64_t>::max() - 1},
            CacheGeneration{std::numeric_limits<uint64_t>::max()});
}

TEST(CacheCommonTypes, RejectsInvalidVersionSelectors) {
  EXPECT_TRUE(VersionSelector{}.valid().hasError());
  EXPECT_TRUE((VersionSelector{VersionSelectorType::STRONG_ETAG, "W/weak"}.valid().hasError()));
}

TEST(CacheCommonTypes, ChecksRangeOverflow) {
  EXPECT_EQ(*(ByteRange{1, 2}.end()), uint64_t{3});
  EXPECT_TRUE((ByteRange{std::numeric_limits<uint64_t>::max(), 1}.end().hasError()));
}

TEST(CacheCommonTypes, OrchestrationRecordsRoundTrip) {
  PrefetchJobId jobId{Uuid::from(1, 2)};
  PrefetchPlanEntry plan{jobId,
                         CacheBlockKey{42, CacheBlockIndex{7}},
                         4096,
                         10,
                         PrefetchPlanEntryState::ADMITTED,
                         Uuid::from(3, 4)};
  PinRecord pin{CacheBlockKey{42, CacheBlockIndex{7}},
                PinOwner{PinOwnerKind::ACTIVE_JOB, PinOwnerId{Uuid::from(1, 2)}},
                100,
                200,
                CacheGeneration{9}};

  ASSERT_OK(plan.valid());
  ASSERT_OK(pin.valid());
  PrefetchPlanEntry decodedPlan;
  PinRecord decodedPin;
  ASSERT_OK(serde::deserialize(decodedPlan, serde::serialize(plan)));
  ASSERT_OK(serde::deserialize(decodedPin, serde::serialize(pin)));
  EXPECT_EQ(decodedPlan, plan);
  EXPECT_EQ(decodedPin, pin);
}

TEST(CacheCommonTypes, RejectsInvalidOrchestrationRecords) {
  PrefetchPlanEntry plan{PrefetchJobId{Uuid::from(1, 2)},
                         CacheBlockKey{42, CacheBlockIndex{7}},
                         4096,
                         0,
                         PrefetchPlanEntryState::PLANNED,
                         Uuid::from(3, 4)};
  EXPECT_TRUE(plan.valid().hasError());

  PinRecord pin{CacheBlockKey{42, CacheBlockIndex{7}},
                PinOwner{PinOwnerKind::EXPLICIT_PIN, PinOwnerId{Uuid::from(5, 6)}},
                200,
                100,
                CacheGeneration{}};
  EXPECT_TRUE(pin.valid().hasError());
}

TEST(CacheCommonTypes, PhaseFourRecordsRoundTrip) {
  UploadJobRecord upload;
  upload.jobId = UploadJobId{Uuid::from(7, 8)};
  upload.ownerUid = flat::Uid{1};
  upload.path = "/checkpoints/model";
  upload.stagingInode = 42;
  upload.stagingLength = 4096;
  upload.destination = {OriginId{1}, "bucket", "checkpoints/model-7"};
  upload.multipartId = "upload-id";
  upload.nextPartNumber = 2;
  upload.parts = {{1, 4096, "etag", "checksum"}};
  upload.state = UploadJobState::PUBLISHING;
  upload.stateVersion = 5;
  upload.createdAtMs = 100;
  upload.updatedAtMs = 200;
  upload.completedObject =
      ImmutableObjectIdentity{OriginId{1}, "bucket", "checkpoints/model-7", {VersionSelectorType::VERSION_ID, "v1"}};

  ReconcileProgress reconcile{ReconcileRunId{Uuid::from(9, 10)},
                              ReconcileRunState::DEGRADED,
                              100,
                              200,
                              10,
                              2,
                              1,
                              1,
                              1,
                              "retryable target"};

  ASSERT_OK(upload.valid());
  ASSERT_OK(reconcile.valid());
  UploadJobRecord decodedUpload;
  ReconcileProgress decodedReconcile;
  ASSERT_OK(serde::deserialize(decodedUpload, serde::serialize(upload)));
  ASSERT_OK(serde::deserialize(decodedReconcile, serde::serialize(reconcile)));
  EXPECT_EQ(decodedUpload, upload);
  EXPECT_EQ(decodedReconcile, reconcile);

  upload.state = UploadJobState::FAILED;
  upload.error = "publish rejected";
  upload.orphanCleanupState = OrphanCleanupState::DELETING;
  upload.orphanCleanupOperationId = Uuid::from(11, 12);
  upload.orphanCleanupEligibleAtMs = 300;
  upload.orphanCleanupAttempts = 1;
  ASSERT_OK(upload.valid());
  ASSERT_OK(serde::deserialize(decodedUpload, serde::serialize(upload)));
  EXPECT_EQ(decodedUpload, upload);
}

TEST(CacheCommonTypes, RejectsInvalidPhaseFourRecords) {
  CompletedUploadPart invalidPart{0, 0, "", {}};
  EXPECT_TRUE(invalidPart.valid().hasError());

  UploadJobRecord open;
  open.jobId = UploadJobId{Uuid::from(7, 8)};
  open.path = "/checkpoints/model";
  open.stagingInode = 42;
  open.destination = {OriginId{1}, "bucket", "checkpoints/model-7"};
  open.state = UploadJobState::OPEN;
  open.stateVersion = 1;
  open.createdAtMs = open.updatedAtMs = 100;
  EXPECT_TRUE(open.valid().hasError());

  ReconcileProgress healthy{ReconcileRunId{Uuid::from(9, 10)}, ReconcileRunState::HEALTHY, 100, 200, 1, 0, 0, 0, 1, {}};
  EXPECT_TRUE(healthy.valid().hasError());
}

TEST(CacheCommonTypes, ChecksPhaseFourCapability) {
  ASSERT_OK(checkPhase4Capability(kCachePhase4ProtocolVersion, true));
  ASSERT_ERROR(checkPhase4Capability(kCachePhase3ProtocolVersion, true), CacheCode::kUpgradeRequired);
  ASSERT_ERROR(checkPhase4Capability(kCachePhase4ProtocolVersion, false), CacheCode::kFeatureDisabled);
}

TEST(CacheCommonTypes, CacheErrorsHaveStableNames) {
  EXPECT_EQ(StatusCode::toString(CacheCode::kFeatureDisabled), "Cache::FeatureDisabled");
  EXPECT_EQ(StatusCode::toErrno(CacheCode::kReadOnlyOriginFile), EROFS);
}

}  // namespace
}  // namespace hf3fs::cache::test
