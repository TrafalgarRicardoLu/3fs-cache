#include <gtest/gtest.h>
#include <limits>

#include "common/serde/Serde.h"
#include "fbs/cache/Common.h"
#include "fbs/storage/Common.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::test {
namespace {

storage::PlacementIdentity placement() {
  auto result = storage::PlacementIdentity::create({flat::ChainId{7}, flat::ChainVersion{3}},
                                                   {flat::TargetId{3}, flat::TargetId{1}, flat::TargetId{3}},
                                                   flat::TargetId{1},
                                                   Uuid::from(1, 2));
  EXPECT_TRUE(result.hasValue());
  return *result;
}

TEST(Phase2CacheCommon, UsesPhase2CapabilityVersions) {
  EXPECT_EQ(kCacheSchemaVersion, 2);
  EXPECT_EQ(kCacheProtocolVersion, 2);
  EXPECT_NE(kCacheProtocolVersion, 1);
}

TEST(Phase2CacheCommon, KeepsCapacityErrorNamesStable) {
  EXPECT_EQ(StatusCode::toString(CacheCode::kRoleMismatch), "Cache::RoleMismatch");
  EXPECT_EQ(StatusCode::toString(CacheCode::kPermitExpired), "Cache::PermitExpired");
  EXPECT_EQ(StatusCode::toString(CacheCode::kPermitConflict), "Cache::PermitConflict");
  EXPECT_EQ(StatusCode::toString(CacheCode::kPlacementMismatch), "Cache::PlacementMismatch");
  EXPECT_EQ(StatusCode::toString(CacheCode::kEventGap), "Cache::EventGap");
  EXPECT_EQ(StatusCode::toString(CacheCode::kJournalFull), "Cache::JournalFull");
}

TEST(Phase2CacheCommon, KeepsStorageEventWireValuesStable) {
  EXPECT_EQ(static_cast<uint8_t>(CacheStorageEventType::DELETED), 0);
  EXPECT_EQ(static_cast<uint8_t>(CacheStorageEventType::EMERGENCY_EVICTED), 1);
  EXPECT_EQ(static_cast<uint8_t>(CacheStorageEventType::LOST), 2);
  EXPECT_EQ(static_cast<uint8_t>(CacheStorageEventType::CORRUPTED), 3);
}

TEST(Phase2CacheCommon, RejectsEvictionEpochOverflow) {
  EXPECT_EQ(*nextEvictionEpoch(EvictionEpoch{}), EvictionEpoch{1});
  ASSERT_ERROR(nextEvictionEpoch(EvictionEpoch{std::numeric_limits<uint64_t>::max()}), CacheCode::kStateConflict);
}

TEST(Phase2CacheCommon, PhysicalDiskIdRoundTripsAndRejectsZero) {
  storage::PhysicalDiskId original{Uuid::from(3, 4)};
  ASSERT_TRUE(original.valid());
  storage::PhysicalDiskId decoded;
  ASSERT_TRUE(serde::deserialize(decoded, serde::serialize(original)));
  EXPECT_EQ(decoded, original);
  EXPECT_TRUE(storage::PhysicalDiskId{}.valid().hasError());
}

TEST(Phase2CacheCommon, PlacementCreationSortsAndDeduplicatesReplicas) {
  auto original = placement();
  EXPECT_EQ(original.expectedReplicaTargets, (std::vector{flat::TargetId{1}, flat::TargetId{3}}));
  ASSERT_TRUE(original.valid());

  storage::PlacementIdentity decoded;
  ASSERT_TRUE(serde::deserialize(decoded, serde::serialize(original)));
  EXPECT_EQ(decoded, original);
}

TEST(Phase2CacheCommon, PlacementRejectsInvalidIdentity) {
  auto original = placement();
  original.expectedReplicaTargets = {flat::TargetId{3}, flat::TargetId{1}};
  ASSERT_ERROR(original.valid(), CacheCode::kPlacementMismatch);

  original = placement();
  original.coordinatorTargetId = flat::TargetId{2};
  ASSERT_ERROR(original.valid(), CacheCode::kPlacementMismatch);

  original = placement();
  original.admissionAttemptId = Uuid::zero();
  EXPECT_TRUE(original.valid().hasError());
}

TEST(Phase2CacheCommon, PermitRoundTripsAndCoversExactPlacement) {
  storage::PermitIdentity original{Uuid::from(5, 6),
                                   placement(),
                                   9,
                                   {{flat::TargetId{1}, 4096}, {flat::TargetId{3}, 8192}}};
  ASSERT_TRUE(original.valid());

  storage::PermitIdentity decoded;
  ASSERT_TRUE(serde::deserialize(decoded, serde::serialize(original)));
  EXPECT_EQ(decoded, original);

  original.footprintByTarget.erase(flat::TargetId{3});
  ASSERT_ERROR(original.valid(), CacheCode::kPermitConflict);
  EXPECT_TRUE(storage::PermitIdentity{}.valid().hasError());
}

}  // namespace
}  // namespace hf3fs::cache::test
