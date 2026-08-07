#include <array>
#include <gtest/gtest.h>
#include <set>

#include "common/kv/KeyPrefix.h"
#include "meta/store/cache/OrchestrationKey.h"
#include "meta/store/cache/UploadJobKey.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

TEST(TestOrchestrationKey, PrefixesAreUniqueAndStable) {
  std::array prefixes{kv::KeyPrefix::PrefetchJob,
                      kv::KeyPrefix::PrefetchPlan,
                      kv::KeyPrefix::PinByBlock,
                      kv::KeyPrefix::PinByOwner,
                      kv::KeyPrefix::UploadJob};
  std::set<uint32_t> values;
  for (auto prefix : prefixes) values.insert(static_cast<uint32_t>(prefix));
  EXPECT_EQ(values.size(), prefixes.size());
  EXPECT_EQ(kv::toStr(kv::KeyPrefix::PrefetchJob), "PFJB");
  EXPECT_EQ(kv::toStr(kv::KeyPrefix::PrefetchPlan), "PFPL");
  EXPECT_EQ(kv::toStr(kv::KeyPrefix::PinByBlock), "PNBL");
  EXPECT_EQ(kv::toStr(kv::KeyPrefix::PinByOwner), "PNOW");
  EXPECT_EQ(kv::toStr(kv::KeyPrefix::UploadJob), "UPJB");
}

TEST(TestOrchestrationKey, UploadJobRoundTripAndRejectsMalformedKeys) {
  cache::UploadJobId jobId{Uuid::from(5, 6)};
  auto key = UploadJobKey::job(jobId);
  ASSERT_EQ(*UploadJobKey::unpack(key), jobId);
  EXPECT_TRUE(key.starts_with(UploadJobKey::prefix()));
  key.push_back('x');
  EXPECT_TRUE(UploadJobKey::unpack(key).hasError());
  EXPECT_TRUE(UploadJobKey::unpack(OrchestrationKey::job(cache::PrefetchJobId{Uuid::from(5, 6)})).hasError());
}

TEST(TestOrchestrationKey, JobAndPlanRoundTripAndSortByBlock) {
  cache::PrefetchJobId jobId{Uuid::from(1, 2)};
  ASSERT_EQ(*OrchestrationKey::unpackJob(OrchestrationKey::job(jobId)), jobId);

  PrefetchPlanKey first{jobId, {9, cache::CacheBlockIndex{2}}};
  PrefetchPlanKey second{jobId, {10, cache::CacheBlockIndex{1}}};
  auto firstKey = OrchestrationKey::plan(first.jobId, first.block);
  auto secondKey = OrchestrationKey::plan(second.jobId, second.block);
  EXPECT_LT(firstKey, secondKey);
  ASSERT_EQ(*OrchestrationKey::unpackPlan(firstKey), first);
  EXPECT_TRUE(firstKey.starts_with(OrchestrationKey::planPrefix(jobId)));
}

TEST(TestOrchestrationKey, PinIndexesRoundTripToSameIdentity) {
  PinIndexKey expected{{42, cache::CacheBlockIndex{7}},
                       {cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerId{Uuid::from(3, 4)}}};
  auto byBlock = OrchestrationKey::pinByBlock(expected.block, expected.owner);
  auto byOwner = OrchestrationKey::pinByOwner(expected.owner, expected.block);
  ASSERT_EQ(*OrchestrationKey::unpackPinByBlock(byBlock), expected);
  ASSERT_EQ(*OrchestrationKey::unpackPinByOwner(byOwner), expected);
  EXPECT_TRUE(byBlock.starts_with(OrchestrationKey::pinByBlockPrefix(expected.block)));
  EXPECT_TRUE(byOwner.starts_with(OrchestrationKey::pinByOwnerPrefix(expected.owner)));
  EXPECT_NE(byBlock.substr(0, 4), byOwner.substr(0, 4));
}

TEST(TestOrchestrationKey, RejectsWrongPrefixAndTrailingBytes) {
  cache::PrefetchJobId jobId{Uuid::from(1, 2)};
  auto key = OrchestrationKey::job(jobId);
  key.push_back('x');
  EXPECT_TRUE(OrchestrationKey::unpackJob(key).hasError());
  EXPECT_TRUE(OrchestrationKey::unpackPlan(OrchestrationKey::job(jobId)).hasError());
}

}  // namespace
}  // namespace hf3fs::meta::server
