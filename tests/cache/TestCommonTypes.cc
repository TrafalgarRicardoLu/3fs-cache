#include <cerrno>
#include <gtest/gtest.h>
#include <limits>

#include "common/serde/Serde.h"
#include "fbs/cache/Common.h"

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

TEST(CacheCommonTypes, CacheErrorsHaveStableNames) {
  EXPECT_EQ(StatusCode::toString(CacheCode::kFeatureDisabled), "Cache::FeatureDisabled");
  EXPECT_EQ(StatusCode::toErrno(CacheCode::kReadOnlyOriginFile), EROFS);
}

}  // namespace
}  // namespace hf3fs::cache::test
