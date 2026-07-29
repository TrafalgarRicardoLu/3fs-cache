#include <gtest/gtest.h>

#include "client/cache/CacheHitReader.h"
#include "tests/GtestHelpers.h"
#include "tests/client/cache/TestOriginHelpers.h"

namespace hf3fs::client::cache {
namespace {

meta::ReadBlockPlan readyPlan(std::span<const uint8_t> data) {
  auto checksum = storage::ChecksumInfo::create(storage::ChecksumType::CRC32C, data.data(), data.size());
  meta::ReadBlockPlan plan;
  plan.actualBlockLength = data.size();
  plan.ready = hf3fs::cache::ReadyIdentity{1,
                                           hf3fs::cache::CacheGeneration{2},
                                           static_cast<uint8_t>(checksum.type),
                                           checksum.value,
                                           data.size()};
  return plan;
}

TEST(TestCacheHitReader, ValidatesGenerationLengthAndChecksum) {
  auto data = test::sequence(8);
  auto plan = readyPlan(data);
  ASSERT_OK(StorageCacheHitReader::validate(plan, data, hf3fs::cache::CacheGeneration{2}));
  ASSERT_ERROR(StorageCacheHitReader::validate(plan, data, hf3fs::cache::CacheGeneration{3}),
               CacheCode::kStaleGeneration);
  ASSERT_ERROR(StorageCacheHitReader::validate(plan,
                                               std::span<const uint8_t>{data.data(), data.size()}.first(7),
                                               hf3fs::cache::CacheGeneration{2}),
               CacheCode::kInvalidResponse);
  data[0] ^= 1;
  ASSERT_ERROR(StorageCacheHitReader::validate(plan, data, hf3fs::cache::CacheGeneration{2}),
               StorageClientCode::kChecksumMismatch);
}

}  // namespace
}  // namespace hf3fs::client::cache
