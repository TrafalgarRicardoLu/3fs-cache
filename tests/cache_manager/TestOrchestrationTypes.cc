#include <gtest/gtest.h>
#include <limits>

#include "common/serde/Serde.h"
#include "fbs/cache_manager/Common.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

DatasetSource namespaceSource(std::string path = "/dataset") {
  DatasetSource source;
  source.source = NamespacePathSource{std::move(path), true};
  return source;
}

PrefetchJobSpec validSpec() {
  PrefetchJobSpec spec;
  spec.jobId = cache::PrefetchJobId{Uuid::from(1, 2)};
  spec.ownerUid = flat::Uid{1000};
  spec.sources = {namespaceSource()};
  spec.priority = 10;
  spec.maxParallelLoads = 4;
  spec.bandwidthLimitBytesPerSec = 1024;
  spec.requiredReadyBps = 9000;
  spec.pinAfterReady = true;
  spec.pinTtlMs = 60000;
  return spec;
}

TEST(OrchestrationTypes, DatasetSourcesRoundTrip) {
  std::vector<DatasetSource> sources;
  sources.push_back(namespaceSource());
  DatasetSource paths;
  paths.source = PathListSource{{"/first", "/second"}};
  sources.push_back(paths);
  DatasetSource manifest;
  manifest.source = ManifestPathSource{"/manifest"};
  sources.push_back(manifest);
  DatasetSource prefix;
  prefix.source = S3PrefixSource{cache::OriginId{1}, "bucket", "prefix/", "/imported"};
  sources.push_back(prefix);

  for (size_t index = 0; index < sources.size(); ++index) {
    ASSERT_OK(sources[index].valid());
    EXPECT_EQ(sources[index].type(), static_cast<DatasetSourceType>(index));
    DatasetSource decoded;
    ASSERT_OK(serde::deserialize(decoded, serde::serialize(sources[index])));
    EXPECT_EQ(decoded, sources[index]);
  }
}

TEST(OrchestrationTypes, RejectsInvalidSourceShapes) {
  EXPECT_TRUE(namespaceSource("relative").valid().hasError());
  EXPECT_TRUE(namespaceSource("/root/../escape").valid().hasError());

  DatasetSource emptyPaths;
  emptyPaths.source = PathListSource{};
  EXPECT_TRUE(emptyPaths.valid().hasError());

  DatasetSource invalidPrefix;
  invalidPrefix.source = S3PrefixSource{cache::OriginId{}, "bucket", "prefix", "/target"};
  EXPECT_TRUE(invalidPrefix.valid().hasError());

  DatasetSource oversizedPaths;
  oversizedPaths.source = PathListSource{std::vector<std::string>(257, "/" + std::string(4094, 'x'))};
  EXPECT_TRUE(oversizedPaths.valid().hasError());
}

TEST(OrchestrationTypes, JobSpecRoundTripsAndUsesHigherNumericPriority) {
  auto spec = validSpec();
  ASSERT_OK(spec.valid());
  EXPECT_GT(spec.priority, uint32_t{1});

  PrefetchJobSpec decoded;
  ASSERT_OK(serde::deserialize(decoded, serde::serialize(spec)));
  EXPECT_EQ(decoded, spec);
}

TEST(OrchestrationTypes, RejectsJobSpecBoundsAndInconsistentPin) {
  auto spec = validSpec();
  spec.requiredReadyBps = 0;
  EXPECT_TRUE(spec.valid().hasError());
  spec.requiredReadyBps = kReadyRatioScaleBps + 1;
  EXPECT_TRUE(spec.valid().hasError());

  spec = validSpec();
  spec.priority = static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) + 1;
  EXPECT_TRUE(spec.valid().hasError());

  spec = validSpec();
  spec.pinAfterReady = false;
  EXPECT_TRUE(spec.valid().hasError());

  spec = validSpec();
  spec.pinTtlMs = kMaxPinTtlMs + 1;
  EXPECT_TRUE(spec.valid().hasError());
}

TEST(OrchestrationTypes, JobRecordRejectsOverlappingCounters) {
  PrefetchJobRecord record;
  record.spec = validSpec();
  record.state = PrefetchJobState::LOADING;
  record.stateVersion = 1;
  record.createdAtMs = 100;
  record.updatedAtMs = 101;
  record.plannedBytes = 1000;
  record.readyBytes = 700;
  record.failedBytes = 400;
  record.plannedBlocks = 10;
  record.readyBlocks = 7;
  record.failedBlocks = 4;
  EXPECT_TRUE(record.valid().hasError());

  record.readyBytes = 600;
  record.readyBlocks = 6;
  ASSERT_OK(record.valid());
}

TEST(OrchestrationTypes, AppendedSpecFieldsKeepDefaults) {
  struct LegacySpec {
    SERDE_STRUCT_FIELD(jobId, cache::PrefetchJobId{});
    SERDE_STRUCT_FIELD(ownerUid, flat::Uid{});
    SERDE_STRUCT_FIELD(sources, std::vector<DatasetSource>{});
    SERDE_STRUCT_FIELD(priority, uint32_t{});
  };

  LegacySpec legacy;
  legacy.jobId = cache::PrefetchJobId{Uuid::from(1, 2)};
  legacy.ownerUid = flat::Uid{1000};
  legacy.sources = {namespaceSource()};
  legacy.priority = 3;

  PrefetchJobSpec current;
  ASSERT_OK(serde::deserialize(current, serde::serialize(legacy)));
  EXPECT_EQ(current.maxParallelLoads, uint32_t{1});
  EXPECT_EQ(current.requiredReadyBps, kReadyRatioScaleBps);
  EXPECT_FALSE(current.pinAfterReady);
  EXPECT_EQ(current.pinTtlMs, uint64_t{0});
  EXPECT_TRUE(current.loadMissing);
  ASSERT_OK(current.valid());
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
