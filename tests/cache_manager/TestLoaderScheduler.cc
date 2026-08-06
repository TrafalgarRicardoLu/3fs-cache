#include <gtest/gtest.h>

#include "cache_manager/admission/CapacityGate.h"
#include "cache_manager/scheduler/HintCoalescer.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

TEST(TestHintCoalescer, DeduplicatesAndPreservesPriorityFifo) {
  HintCoalescer hints;
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 1}));
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{1}, 4096, EnsureReason::PREFETCH, 1}));
  ASSERT_FALSE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 10}));
  ASSERT_EQ(hints.size(), size_t{2});
  auto first = hints.pop();
  ASSERT_TRUE(first.has_value());
  ASSERT_EQ(first->block, cache::CacheBlockIndex{0});
  ASSERT_EQ(first->priority, 10);
  auto second = hints.pop();
  ASSERT_EQ(second->block, cache::CacheBlockIndex{1});
}

TEST(TestHintCoalescer, PopsContiguousRangeWithinByteLimit) {
  HintCoalescer hints;
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096}));
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{1}, 4096}));
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{2}, 4096}));
  auto batch = hints.popBatch(8192);
  ASSERT_EQ(batch.size(), size_t{2});
  ASSERT_EQ(batch[0].block, cache::CacheBlockIndex{0});
  ASSERT_EQ(batch[1].block, cache::CacheBlockIndex{1});
  ASSERT_EQ(hints.size(), size_t{1});
}

TEST(TestHintCoalescer, MergesJobClaimsAndCancelsOnlySelectedOwner) {
  HintCoalescer hints;
  auto foreground = LoadHint{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 3};
  ASSERT_TRUE(hints.enqueue(foreground));
  std::vector<status_code_t> completions;
  auto firstJob = cache::PrefetchJobId{Uuid::from(1, 1)};
  auto secondJob = cache::PrefetchJobId{Uuid::from(1, 2)};
  LoadHint first{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 7};
  first.jobClaims.push_back({firstJob, 7, [&](const Status &status) { completions.push_back(status.code()); }});
  ASSERT_OK(hints.attach(std::move(first)));
  LoadHint second{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 9};
  second.jobClaims.push_back({secondJob, 9, [&](const Status &status) { completions.push_back(status.code()); }});
  ASSERT_OK(hints.attach(std::move(second)));

  ASSERT_TRUE(hints.cancel({1, cache::CacheBlockIndex{0}}, firstJob));
  auto merged = hints.pop();
  ASSERT_TRUE(merged);
  EXPECT_EQ(merged->priority, 9);
  ASSERT_EQ(merged->jobClaims.size(), size_t{1});
  EXPECT_EQ(merged->jobClaims[0].jobId, secondJob);
  merged->notify(Status::OK);
  EXPECT_EQ(completions, (std::vector<status_code_t>{StatusCode::kOK}));
}

TEST(TestHintCoalescer, BoundsJobClaimsWithoutDroppingExistingOwners) {
  HintCoalescer hints;
  LoadHint initial{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 1};
  for (size_t index = 0; index < kMaxJobClaimsPerHint; ++index) {
    initial.jobClaims.push_back({cache::PrefetchJobId{Uuid::from(1, index + 1)}, static_cast<int32_t>(index), {}});
  }
  ASSERT_OK(hints.attach(std::move(initial)));
  LoadHint overflow{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 100};
  overflow.jobClaims.push_back({cache::PrefetchJobId{Uuid::from(2, 1)}, 100, {}});
  ASSERT_ERROR(hints.attach(std::move(overflow)), StatusCode::kQueueConflict);
  auto retained = hints.pop();
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->jobClaims.size(), kMaxJobClaimsPerHint);
}

TEST(TestHintCoalescer, CountsOnlyExclusiveQueuedJobClaimsForDrain) {
  HintCoalescer hints;
  LoadHint prefetch{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 7};
  prefetch.jobClaims.push_back({cache::PrefetchJobId{Uuid::from(1, 1)}, 7, {}});
  ASSERT_TRUE(hints.enqueue(std::move(prefetch)));
  EXPECT_EQ(hints.exclusiveJobClaims(), 1u);

  LoadHint foreground{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 3};
  ASSERT_OK(hints.attach(std::move(foreground)));
  EXPECT_EQ(hints.exclusiveJobClaims(), 0u);
}

TEST(TestHintCoalescer, StrictPriorityPreventsAdjacentBlocksFromRidingAlong) {
  HintCoalescer hints;
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 10}));
  ASSERT_TRUE(hints.enqueue({meta::InodeId{1}, cache::CacheBlockIndex{1}, 4096, EnsureReason::PREFETCH, 1}));
  ASSERT_TRUE(hints.enqueue({meta::InodeId{2}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 5}));
  auto first = hints.popBatch(1U << 20);
  ASSERT_EQ(first.size(), size_t{1});
  EXPECT_EQ(first[0].priority, 10);
  auto second = hints.pop();
  ASSERT_TRUE(second);
  EXPECT_EQ(second->priority, 5);
}

TEST(TestHintCoalescer, ContiguousBatchRequiresMatchingJobOwners) {
  HintCoalescer hints;
  auto firstJob = cache::PrefetchJobId{Uuid::from(1, 1)};
  auto secondJob = cache::PrefetchJobId{Uuid::from(1, 2)};
  LoadHint first{meta::InodeId{1}, cache::CacheBlockIndex{0}, 4096, EnsureReason::PREFETCH, 7};
  first.jobClaims.push_back({firstJob, 7, {}});
  LoadHint next{meta::InodeId{1}, cache::CacheBlockIndex{1}, 4096, EnsureReason::PREFETCH, 7};
  next.jobClaims.push_back({secondJob, 7, {}});
  ASSERT_OK(hints.attach(std::move(first)));
  ASSERT_OK(hints.attach(std::move(next)));
  EXPECT_EQ(hints.popBatch(8192).size(), size_t{1});
  EXPECT_EQ(hints.size(), size_t{1});
}

TEST(TestCapacityGate, EnforcesGlobalAndOriginLimitsAndReleases) {
  CapacityGate gate({2, 8192}, {{cache::OriginId{1}, {1, 4096}}, {cache::OriginId{2}, {2, 8192}}});
  auto first = gate.tryAcquire(cache::OriginId{1}, 4096);
  ASSERT_OK(first);
  ASSERT_ERROR(gate.tryAcquire(cache::OriginId{1}, 1), CacheCode::kCapacityExceeded);
  auto second = gate.tryAcquire(cache::OriginId{2}, 4096);
  ASSERT_OK(second);
  ASSERT_ERROR(gate.tryAcquire(cache::OriginId{2}, 1), CacheCode::kCapacityExceeded);
  ASSERT_EQ(gate.inflightBytes(), uint64_t{8192});
  first = CapacityGate::Permit{};
  ASSERT_EQ(gate.inflightBytes(), uint64_t{4096});
  ASSERT_OK(gate.tryAcquire(cache::OriginId{1}, 4096));
  ASSERT_ERROR(gate.tryAcquire(cache::OriginId{9}, 1), StatusCode::kInvalidConfig);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
