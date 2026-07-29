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
