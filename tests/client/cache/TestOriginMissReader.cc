#include <atomic>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <gtest/gtest.h>

#include "client/cache/BufferAssembler.h"
#include "client/cache/LocalMissSingleflight.h"
#include "client/cache/OriginMissReader.h"
#include "client/cache/OriginRangePlanner.h"
#include "tests/GtestHelpers.h"
#include "tests/client/cache/TestOriginHelpers.h"

namespace hf3fs::client::cache {
namespace {

TEST(TestOriginRangePlanner, AlignsClipsAndSplitsRanges) {
  auto empty = OriginRangePlanner::plan(16, 4, {16, 1}, 8);
  ASSERT_OK(empty);
  EXPECT_TRUE(empty->empty());

  auto one = OriginRangePlanner::plan(17, 4, {1, 1}, 8);
  ASSERT_OK(one);
  ASSERT_EQ(one->size(), size_t{1});
  EXPECT_EQ((*one)[0], (hf3fs::cache::ByteRange{0, 4}));

  auto crossing = OriginRangePlanner::plan(17, 4, {3, 11}, 8);
  ASSERT_OK(crossing);
  ASSERT_EQ(crossing->size(), size_t{2});
  EXPECT_EQ((*crossing)[0], (hf3fs::cache::ByteRange{0, 8}));
  EXPECT_EQ((*crossing)[1], (hf3fs::cache::ByteRange{8, 8}));

  auto eof = OriginRangePlanner::plan(17, 4, {15, 20}, 8);
  ASSERT_OK(eof);
  ASSERT_EQ(eof->size(), size_t{1});
  EXPECT_EQ((*eof)[0], (hf3fs::cache::ByteRange{12, 5}));

  ASSERT_ERROR(OriginRangePlanner::plan(17, 16, {0, 1}, 8), CacheCode::kRequestTooLarge);
}

TEST(TestBufferAssembler, HandlesUnalignedEofAndUnorderedCompletion) {
  std::vector<uint8_t> output(10, 0xff);
  std::vector<OriginRangeData> ranges = {
      {{8, 8}, {8, 9, 10, 11, 12, 13, 14, 15}},
      {{0, 8}, {0, 1, 2, 3, 4, 5, 6, 7}},
  };
  auto assembled = BufferAssembler::assemble({3, 10}, 16, output, std::move(ranges));
  ASSERT_OK(assembled);
  EXPECT_EQ(*assembled, size_t{10});
  EXPECT_EQ(output, (std::vector<uint8_t>{3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));

  output.assign(10, 0xff);
  assembled = BufferAssembler::assemble({14, 10}, 16, output, {{{12, 4}, {12, 13, 14, 15}}});
  ASSERT_OK(assembled);
  EXPECT_EQ(*assembled, size_t{2});
  EXPECT_EQ(output[0], 14);
  EXPECT_EQ(output[1], 15);
  EXPECT_EQ(output[2], 0xff);
}

TEST(TestLocalMissSingleflight, SharesSuccessAndFailureThenCleansFuture) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    LocalMissSingleflight singleflight;
    folly::coro::Baton entered;
    folly::coro::Baton release;
    std::atomic<size_t> loads{0};
    auto loader = [&]() -> CoTryTask<std::vector<uint8_t>> {
      auto current = ++loads;
      if (current == 1) {
        entered.post();
        co_await release;
      }
      co_return std::vector<uint8_t>{1, 2, 3};
    };
    auto coordinator = [&]() -> CoTask<Void> {
      co_await entered;
      release.post();
      co_return Void{};
    };
    auto object = test::identity();
    auto [first, second, _] = co_await folly::coro::collectAll(singleflight.run(object, {0, 4}, loader),
                                                               singleflight.run(object, {0, 4}, loader),
                                                               coordinator());
    CO_ASSERT_OK(first);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(*first, *second);
    CO_ASSERT_EQ(loads.load(), size_t{1});
    CO_ASSERT_EQ(singleflight.active(), size_t{0});

    folly::coro::Baton failureEntered;
    folly::coro::Baton failureRelease;
    std::atomic<size_t> failureLoads{0};
    auto failures = [&]() -> CoTryTask<std::vector<uint8_t>> {
      ++failureLoads;
      failureEntered.post();
      co_await failureRelease;
      co_return makeError(CacheCode::kVersionMismatch);
    };
    auto failureCoordinator = [&]() -> CoTask<Void> {
      co_await failureEntered;
      failureRelease.post();
      co_return Void{};
    };
    auto [failedFirst, failedSecond, unused] =
        co_await folly::coro::collectAll(singleflight.run(object, {0, 4}, failures),
                                         singleflight.run(object, {0, 4}, failures),
                                         failureCoordinator());
    CO_ASSERT_ERROR(failedFirst, CacheCode::kVersionMismatch);
    CO_ASSERT_ERROR(failedSecond, CacheCode::kVersionMismatch);
    CO_ASSERT_EQ(failureLoads.load(), size_t{1});
    CO_ASSERT_EQ(singleflight.active(), size_t{0});
    size_t retryLoads = 0;
    auto retried = co_await singleflight.run(object, {0, 4}, [&]() -> CoTryTask<std::vector<uint8_t>> {
      ++retryLoads;
      co_return std::vector<uint8_t>{4};
    });
    CO_ASSERT_OK(retried);
    CO_ASSERT_EQ(retryLoads, size_t{1});
  }());
}

TEST(TestOriginMissReader, DoesNotExposePartialDataOnSegmentFailure) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto bytes = test::sequence(24);
    test::FakeObjectStore store(bytes);
    store.failureCall = 2;
    store.failureCode = CacheCode::kVersionMismatch;
    LocalMissSingleflight singleflight;
    OriginMissReader reader(store, singleflight, {.maxRangeBytes = 8, .maxInflightBytes = 8});
    std::vector<uint8_t> output(16, 0xaa);
    auto result = co_await reader.read(test::identity(), bytes.size(), 4, 1, output);
    CO_ASSERT_ERROR(result, CacheCode::kVersionMismatch);
    CO_ASSERT_EQ(output, std::vector<uint8_t>(16, 0xaa));
  }());
}

}  // namespace
}  // namespace hf3fs::client::cache
