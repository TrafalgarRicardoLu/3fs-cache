#include <atomic>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <gtest/gtest.h>

#include "client/cache/CacheReadPipeline.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"
#include "tests/client/cache/TestOriginHelpers.h"

namespace hf3fs::client::cache {
namespace {

TEST(TestCacheReadPipelineMiss, ReadsZeroUnalignedCrossBlockAndEof) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto bytes = test::sequence(17);
    test::FakeObjectStore store(bytes);
    LocalMissSingleflight singleflight;
    OriginMissReader missReader(store, singleflight, {.maxRangeBytes = 8, .maxInflightBytes = 8});
    CacheReadPipeline pipeline(missReader);
    auto inode = meta::server::Inode::newOriginFile(meta::InodeId{1},
                                                    meta::Acl{},
                                                    bytes.size(),
                                                    meta::Layout::newEmpty(flat::ChainTableId{1}, 4, 1),
                                                    test::identity(),
                                                    UtcClock::now());

    std::vector<uint8_t> empty;
    auto result = co_await pipeline.read(inode, std::nullopt, 0, empty);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(*result, size_t{0});
    CO_ASSERT_EQ(store.calls.load(), size_t{0});

    std::vector<uint8_t> output(11);
    result = co_await pipeline.read(inode, std::nullopt, 3, output);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(*result, output.size());
    CO_ASSERT_EQ(output, std::vector<uint8_t>(bytes.begin() + 3, bytes.begin() + 14));

    output.assign(8, 0xaa);
    result = co_await pipeline.read(inode, std::nullopt, 15, output);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(*result, size_t{2});
    CO_ASSERT_EQ(output[0], 15);
    CO_ASSERT_EQ(output[1], 16);
    CO_ASSERT_EQ(output[2], 0xaa);
  }());
}

TEST(TestCacheReadPipelineMiss, RejectsRegularFilesWithoutCallingOrigin) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    test::FakeObjectStore store(test::sequence(8));
    LocalMissSingleflight singleflight;
    OriginMissReader missReader(store, singleflight);
    CacheReadPipeline pipeline(missReader);
    meta::Inode regular(meta::InodeId{1}, meta::InodeData{});
    std::vector<uint8_t> output(4);
    auto result = co_await pipeline.read(regular, std::nullopt, 0, output);
    CO_ASSERT_ERROR(result, StatusCode::kInvalidArg);
    CO_ASSERT_EQ(store.calls.load(), size_t{0});
  }());
}

TEST(TestCacheReadPipelineMiss, FullIdentitySeparatesSingleflightKeys) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    LocalMissSingleflight singleflight;
    std::atomic<size_t> loads{0};
    folly::coro::Baton bothEntered;
    folly::coro::Baton release;
    auto load = [&]() -> CoTryTask<std::vector<uint8_t>> {
      if (++loads == 2) bothEntered.post();
      co_await release;
      co_return std::vector<uint8_t>{1};
    };
    auto coordinator = [&]() -> CoTask<Void> {
      co_await bothEntered;
      release.post();
      co_return Void{};
    };
    auto [first, second, unused] =
        co_await folly::coro::collectAll(singleflight.run(test::identity("bucket-a", "key"), {0, 1}, load),
                                         singleflight.run(test::identity("bucket-b", "key"), {0, 1}, load),
                                         coordinator());
    CO_ASSERT_OK(first);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(loads.load(), size_t{2});
  }());
}

}  // namespace
}  // namespace hf3fs::client::cache
