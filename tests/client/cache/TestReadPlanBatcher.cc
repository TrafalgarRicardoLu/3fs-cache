#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "client/cache/ReadPlanBatcher.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"
#include "tests/client/cache/TestOriginHelpers.h"

namespace hf3fs::client::cache {
namespace {

meta::server::Inode openedInode(uint64_t blocks) {
  return meta::server::Inode::newOriginFile(meta::InodeId{1},
                                            meta::Acl{},
                                            blocks * 4096,
                                            meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1),
                                            test::identity(),
                                            UtcClock::now());
}

meta::GetFileReadPlanReq requestFor(const meta::Inode &inode) {
  meta::GetFileReadPlanReq request;
  request.openSessionId = Uuid::random();
  request.inode = inode.id;
  request.offset = 0;
  request.length = inode.fileLength();
  request.cacheProtocolVersion = hf3fs::cache::kCacheProtocolVersion;
  return request;
}

meta::GetFileReadPlanRsp responseFor(const meta::GetFileReadPlanReq &request,
                                     hf3fs::cache::ImmutableObjectIdentity object) {
  meta::GetFileReadPlanRsp response;
  response.inode = request.inode;
  response.object = std::move(object);
  auto first = request.offset / 4096;
  auto count = request.length / 4096;
  response.blocks.reserve(count);
  for (uint64_t i = 0; i < count; ++i) {
    meta::ReadBlockPlan block;
    block.key = {request.inode.u64(), hf3fs::cache::CacheBlockIndex{static_cast<uint32_t>(first + i)}};
    response.blocks.push_back(std::move(block));
  }
  return response;
}

TEST(TestReadPlanBatcher, SplitsAtOneThousandBlocks) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto inode = openedInode(1001);
    auto request = requestFor(inode);
    size_t calls = 0;
    auto result =
        co_await ReadPlanBatcher::fetch(inode,
                                        request,
                                        [&](meta::GetFileReadPlanReq batch) -> CoTryTask<meta::GetFileReadPlanRsp> {
                                          ++calls;
                                          co_return responseFor(batch, test::identity());
                                        });
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(calls, size_t{2});
    CO_ASSERT_EQ(result->blocks.size(), size_t{1001});
    CO_ASSERT_EQ(result->blocks.front().key.block, hf3fs::cache::CacheBlockIndex{0});
    CO_ASSERT_EQ(result->blocks.back().key.block, hf3fs::cache::CacheBlockIndex{1000});
  }());
}

TEST(TestReadPlanBatcher, RestartsAllBatchesWhenIdentityChanges) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto inode = openedInode(1001);
    auto request = requestFor(inode);
    size_t calls = 0;
    auto result = co_await ReadPlanBatcher::fetch(
        inode,
        request,
        [&](meta::GetFileReadPlanReq batch) -> CoTryTask<meta::GetFileReadPlanRsp> {
          ++calls;
          auto object = calls == 2 ? test::identity("changed", "key") : test::identity();
          co_return responseFor(batch, std::move(object));
        });
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(calls, size_t{4});
    CO_ASSERT_EQ(result->object, test::identity());
    CO_ASSERT_EQ(result->blocks.size(), size_t{1001});
  }());
}

}  // namespace
}  // namespace hf3fs::client::cache
