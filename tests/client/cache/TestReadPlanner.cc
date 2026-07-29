#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "client/cache/ReadPlanner.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"
#include "tests/client/cache/TestOriginHelpers.h"

namespace hf3fs::client::cache {
namespace {

class PlanSource : public IReadPlanSource {
 public:
  PlanSource(meta::Inode inode, std::vector<uint8_t> data)
      : inode(std::move(inode)),
        data(std::move(data)) {}

  CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) override {
    ++calls;
    meta::GetFileReadPlanRsp response;
    response.inode = inode.id;
    response.object = inode.asOriginFile().object;
    auto blockSize = uint64_t{inode.fileLayout().chunkSize};
    auto end = std::min(inode.fileLength(), request.offset + request.length);
    for (auto blockIndex = request.offset / blockSize; blockIndex * blockSize < end; ++blockIndex) {
      auto begin = blockIndex * blockSize;
      auto length = std::min(blockSize, inode.fileLength() - begin);
      meta::ReadBlockPlan block;
      block.key = {inode.id.u64(), hf3fs::cache::CacheBlockIndex{static_cast<uint32_t>(blockIndex)}};
      block.fileRange = {begin, length};
      block.originRange = block.fileRange;
      block.state = blockIndex == 1 ? hf3fs::cache::CacheBlockState::READY : hf3fs::cache::CacheBlockState::NONE;
      block.chunkId = meta::ChunkId(inode.id, 0, static_cast<uint32_t>(blockIndex));
      block.chainId = flat::ChainId{1};
      block.actualBlockLength = length;
      if (block.state == hf3fs::cache::CacheBlockState::READY) {
        auto checksum = storage::ChecksumInfo::create(storage::ChecksumType::CRC32C, data.data() + begin, length);
        block.loadEpoch = 7;
        block.ready = hf3fs::cache::ReadyIdentity{7,
                                                  hf3fs::cache::CacheGeneration{3},
                                                  static_cast<uint8_t>(checksum.type),
                                                  checksum.value,
                                                  length};
      }
      response.blocks.push_back(std::move(block));
    }
    co_return response;
  }

  meta::Inode inode;
  std::vector<uint8_t> data;
  size_t calls{0};
};

meta::Inode originInode(const std::vector<uint8_t> &data) {
  return meta::server::Inode::newOriginFile(meta::InodeId{1},
                                            meta::Acl{},
                                            data.size(),
                                            meta::Layout::newEmpty(flat::ChainTableId{1}, 4, 1),
                                            test::identity(),
                                            UtcClock::now());
}

TEST(TestReadPlanner, ClassifiesAndValidatesUnalignedPlan) {
  auto data = test::sequence(12);
  auto source = std::make_shared<PlanSource>(originInode(data), data);
  ReadPlanner planner(source);
  meta::SessionInfo session{ClientId::random(), Uuid::random()};
  auto result = folly::coro::blockingWait(planner.plan(flat::UserInfo{}, source->inode, session, 1, 10));
  ASSERT_OK(result);
  ASSERT_EQ(result->blocks.size(), size_t{3});
  ASSERT_EQ(result->blocks[1].state, hf3fs::cache::CacheBlockState::READY);
  ASSERT_EQ(result->blocks[0].state, hf3fs::cache::CacheBlockState::NONE);
  ASSERT_EQ(source->calls, size_t{1});
}

TEST(TestReadPlanner, RejectsPlanGap) {
  class GapSource final : public PlanSource {
   public:
    using PlanSource::PlanSource;
    CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) final {
      auto response = co_await PlanSource::fetch(std::move(request));
      CO_RETURN_ON_ERROR(response);
      response->blocks.erase(response->blocks.begin() + 1);
      co_return std::move(*response);
    }
  };
  auto data = test::sequence(12);
  auto source = std::make_shared<GapSource>(originInode(data), data);
  ReadPlanner planner(source);
  auto result = folly::coro::blockingWait(
      planner.plan(flat::UserInfo{}, source->inode, {ClientId::random(), Uuid::random()}, 0, data.size()));
  ASSERT_ERROR(result, CacheCode::kInvalidResponse);
}

}  // namespace
}  // namespace hf3fs::client::cache
