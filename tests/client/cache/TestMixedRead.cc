#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "client/cache/CacheReadPipeline.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"
#include "tests/client/cache/TestOriginHelpers.h"

namespace hf3fs::client::cache {
namespace {

class MixedPlanSource final : public IReadPlanSource {
 public:
  MixedPlanSource(meta::Inode inode, const std::vector<uint8_t> &data, bool allReady)
      : inode(std::move(inode)),
        data(data),
        allReady(allReady) {}

  CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) final {
    meta::GetFileReadPlanRsp response;
    response.inode = inode.id;
    response.object = inode.asOriginFile().object;
    constexpr uint64_t blockSize = 4;
    auto end = std::min(inode.fileLength(), request.offset + request.length);
    for (auto index = request.offset / blockSize; index * blockSize < end; ++index) {
      auto begin = index * blockSize;
      auto length = std::min(blockSize, inode.fileLength() - begin);
      meta::ReadBlockPlan block;
      block.key = {inode.id.u64(), hf3fs::cache::CacheBlockIndex{static_cast<uint32_t>(index)}};
      block.fileRange = {begin, length};
      block.originRange = block.fileRange;
      block.state =
          allReady || index != 1 ? hf3fs::cache::CacheBlockState::READY : hf3fs::cache::CacheBlockState::LOADING;
      block.chunkId = meta::ChunkId(inode.id, 0, static_cast<uint32_t>(index));
      block.chainId = flat::ChainId{1};
      block.actualBlockLength = length;
      if (block.state == hf3fs::cache::CacheBlockState::READY) {
        auto checksum = storage::ChecksumInfo::create(storage::ChecksumType::CRC32C, data.data() + begin, length);
        block.loadEpoch = 1;
        block.ready = hf3fs::cache::ReadyIdentity{1,
                                                  hf3fs::cache::CacheGeneration{2},
                                                  static_cast<uint8_t>(checksum.type),
                                                  checksum.value,
                                                  length};
      }
      response.blocks.push_back(std::move(block));
    }
    co_return response;
  }

  meta::Inode inode;
  const std::vector<uint8_t> &data;
  bool allReady;
};

class MixedHitReader final : public ICacheHitReader {
 public:
  enum class Failure { NONE, NOT_FOUND, GENERATION, CHECKSUM };

  explicit MixedHitReader(const std::vector<uint8_t> &data)
      : data(data) {}

  CoTryTask<std::vector<uint8_t>> readFullBlock(const meta::ReadBlockPlan &plan, const flat::UserInfo &) final {
    ++calls;
    if (failure != Failure::NONE && plan.key.block == hf3fs::cache::CacheBlockIndex{2}) {
      if (failure == Failure::GENERATION) co_return makeError(CacheCode::kStaleGeneration);
      if (failure == Failure::CHECKSUM) co_return makeError(StorageClientCode::kChecksumMismatch);
      co_return makeError(StorageClientCode::kChunkNotFound);
    }
    co_return std::vector<uint8_t>(data.begin() + plan.fileRange.offset,
                                   data.begin() + plan.fileRange.offset + plan.fileRange.length);
  }

  const std::vector<uint8_t> &data;
  Failure failure{Failure::NONE};
  size_t calls{0};
};

class MixedReporter final : public IEnsureCachedReporter {
 public:
  CoTryTask<void> ensure(meta::InodeId, hf3fs::cache::CacheBlockIndex block) final {
    ensured.push_back(block);
    if (down) co_return makeError(CacheCode::kUnavailable);
    co_return Void{};
  }
  CoTryTask<void> report(const flat::UserInfo &,
                         const meta::SessionInfo &,
                         const meta::ReadBlockPlan &plan,
                         cache_manager::InvalidReason reason) final {
    reported.emplace_back(plan.key.block, reason);
    if (down) co_return makeError(CacheCode::kUnavailable);
    co_return Void{};
  }

  std::vector<hf3fs::cache::CacheBlockIndex> ensured;
  std::vector<std::pair<hf3fs::cache::CacheBlockIndex, cache_manager::InvalidReason>> reported;
  bool down{false};
};

meta::Inode originInode(const std::vector<uint8_t> &data) {
  return meta::server::Inode::newOriginFile(meta::InodeId{1},
                                            meta::Acl{},
                                            data.size(),
                                            meta::Layout::newEmpty(flat::ChainTableId{1}, 4, 1),
                                            test::identity(),
                                            UtcClock::now());
}

TEST(TestMixedRead, ReadsHitsMissesAndFallsBackOnlyFailedHit) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto data = test::sequence(12);
    test::FakeObjectStore store(data);
    LocalMissSingleflight singleflight;
    OriginMissReader missReader(store, singleflight, {.maxRangeBytes = 4, .maxInflightBytes = 8});
    auto source = std::make_shared<MixedPlanSource>(originInode(data), data, false);
    ReadPlanner planner(source);
    MixedHitReader hitReader(data);
    hitReader.failure = MixedHitReader::Failure::CHECKSUM;
    MixedReporter reporter;
    CacheReadPipeline pipeline(missReader, planner, hitReader, &reporter);
    std::vector<uint8_t> output(10);
    auto result = co_await pipeline.read(flat::UserInfo{},
                                         source->inode,
                                         meta::SessionInfo{ClientId::random(), Uuid::random()},
                                         1,
                                         output);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(*result, output.size());
    CO_ASSERT_EQ(output, (std::vector<uint8_t>(data.begin() + 1, data.begin() + 11)));
    CO_ASSERT_EQ(hitReader.calls, size_t{2});
    CO_ASSERT_EQ(store.calls.load(), size_t{2});
    CO_ASSERT_EQ(reporter.ensured.size(), size_t{2});
    CO_ASSERT_EQ(reporter.reported.size(), size_t{1});
    CO_ASSERT_EQ(reporter.reported[0].second, cache_manager::InvalidReason::CHECKSUM_MISMATCH);
  }());
}

TEST(TestMixedRead, CacheManagerFailureDoesNotAffectColdRead) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto data = test::sequence(12);
    test::FakeObjectStore store(data);
    LocalMissSingleflight singleflight;
    OriginMissReader missReader(store, singleflight);
    auto source = std::make_shared<MixedPlanSource>(originInode(data), data, false);
    ReadPlanner planner(source);
    MixedHitReader hitReader(data);
    MixedReporter reporter;
    reporter.down = true;
    CacheReadPipeline pipeline(missReader, planner, hitReader, &reporter);
    std::vector<uint8_t> output(data.size());
    auto result = co_await pipeline.read(flat::UserInfo{},
                                         source->inode,
                                         meta::SessionInfo{ClientId::random(), Uuid::random()},
                                         0,
                                         output);
    CO_ASSERT_OK(result);
    CO_ASSERT_EQ(output, data);
    CO_ASSERT_EQ(reporter.ensured.size(), size_t{1});
  }());
}

TEST(TestMixedRead, MapsNotFoundAndGenerationFailures) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto data = test::sequence(12);
    test::FakeObjectStore store(data);
    LocalMissSingleflight singleflight;
    OriginMissReader missReader(store, singleflight);
    auto source = std::make_shared<MixedPlanSource>(originInode(data), data, true);
    ReadPlanner planner(source);
    MixedHitReader hitReader(data);
    MixedReporter reporter;
    CacheReadPipeline pipeline(missReader, planner, hitReader, &reporter);
    std::vector<uint8_t> output(data.size());
    auto session = meta::SessionInfo{ClientId::random(), Uuid::random()};

    hitReader.failure = MixedHitReader::Failure::NOT_FOUND;
    CO_ASSERT_OK(co_await pipeline.read(flat::UserInfo{}, source->inode, session, 0, output));
    CO_ASSERT_EQ(reporter.reported.back().second, cache_manager::InvalidReason::NOT_FOUND);

    hitReader.failure = MixedHitReader::Failure::GENERATION;
    CO_ASSERT_OK(co_await pipeline.read(flat::UserInfo{}, source->inode, session, 0, output));
    CO_ASSERT_EQ(reporter.reported.back().second, cache_manager::InvalidReason::GENERATION_MISMATCH);
  }());
}

TEST(TestMixedRead, AllHitDoesNotAccessOriginOrManager) {
  folly::coro::blockingWait([]() -> CoTask<void> {
    auto data = test::sequence(12);
    test::FakeObjectStore store(data);
    LocalMissSingleflight singleflight;
    OriginMissReader missReader(store, singleflight);
    auto source = std::make_shared<MixedPlanSource>(originInode(data), data, true);
    ReadPlanner planner(source);
    MixedHitReader hitReader(data);
    MixedReporter reporter;
    CacheReadPipeline pipeline(missReader, planner, hitReader, &reporter);
    std::vector<uint8_t> output(data.size());
    CO_ASSERT_OK(co_await pipeline.read(flat::UserInfo{},
                                        source->inode,
                                        meta::SessionInfo{ClientId::random(), Uuid::random()},
                                        0,
                                        output));
    CO_ASSERT_EQ(output, data);
    CO_ASSERT_EQ(store.calls.load(), size_t{0});
    CO_ASSERT_TRUE(reporter.ensured.empty());
    CO_ASSERT_TRUE(reporter.reported.empty());
  }());
}

}  // namespace
}  // namespace hf3fs::client::cache
