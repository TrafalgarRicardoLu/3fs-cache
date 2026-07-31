#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/eviction/EvictingWorker.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

meta::CacheEvictionIdentity identity(uint64_t inode) {
  auto placement = *storage::PlacementIdentity::create({flat::ChainId{1}, flat::ChainVersion{1}},
                                                       {flat::TargetId{1}},
                                                       flat::TargetId{1},
                                                       Uuid::from(2, inode));
  meta::CacheEvictionIdentity result;
  result.key = {inode, cache::CacheBlockIndex{0}};
  result.ready = {1, cache::CacheGeneration{1}, 1, static_cast<uint32_t>(inode), 4096};
  result.placement = placement;
  result.evictionEpoch = cache::EvictionEpoch{1};
  result.retireOperationId = Uuid::from(3, inode);
  result.reason = cache::EvictionReason::CAPACITY_WATERMARK;
  return result;
}

class Backend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final { co_return makeError(StatusCode::kNotImplemented); }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &,
                                                       cache::CacheBlockIndex,
                                                       const meta::CacheBlockLease &,
                                                       std::vector<uint8_t>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &,
                         const meta::CacheBlockLease &,
                         const storage::CacheChunkGenerationInfo &) final {
    co_return Void{};
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final { co_return Void{}; }
  CoTryTask<meta::ListEvictingCacheBlocksRsp> listEvicting(std::optional<cache::CacheBlockKey>, uint32_t) final {
    meta::ListEvictingCacheBlocksRsp response;
    if (!listed) {
      response.items = items;
      listed = true;
    }
    co_return response;
  }
  CoTryTask<bool> coordinateRetire(const meta::CacheEvictionIdentity &item) final {
    operations.push_back(item.retireOperationId);
    if (failFirst && operations.size() == 1) co_return makeError(CacheCode::kUnavailable);
    co_return true;
  }

  bool listed{false};
  bool failFirst{false};
  std::vector<meta::CacheEvictionIdentity> items;
  std::vector<Uuid> operations;
};

TEST(TestEvictingWorker, ContinuesPastFailureAndReplaysAfterRestart) {
  auto backend = std::make_shared<Backend>();
  backend->items = {identity(1), identity(2)};
  backend->failFirst = true;
  EvictingWorker worker(backend, 100);
  auto first = folly::coro::blockingWait(worker.runOnce());
  ASSERT_OK(first);
  EXPECT_EQ(first->failed, 1);
  EXPECT_EQ(first->completed, 1);

  backend->listed = false;
  backend->failFirst = false;
  auto replay = folly::coro::blockingWait(worker.runOnce());
  ASSERT_OK(replay);
  EXPECT_EQ(replay->completed, 2);
  EXPECT_EQ(backend->operations.size(), 4);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
