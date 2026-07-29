#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/service/EnsureCached.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

class EnsureBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final { co_return inode; }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase> items) final {
    seen = std::move(items);
    meta::EnqueueCacheBlocksRsp response;
    for (size_t i = 0; i < seen.size(); ++i) {
      if (capacityReject) {
        response.results.emplace_back(makeError(CacheCode::kCapacityExceeded));
      } else {
        response.results.emplace_back(meta::CacheBlockMutationResult{seen[i].key, state});
      }
    }
    co_return response;
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
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }

  meta::Inode inode = [] {
    auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                                 "bucket",
                                                 "key",
                                                 {cache::VersionSelectorType::VERSION_ID, "v1"}};
    return meta::Inode{
        meta::InodeId{9},
        meta::InodeData{
            meta::OriginFile{8193, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), std::move(object)}}};
  }();
  cache::CacheBlockState state{cache::CacheBlockState::QUEUED};
  bool capacityReject{false};
  std::vector<meta::CacheBlockRequestBase> seen;
};

EnsureCachedReq request() {
  EnsureCachedReq req;
  req.service = {"cache-manager", "token"};
  req.inode = meta::InodeId{9};
  req.beginBlock = cache::CacheBlockIndex{0};
  req.blockCount = 3;
  req.priority = 7;
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return req;
}

TEST(TestEnsureCached, EnqueuesExactBlocksAndDeduplicatesRestartedQueue) {
  auto backend = std::make_shared<EnsureBackend>();
  HintCoalescer hints;
  EnsureCached ensure(backend, hints);
  auto first = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(first);
  ASSERT_EQ(first->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_EQ(backend->seen.size(), size_t{3});
  ASSERT_EQ(backend->seen[2].blockLength, uint64_t{1});
  ASSERT_EQ(hints.size(), size_t{3});

  auto repeated = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(repeated);
  ASSERT_EQ(repeated->status, EnsureCachedStatus::ATTACHED);
  ASSERT_EQ(hints.size(), size_t{3});
}

TEST(TestEnsureCached, AttachesExistingStatesAndBypassesCapacity) {
  auto backend = std::make_shared<EnsureBackend>();
  HintCoalescer hints;
  EnsureCached ensure(backend, hints);
  backend->state = cache::CacheBlockState::READY;
  auto ready = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(ready);
  ASSERT_EQ(ready->status, EnsureCachedStatus::ATTACHED);
  ASSERT_EQ(hints.size(), size_t{0});

  backend->capacityReject = true;
  auto bypassed = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(bypassed);
  ASSERT_EQ(bypassed->status, EnsureCachedStatus::BYPASSED);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
