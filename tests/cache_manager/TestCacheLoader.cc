#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/loader/CacheLoader.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

meta::Inode originInode(bool superseded = false) {
  auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                               "bucket",
                                               "object",
                                               {cache::VersionSelectorType::VERSION_ID, "version-1"}};
  meta::OriginFile origin{3 * 4096, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), std::move(object)};
  origin.superseded = superseded;
  return meta::Inode{meta::InodeId{7}, meta::InodeData{std::move(origin)}};
}

storage::PermitIdentity phase2Permit() {
  storage::PlacementIdentity placement{{flat::ChainId{1}, flat::ChainVersion{1}},
                                       {flat::TargetId{1}},
                                       flat::TargetId{1},
                                       Uuid::from(2, 1)};
  return {Uuid::from(3, 1), placement, 1, {{flat::TargetId{1}, 4096}}};
}

class MockBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final {
    events.push_back("stat");
    ++statCalls;
    co_return statCalls > 1 && changeAfterWrite ? originInode(true) : inode;
  }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &) final {
    events.push_back("acquire");
    co_return lease;
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange range) final {
    events.push_back("origin");
    ranges.push_back(range);
    if (originError.has_value()) co_return makeError(*originError);
    co_return std::vector<uint8_t>(range.length, uint8_t{0x5a});
  }
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &,
                                                       cache::CacheBlockIndex,
                                                       const meta::CacheBlockLease &activeLease,
                                                       std::vector<uint8_t> data) final {
    events.push_back("replace");
    ++replaceCalls;
    if (replaceError.has_value() || replaceCalls == failReplaceCall) {
      co_return makeError(replaceError.value_or(Status(CacheCode::kUnavailable, "storage failed")));
    }
    auto checksum = storage::ChecksumInfo::create(storage::ChecksumType::CRC32C, data.data(), data.size());
    co_return storage::CacheChunkGenerationInfo{activeLease.cacheGeneration, false, data.size(), checksum};
  }
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &,
                         const meta::CacheBlockLease &activeLease,
                         const storage::CacheChunkGenerationInfo &) final {
    events.push_back("commit");
    if (commitError.has_value()) co_return makeError(*commitError);
    committedPermit = activeLease.permit;
    ++commits;
    co_return Void{};
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    events.push_back("fail");
    ++fails;
    co_return Void{};
  }
  CoTryTask<storage::CachePermitResult> renewPermit(const storage::PermitIdentity &permit, uint64_t expiresAtNs) final {
    events.push_back("renew");
    renewed.push_back(permit);
    if (renewError) co_return makeError(*renewError);
    co_return storage::CachePermitResult{permit, renewedState, expiresAtNs};
  }
  CoTryTask<void> releasePermit(const storage::PermitIdentity &permit) final {
    events.push_back("release");
    released.push_back(permit);
    co_return Void{};
  }

  meta::Inode inode = originInode();
  meta::CacheBlockLease lease{Uuid::random(), 1, cache::CacheGeneration{1}};
  std::optional<Status> originError;
  std::optional<Status> replaceError;
  std::optional<Status> commitError;
  std::optional<Status> renewError;
  std::optional<storage::PermitIdentity> committedPermit;
  cache::CachePermitState renewedState{cache::CachePermitState::RESERVED};
  bool changeAfterWrite{false};
  int statCalls{0};
  int commits{0};
  int fails{0};
  int replaceCalls{0};
  int failReplaceCall{-1};
  std::vector<std::string> events;
  std::vector<cache::ByteRange> ranges;
  std::vector<storage::PermitIdentity> renewed;
  std::vector<storage::PermitIdentity> released;
};

TEST(TestCacheLoader, MergesAndSplitsContiguousRanges) {
  std::vector hints{LoadHint{meta::InodeId{7}, cache::CacheBlockIndex{2}, 1024},
                    LoadHint{meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096},
                    LoadHint{meta::InodeId{7}, cache::CacheBlockIndex{1}, 4096}};
  auto ranges = CacheLoader::mergeRanges(hints, 4096);
  ASSERT_OK(ranges);
  ASSERT_EQ(ranges->size(), size_t{1});
  ASSERT_EQ((*ranges)[0], (cache::ByteRange{0, 9216}));
}

TEST(TestCacheLoader, LoadsOriginBeforeStorageAndCommits) {
  auto backend = std::make_shared<MockBackend>();
  CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
  CacheLoader loader(backend, gate);
  auto result = folly::coro::blockingWait(
      loader.load({meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1}));
  ASSERT_OK(result);
  ASSERT_EQ(backend->commits, 1);
  ASSERT_EQ(backend->fails, 0);
  ASSERT_EQ(backend->events, (std::vector<std::string>{"stat", "acquire", "origin", "replace", "stat", "commit"}));
}

TEST(TestCacheLoader, FetchesContiguousBlocksInOneOriginRange) {
  auto backend = std::make_shared<MockBackend>();
  CapacityGate gate({2, 8192}, {{cache::OriginId{1}, {2, 8192}}});
  CacheLoader loader(backend, gate);
  auto result = folly::coro::blockingWait(
      loader.loadBatch({{meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1},
                        {meta::InodeId{7}, cache::CacheBlockIndex{1}, 4096, EnsureReason::FOREGROUND_MISS, 1}}));
  ASSERT_OK(result);
  ASSERT_EQ(backend->ranges, (std::vector<cache::ByteRange>{{0, 8192}}));
  ASSERT_EQ(backend->commits, 2);
  ASSERT_EQ(backend->fails, 0);
  ASSERT_EQ(
      backend->events,
      (std::vector<
          std::string>{"stat", "acquire", "acquire", "origin", "replace", "replace", "stat", "commit", "commit"}));
}

TEST(TestCacheLoader, SplitsRangeAtInflightByteLimit) {
  auto backend = std::make_shared<MockBackend>();
  CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
  CacheLoader loader(backend, gate);
  auto result = folly::coro::blockingWait(
      loader.loadBatch({{meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1},
                        {meta::InodeId{7}, cache::CacheBlockIndex{1}, 4096, EnsureReason::FOREGROUND_MISS, 1}}));
  ASSERT_OK(result);
  ASSERT_EQ(backend->ranges, (std::vector<cache::ByteRange>{{0, 4096}, {4096, 4096}}));
  ASSERT_EQ(backend->commits, 2);
}

TEST(TestCacheLoader, FailsFenceOnOriginWriteAndCommitErrors) {
  for (auto failure : {0, 1, 2}) {
    auto backend = std::make_shared<MockBackend>();
    if (failure == 0) backend->originError = Status(CacheCode::kVersionMismatch, "changed");
    if (failure == 1) backend->replaceError = Status(CacheCode::kUnavailable, "storage failed");
    if (failure == 2) backend->commitError = Status(CacheCode::kTimeout, "commit timed out");
    CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
    CacheLoader loader(backend, gate);
    auto result = folly::coro::blockingWait(
        loader.load({meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1}));
    ASSERT_TRUE(result.hasError());
    ASSERT_EQ(backend->fails, 1);
    ASSERT_EQ(backend->commits, 0);
  }
}

TEST(TestCacheLoader, CommitsSuccessfulBlocksAfterPartialStorageFailure) {
  auto backend = std::make_shared<MockBackend>();
  backend->failReplaceCall = 2;
  CapacityGate gate({2, 8192}, {{cache::OriginId{1}, {2, 8192}}});
  CacheLoader loader(backend, gate);
  auto result = folly::coro::blockingWait(
      loader.loadBatch({{meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1},
                        {meta::InodeId{7}, cache::CacheBlockIndex{1}, 4096, EnsureReason::FOREGROUND_MISS, 1}}));
  ASSERT_ERROR(result, CacheCode::kUnavailable);
  ASSERT_EQ(backend->commits, 1);
  ASSERT_EQ(backend->fails, 1);
}

TEST(TestCacheLoader, RejectsSupersededInodeAfterStorageWrite) {
  auto backend = std::make_shared<MockBackend>();
  backend->changeAfterWrite = true;
  CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
  CacheLoader loader(backend, gate);
  auto result = folly::coro::blockingWait(
      loader.load({meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1}));
  ASSERT_ERROR(result, CacheCode::kVersionMismatch);
  ASSERT_EQ(backend->fails, 1);
  ASSERT_EQ(backend->commits, 0);
}

TEST(TestCacheLoader, RenewsAndThreadsPhase2PermitThroughReplaceAndCommit) {
  auto backend = std::make_shared<MockBackend>();
  backend->lease.permit = phase2Permit();
  CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
  CacheLoader loader(backend, gate, 1_s, [] { return uint64_t{1000}; });
  auto result = folly::coro::blockingWait(
      loader.load({meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1}));
  ASSERT_OK(result);
  ASSERT_EQ(backend->renewed, std::vector<storage::PermitIdentity>{phase2Permit()});
  ASSERT_EQ(backend->committedPermit, backend->lease.permit);
  ASSERT_TRUE(backend->released.empty());
  ASSERT_EQ(backend->events,
            (std::vector<std::string>{"stat", "acquire", "origin", "renew", "replace", "stat", "commit"}));
}

TEST(TestCacheLoader, ExpiredPhase2PermitFailsBeforeReplaceAndIsReleased) {
  auto backend = std::make_shared<MockBackend>();
  backend->lease.permit = phase2Permit();
  backend->renewError = Status(CacheCode::kPermitExpired, "expired");
  CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
  CacheLoader loader(backend, gate, 1_s, [] { return uint64_t{1000}; });
  auto result = folly::coro::blockingWait(
      loader.load({meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1}));
  ASSERT_ERROR(result, CacheCode::kPermitExpired);
  ASSERT_EQ(backend->replaceCalls, 0);
  ASSERT_EQ(backend->fails, 1);
  ASSERT_EQ(backend->released, std::vector<storage::PermitIdentity>{phase2Permit()});
}

TEST(TestCacheLoader, CommitFailureReleasesPhase2Permit) {
  auto backend = std::make_shared<MockBackend>();
  backend->lease.permit = phase2Permit();
  backend->commitError = Status(RPCCode::kTimeout, "commit timeout");
  CapacityGate gate({1, 4096}, {{cache::OriginId{1}, {1, 4096}}});
  CacheLoader loader(backend, gate, 1_s, [] { return uint64_t{1000}; });
  auto result = folly::coro::blockingWait(
      loader.load({meta::InodeId{7}, cache::CacheBlockIndex{0}, 4096, EnsureReason::FOREGROUND_MISS, 1}));
  ASSERT_ERROR(result, RPCCode::kTimeout);
  ASSERT_EQ(backend->replaceCalls, 1);
  ASSERT_EQ(backend->commits, 0);
  ASSERT_EQ(backend->fails, 1);
  ASSERT_EQ(backend->released, std::vector<storage::PermitIdentity>{phase2Permit()});
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
