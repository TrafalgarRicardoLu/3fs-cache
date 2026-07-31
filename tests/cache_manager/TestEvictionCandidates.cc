#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <optional>

#include "cache_manager/eviction/EvictionCandidateSource.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

storage::PhysicalDiskId disk(uint64_t id) { return storage::PhysicalDiskId{Uuid::from(0, id)}; }

storage::CacheSpaceInfo space(storage::PhysicalDiskId diskId, std::vector<flat::TargetId> targets) {
  storage::CacheSpaceInfo result;
  result.physicalDiskId = diskId;
  result.role = storage::StorageRole::CACHE_ONLY;
  result.targets = std::move(targets);
  result.capacityBytes = 1000;
  result.physicalUsedBytes = 500;
  result.allocatableBytes = 400;
  result.enforcedHighWatermark = 0.9;
  return result;
}

void observe(PhysicalTopology &topology, flat::NodeId node, std::initializer_list<storage::CacheSpaceInfo> spaces) {
  storage::QueryCacheSpaceRsp response;
  for (const auto &item : spaces) response.results.emplace_back(item);
  ASSERT_OK(topology.updateSpace(node, response, SteadyTime{}, SteadyTime{}));
}

meta::ReadyCacheBlockStatus ready(uint64_t inode,
                                  uint32_t block,
                                  std::vector<flat::TargetId> targets,
                                  std::vector<uint64_t> footprints) {
  std::sort(targets.begin(), targets.end());
  auto placement = *storage::PlacementIdentity::create({flat::ChainId{1}, flat::ChainVersion{7}},
                                                       targets,
                                                       targets.front(),
                                                       Uuid::from(8, inode + block));
  storage::FootprintByTarget footprintByTarget;
  for (size_t i = 0; i < targets.size(); ++i) footprintByTarget.emplace(targets[i], footprints.at(i));
  storage::PermitIdentity permit{Uuid::from(7, 1), placement, 1, std::move(footprintByTarget)};
  return {{inode, cache::CacheBlockIndex{block}},
          {1, cache::CacheGeneration{1}, 1, 1234, 4096},
          flat::ChainId{1},
          4096,
          cache::ChargeKind::COMMITTED,
          4096,
          std::move(placement),
          std::move(permit),
          UtcTime::fromMicroseconds(100),
          UtcTime::fromMicroseconds(200)};
}

meta::ListReadyCacheBlocksRsp makePage(std::vector<meta::ReadyCacheBlockStatus> items, bool more) {
  meta::ListReadyCacheBlocksRsp response;
  response.items = std::move(items);
  response.more = more;
  return response;
}

class CandidateBackend : public CacheManagerBackend {
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
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<meta::ListReadyCacheBlocksRsp> listReadyCacheBlocks(std::optional<cache::CacheBlockKey> after,
                                                                uint32_t limit) final {
    calls.push_back({after, limit});
    if (pages.empty()) co_return makeError(CacheCode::kInvalidResponse, "unexpected candidate page");
    auto page = std::move(pages.front());
    pages.erase(pages.begin());
    co_return page;
  }

  struct Call {
    std::optional<cache::CacheBlockKey> after;
    uint32_t limit;
  };
  std::vector<meta::ListReadyCacheBlocksRsp> pages;
  std::vector<Call> calls;
};

TEST(TestEvictionCandidates, UsesPersistedPlacementAcrossRouteChange) {
  auto first = disk(1);
  auto second = disk(2);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{1}, {space(first, {flat::TargetId{1}})});
  observe(topology, flat::NodeId{2}, {space(second, {flat::TargetId{2}})});

  auto changed = std::make_shared<flat::RoutingInfo>();
  flat::ChainInfo chain;
  chain.chainId = flat::ChainId{1};
  chain.chainVersion = flat::ChainVersion{9};
  flat::ChainTargetInfo current;
  current.targetId = flat::TargetId{3};
  current.publicState = flat::PublicTargetState::SERVING;
  chain.targets.push_back(current);
  changed->chains.emplace(chain.chainId, chain);
  topology.updateRouting(std::make_shared<client::RoutingInfo>(std::move(changed), SteadyTime{}));

  auto backend = std::make_shared<CandidateBackend>();
  backend->pages.push_back(makePage({ready(7, 0, {flat::TargetId{1}, flat::TargetId{2}}, {64, 128})}, false));
  EvictionCandidateSource source(backend, topology, 16);
  auto page = folly::coro::blockingWait(source.next(std::nullopt, {first}));
  ASSERT_OK(page);
  ASSERT_EQ(page->candidates.size(), size_t{1});
  EXPECT_EQ(page->candidates.front().physicalFootprintByDisk.at(first), 64);
  EXPECT_EQ(page->candidates.front().physicalFootprintByDisk.at(second), 128);
}

TEST(TestEvictionCandidates, AggregatesTwoReplicasOnOneDisk) {
  auto shared = disk(1);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{1}, {space(shared, {flat::TargetId{1}, flat::TargetId{2}})});
  auto backend = std::make_shared<CandidateBackend>();
  backend->pages.push_back(makePage({ready(7, 0, {flat::TargetId{1}, flat::TargetId{2}}, {64, 128})}, false));
  EvictionCandidateSource source(backend, topology, 16);
  auto page = folly::coro::blockingWait(source.next(std::nullopt, {shared}));
  ASSERT_OK(page);
  ASSERT_EQ(page->candidates.front().physicalFootprintByDisk.size(), size_t{1});
  EXPECT_EQ(page->candidates.front().physicalFootprintByDisk.at(shared), 192);
}

TEST(TestEvictionCandidates, UnknownOriginalDiskFailsClosed) {
  auto known = disk(1);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{1}, {space(known, {flat::TargetId{1}})});
  auto backend = std::make_shared<CandidateBackend>();
  backend->pages.push_back(makePage({ready(7, 0, {flat::TargetId{1}, flat::TargetId{9}}, {64, 128})}, false));
  EvictionCandidateSource source(backend, topology, 16);
  ASSERT_ERROR(folly::coro::blockingWait(source.next(std::nullopt, {known})), CacheCode::kUnavailable);
}

TEST(TestEvictionCandidates, FiltersUnpressuredCandidatesAndAdvancesPages) {
  auto pressured = disk(1);
  auto free = disk(2);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{1}, {space(pressured, {flat::TargetId{1}})});
  observe(topology, flat::NodeId{2}, {space(free, {flat::TargetId{2}})});
  auto backend = std::make_shared<CandidateBackend>();
  backend->pages.push_back(
      makePage({ready(7, 0, {flat::TargetId{1}}, {64}), ready(7, 1, {flat::TargetId{2}}, {128})}, true));
  backend->pages.push_back(makePage({ready(8, 0, {flat::TargetId{1}, flat::TargetId{2}}, {32, 48})}, false));
  EvictionCandidateSource source(backend, topology, 2);

  auto first = folly::coro::blockingWait(source.next(std::nullopt, {pressured}));
  ASSERT_OK(first);
  ASSERT_EQ(first->candidates.size(), size_t{1});
  ASSERT_TRUE(first->more);
  auto expectedCursor = std::optional<cache::CacheBlockKey>{{7, cache::CacheBlockIndex{1}}};
  ASSERT_EQ(first->nextAfter, expectedCursor);

  auto second = folly::coro::blockingWait(source.next(first->nextAfter, {pressured}));
  ASSERT_OK(second);
  ASSERT_EQ(second->candidates.size(), size_t{1});
  ASSERT_FALSE(second->more);
  ASSERT_EQ(backend->calls[1].after, first->nextAfter);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
