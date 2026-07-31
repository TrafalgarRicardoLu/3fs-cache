#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/capacity/PhysicalPreflight.h"
#include "cache_manager/eviction/EvictionController.h"
#include "cache_manager/eviction/LRUEvictionPolicy.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

SteadyTime at(uint64_t seconds) { return SteadyTime{std::chrono::seconds(seconds)}; }

storage::PhysicalDiskId disk(uint64_t id) { return storage::PhysicalDiskId{Uuid::from(0, id)}; }

flat::TargetInfo target(uint64_t targetId, uint32_t nodeId, storage::PhysicalDiskId diskId) {
  flat::TargetInfo result;
  result.targetId = flat::TargetId{targetId};
  result.nodeId = flat::NodeId{nodeId};
  result.physicalDiskId = diskId;
  result.storageRole = storage::StorageRole::CACHE_ONLY;
  return result;
}

std::shared_ptr<client::RoutingInfo> routing(
    std::initializer_list<std::pair<flat::ChainId, std::vector<flat::TargetId>>> chains,
    std::initializer_list<flat::TargetInfo> targets) {
  auto raw = std::make_shared<flat::RoutingInfo>();
  for (const auto &[chainId, targetIds] : chains) {
    flat::ChainInfo chain;
    chain.chainId = chainId;
    chain.chainVersion = flat::ChainVersion{3};
    for (auto targetId : targetIds) {
      flat::ChainTargetInfo replica;
      replica.targetId = targetId;
      replica.publicState = flat::PublicTargetState::SERVING;
      chain.targets.push_back(replica);
    }
    raw->chains.emplace(chainId, std::move(chain));
  }
  for (const auto &info : targets) raw->targets.emplace(info.targetId, info);
  return std::make_shared<client::RoutingInfo>(std::move(raw), at(0));
}

storage::CacheSpaceInfo space(storage::PhysicalDiskId diskId,
                              std::vector<flat::TargetId> targets,
                              uint64_t used,
                              uint64_t reserved = 0) {
  storage::CacheSpaceInfo result;
  result.physicalDiskId = diskId;
  result.role = storage::StorageRole::CACHE_ONLY;
  result.targets = std::move(targets);
  result.capacityBytes = 1000;
  result.physicalUsedBytes = used;
  result.allocatableBytes = 1000 - std::min(used, uint64_t{1000});
  result.reservedBytes = reserved;
  result.enforcedHighWatermark = 0.9;
  return result;
}

void observe(PhysicalTopology &topology,
             flat::NodeId node,
             std::initializer_list<storage::CacheSpaceInfo> spaces,
             SteadyTime request = at(1),
             SteadyTime received = at(2)) {
  storage::QueryCacheSpaceRsp response;
  for (const auto &item : spaces) response.results.emplace_back(item);
  ASSERT_OK(topology.updateSpace(node, response, request, received));
}

meta::ReadyCacheBlockStatus ready(uint64_t inode,
                                  flat::ChainId chainId,
                                  std::vector<flat::TargetId> targets,
                                  std::vector<uint64_t> footprints,
                                  int64_t accessUs = 100) {
  std::sort(targets.begin(), targets.end());
  auto placement = *storage::PlacementIdentity::create({chainId, flat::ChainVersion{3}},
                                                       targets,
                                                       targets.front(),
                                                       Uuid::from(8, inode));
  storage::FootprintByTarget footprintByTarget;
  for (size_t i = 0; i < targets.size(); ++i) footprintByTarget.emplace(targets[i], footprints.at(i));
  storage::PermitIdentity permit{Uuid::from(7, inode), placement, 1, std::move(footprintByTarget)};
  return {{inode, cache::CacheBlockIndex{0}},
          {1, cache::CacheGeneration{1}, 1, static_cast<uint32_t>(inode), 4096},
          chainId,
          4096,
          cache::ChargeKind::COMMITTED,
          4096,
          std::move(placement),
          std::move(permit),
          UtcTime::fromMicroseconds(accessUs),
          UtcTime::fromMicroseconds(accessUs)};
}

class ControllerBackend : public CacheManagerBackend {
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
  CoTryTask<meta::ListReadyCacheBlocksRsp> listReadyCacheBlocks(std::optional<cache::CacheBlockKey>, uint32_t) final {
    if (pages.empty()) co_return meta::ListReadyCacheBlocksRsp{};
    auto page = std::move(pages.front());
    pages.erase(pages.begin());
    for (const auto &status : page.items) placements[status.key.inode] = status.placement;
    co_return page;
  }
  CoTryTask<meta::BeginEvictCacheBlocksRsp> beginEvict(std::vector<meta::BeginEvictCacheBlockItem> items) final {
    beginCalls.push_back(items);
    meta::BeginEvictCacheBlocksRsp response;
    for (const auto &item : items) {
      if (conflicts.contains(item.key.inode)) {
        response.results.emplace_back(makeError(CacheCode::kStateConflict));
        continue;
      }
      response.results.emplace_back(meta::CacheEvictionIdentity{item.key,
                                                                item.expectedReady,
                                                                placements.at(item.key.inode),
                                                                cache::EvictionEpoch{1},
                                                                Uuid::from(9, item.key.inode),
                                                                item.reason});
    }
    co_return response;
  }

  std::vector<meta::ListReadyCacheBlocksRsp> pages;
  std::map<uint64_t, storage::PlacementIdentity> placements;
  std::set<uint64_t> conflicts;
  std::vector<std::vector<meta::BeginEvictCacheBlockItem>> beginCalls;
};

meta::ListReadyCacheBlocksRsp page(std::vector<meta::ReadyCacheBlockStatus> items, bool more = false) {
  meta::ListReadyCacheBlocksRsp result;
  result.items = std::move(items);
  result.more = more;
  return result;
}

EvictionControllerConfig config() {
  EvictionControllerConfig result;
  result.snapshotMaxAge = 30_s;
  result.protectionPeriod = 0_ns;
  result.candidatePageSize = 16;
  result.batchSize = 16;
  return result;
}

class InvalidPolicy final : public EvictionPolicy {
 public:
  Result<std::vector<size_t>> select(std::span<const EvictionCandidate>, const EvictionContext &) final {
    return std::vector<size_t>{0, 0};
  }
};

TEST(TestEvictionController, AppliesHighLowHysteresisAndPausesOnlyAffectedChains) {
  auto pressured = disk(1);
  auto free = disk(2);
  PhysicalTopology topology;
  topology.updateRouting(routing({{flat::ChainId{1}, {flat::TargetId{1}}}, {flat::ChainId{2}, {flat::TargetId{2}}}},
                                 {target(1, 10, pressured), target(2, 20, free)}));
  observe(topology, flat::NodeId{10}, {space(pressured, {flat::TargetId{1}}, 900)});
  observe(topology, flat::NodeId{20}, {space(free, {flat::TargetId{2}}, 100)});
  auto backend = std::make_shared<ControllerBackend>();
  backend->pages.push_back(page({ready(1, flat::ChainId{1}, {flat::TargetId{1}}, {120})}));
  EvictionPressureState pressure;
  LRUEvictionPolicy policy;
  EvictionController controller(backend, topology, policy, pressure, config());

  auto first = folly::coro::blockingWait(controller.runOnce(at(3), UtcTime::fromMicroseconds(1000)));
  ASSERT_OK(first);
  EXPECT_EQ(first->deficits.at(pressured), 101);
  EXPECT_EQ(first->begun, size_t{1});
  EXPECT_TRUE(first->targetMet);
  EXPECT_TRUE(pressure.contains(pressured));
  EXPECT_FALSE(pressure.contains(free));

  PhysicalPreflight preflight(topology, 30_s, 0.9, &pressure);
  ASSERT_ERROR(preflight.tryReserve(flat::ChainId{1}, {{flat::TargetId{1}, 1}}, at(3)), CacheCode::kCapacityExceeded);
  ASSERT_OK(preflight.tryReserve(flat::ChainId{2}, {{flat::TargetId{2}, 1}}, at(3)));

  observe(topology, flat::NodeId{10}, {space(pressured, {flat::TargetId{1}}, 850)}, at(4), at(5));
  ASSERT_OK(folly::coro::blockingWait(controller.runOnce(at(6), UtcTime::fromMicroseconds(1100))));
  EXPECT_TRUE(pressure.contains(pressured));

  observe(topology, flat::NodeId{10}, {space(pressured, {flat::TargetId{1}}, 799)}, at(7), at(8));
  auto recovered = folly::coro::blockingWait(controller.runOnce(at(9), UtcTime::fromMicroseconds(1200)));
  ASSERT_OK(recovered);
  EXPECT_TRUE(recovered->targetMet);
  EXPECT_FALSE(pressure.contains(pressured));
  ASSERT_OK(preflight.tryReserve(flat::ChainId{1}, {{flat::TargetId{1}, 1}}, at(9)));
}

TEST(TestEvictionController, HandlesMultipleDisksAndSkipsCasConflicts) {
  auto firstDisk = disk(1);
  auto secondDisk = disk(2);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{10}, {space(firstDisk, {flat::TargetId{1}}, 920)});
  observe(topology, flat::NodeId{20}, {space(secondDisk, {flat::TargetId{2}}, 910)});
  auto first = ready(1, flat::ChainId{1}, {flat::TargetId{1}}, {130});
  auto shared = ready(2, flat::ChainId{2}, {flat::TargetId{1}, flat::TargetId{2}}, {20, 120});
  auto backend = std::make_shared<ControllerBackend>();
  backend->pages.push_back(page({first, shared}));
  backend->conflicts.insert(first.key.inode);
  EvictionPressureState pressure;
  LRUEvictionPolicy policy;
  EvictionController controller(backend, topology, policy, pressure, config());

  auto result = folly::coro::blockingWait(controller.runOnce(at(3), UtcTime::fromMicroseconds(1000)));
  ASSERT_OK(result);
  EXPECT_EQ(result->selected, size_t{2});
  EXPECT_EQ(result->begun, size_t{1});
  EXPECT_EQ(result->conflicts, size_t{1});
  EXPECT_FALSE(result->targetMet);
  ASSERT_EQ(backend->beginCalls.size(), size_t{1});
  EXPECT_EQ(backend->beginCalls.front()[0].reason, cache::EvictionReason::CAPACITY_WATERMARK);
}

TEST(TestEvictionController, RejectsInvalidPolicyBeforeMutation) {
  auto diskId = disk(1);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{10}, {space(diskId, {flat::TargetId{1}}, 900)});
  auto backend = std::make_shared<ControllerBackend>();
  backend->pages.push_back(page({ready(1, flat::ChainId{1}, {flat::TargetId{1}}, {120})}));
  EvictionPressureState pressure;
  InvalidPolicy policy;
  EvictionController controller(backend, topology, policy, pressure, config());

  ASSERT_ERROR(folly::coro::blockingWait(controller.runOnce(at(3), UtcTime::fromMicroseconds(1000))),
               StatusCode::kInvalidArg);
  EXPECT_TRUE(backend->beginCalls.empty());
  EXPECT_TRUE(pressure.contains(diskId));
}

TEST(TestEvictionController, KeepsAdmissionPausedWhenCandidatesAreInsufficientOrProtected) {
  auto diskId = disk(1);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{10}, {space(diskId, {flat::TargetId{1}}, 900)});
  auto backend = std::make_shared<ControllerBackend>();
  backend->pages.push_back(page({ready(1, flat::ChainId{1}, {flat::TargetId{1}}, {20})}));
  EvictionPressureState pressure;
  LRUEvictionPolicy policy;
  EvictionController controller(backend, topology, policy, pressure, config());

  auto insufficient = folly::coro::blockingWait(controller.runOnce(at(3), UtcTime::fromMicroseconds(1000)));
  ASSERT_OK(insufficient);
  EXPECT_FALSE(insufficient->targetMet);
  EXPECT_TRUE(pressure.contains(diskId));

  auto protectedConfig = config();
  protectedConfig.protectionPeriod = 10_min;
  auto protectedBackend = std::make_shared<ControllerBackend>();
  protectedBackend->pages.push_back(page({ready(2, flat::ChainId{1}, {flat::TargetId{1}}, {200}, 900)}));
  EvictionController protectedController(protectedBackend, topology, policy, pressure, protectedConfig);
  auto protectedResult = folly::coro::blockingWait(protectedController.runOnce(at(3), UtcTime::fromMicroseconds(1000)));
  ASSERT_OK(protectedResult);
  EXPECT_EQ(protectedResult->selected, size_t{0});
  EXPECT_TRUE(protectedBackend->beginCalls.empty());
  EXPECT_TRUE(pressure.contains(diskId));
}

TEST(TestEvictionController, ReplaysTheSameReadyFenceAfterRestart) {
  auto diskId = disk(1);
  PhysicalTopology topology;
  observe(topology, flat::NodeId{10}, {space(diskId, {flat::TargetId{1}}, 900)});
  auto status = ready(1, flat::ChainId{1}, {flat::TargetId{1}}, {120});
  auto backend = std::make_shared<ControllerBackend>();
  backend->pages.push_back(page({status}));
  backend->pages.push_back(page({status}));
  EvictionPressureState firstPressure;
  EvictionPressureState restartedPressure;
  LRUEvictionPolicy policy;

  EvictionController first(backend, topology, policy, firstPressure, config());
  ASSERT_OK(folly::coro::blockingWait(first.runOnce(at(3), UtcTime::fromMicroseconds(1000))));
  EvictionController restarted(backend, topology, policy, restartedPressure, config());
  ASSERT_OK(folly::coro::blockingWait(restarted.runOnce(at(3), UtcTime::fromMicroseconds(1000))));

  ASSERT_EQ(backend->beginCalls.size(), size_t{2});
  ASSERT_EQ(backend->beginCalls[0].size(), size_t{1});
  ASSERT_EQ(backend->beginCalls[1].size(), size_t{1});
  EXPECT_EQ(backend->beginCalls[0][0].key, backend->beginCalls[1][0].key);
  EXPECT_EQ(backend->beginCalls[0][0].expectedReady, backend->beginCalls[1][0].expectedReady);
  EXPECT_TRUE(restartedPressure.contains(diskId));
}

TEST(TestEvictionController, ValidatesWatermarksAndProtocolBatchLimits) {
  auto invalid = config();
  invalid.lowWatermark = invalid.highWatermark;
  ASSERT_ERROR(invalid.valid(), StatusCode::kInvalidConfig);
  invalid = config();
  invalid.batchSize = cache::kMaxPhase2BatchItems + 1;
  ASSERT_ERROR(invalid.valid(), StatusCode::kInvalidConfig);

  Config runtime;
  runtime.set_service_token("test");
  runtime.set_capacity_low_watermark(0.95);
  ASSERT_ERROR(runtime.validateRuntime(), StatusCode::kInvalidConfig);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
