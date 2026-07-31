#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/capacity/PhysicalPreflight.h"
#include "cache_manager/capacity/SpacePoller.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

SteadyTime at(uint64_t seconds) { return SteadyTime{std::chrono::seconds(seconds)}; }

storage::PhysicalDiskId disk(uint64_t value) { return storage::PhysicalDiskId{Uuid::from(0, value)}; }

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
                              uint64_t allocatable = 1000) {
  storage::CacheSpaceInfo result;
  result.physicalDiskId = diskId;
  result.role = storage::StorageRole::CACHE_ONLY;
  result.targets = std::move(targets);
  result.capacityBytes = 1000;
  result.physicalUsedBytes = used;
  result.allocatableBytes = allocatable;
  result.enforcedHighWatermark = 0.9;
  return result;
}

storage::QueryCacheSpaceRsp response(std::initializer_list<storage::CacheSpaceInfo> spaces) {
  storage::QueryCacheSpaceRsp result;
  for (const auto &item : spaces) result.results.emplace_back(item);
  return result;
}

TEST(TestPhysicalCapacity, SharedDiskAddsReplicaFootprintsAndCountsCapacityOnce) {
  auto shared = disk(1);
  PhysicalTopology topology;
  topology.updateRouting(routing({{flat::ChainId{1}, {flat::TargetId{1}, flat::TargetId{2}}}},
                                 {target(1, 10, shared), target(2, 10, shared)}));
  SpacePoller poller(topology);
  ASSERT_OK(poller.observe(flat::NodeId{10},
                           response({space(shared, {flat::TargetId{1}, flat::TargetId{2}}, 600)}),
                           at(1),
                           at(2)));

  PhysicalPreflight preflight(topology, 15_s, 0.9);
  {
    auto reserved = preflight.tryReserve(flat::ChainId{1}, {{flat::TargetId{1}, 100}, {flat::TargetId{2}, 100}}, at(3));
    ASSERT_OK(reserved);
    ASSERT_EQ(reserved->bytesByDisk().size(), 1);
    EXPECT_EQ(reserved->bytesByDisk().at(shared), 200);
    EXPECT_EQ(preflight.locallyReserved(shared), 200);

    ASSERT_ERROR(preflight.tryReserve(flat::ChainId{1}, {{flat::TargetId{1}, 50}, {flat::TargetId{2}, 50}}, at(3)),
                 CacheCode::kCapacityExceeded);
  }
  EXPECT_EQ(preflight.locallyReserved(shared), 0);
}

TEST(TestPhysicalCapacity, RejectsOnlyChainsWithMissingOrStaleTopology) {
  auto first = disk(1);
  auto missing = disk(2);
  PhysicalTopology topology;
  topology.updateRouting(routing({{flat::ChainId{1}, {flat::TargetId{1}}}, {flat::ChainId{2}, {flat::TargetId{2}}}},
                                 {target(1, 10, first), target(2, 20, missing)}));
  ASSERT_OK(topology.updateSpace(flat::NodeId{10}, response({space(first, {flat::TargetId{1}}, 100)}), at(1), at(2)));

  ASSERT_OK(topology.resolve(flat::ChainId{1}, at(3), 15_s, 0.9));
  ASSERT_ERROR(topology.resolve(flat::ChainId{2}, at(3), 15_s, 0.9), CacheCode::kUnavailable);
  ASSERT_ERROR(topology.resolve(flat::ChainId{1}, at(18), 15_s, 0.9), CacheCode::kUnavailable);
}

TEST(TestPhysicalCapacity, RejectsSlowPollAndWatermarkMismatch) {
  auto diskId = disk(1);
  PhysicalTopology slow;
  slow.updateRouting(routing({{flat::ChainId{1}, {flat::TargetId{1}}}}, {target(1, 10, diskId)}));
  std::vector times{at(1), at(20)};
  size_t clockIndex = 0;
  SpacePoller poller(
      slow,
      [diskId](const storage::QueryCacheSpaceReq &) -> CoTryTask<storage::QueryCacheSpaceRsp> {
        co_return response({space(diskId, {flat::TargetId{1}}, 100)});
      },
      [&] { return times.at(clockIndex++); });
  ASSERT_OK(folly::coro::blockingWait(poller.poll(flat::NodeId{10}, {flat::TargetId{1}})));
  ASSERT_ERROR(slow.resolve(flat::ChainId{1}, at(20), 15_s, 0.9), CacheCode::kUnavailable);

  PhysicalTopology mismatch;
  mismatch.updateRouting(routing({{flat::ChainId{1}, {flat::TargetId{1}}}}, {target(1, 10, diskId)}));
  auto advertised = space(diskId, {flat::TargetId{1}}, 100);
  advertised.enforcedHighWatermark = 0.8;
  ASSERT_OK(mismatch.updateSpace(flat::NodeId{10}, response({advertised}), at(1), at(2)));
  ASSERT_ERROR(mismatch.resolve(flat::ChainId{1}, at(3), 15_s, 0.9), CacheCode::kStateConflict);
}

TEST(TestPhysicalCapacity, DifferentDisksApplyIndependentPressure) {
  auto pressured = disk(1);
  auto free = disk(2);
  PhysicalTopology topology;
  topology.updateRouting(routing({{flat::ChainId{1}, {flat::TargetId{1}, flat::TargetId{2}}}},
                                 {target(1, 10, pressured), target(2, 20, free)}));
  ASSERT_OK(
      topology.updateSpace(flat::NodeId{10}, response({space(pressured, {flat::TargetId{1}}, 850)}), at(1), at(2)));
  ASSERT_OK(topology.updateSpace(flat::NodeId{20}, response({space(free, {flat::TargetId{2}}, 100)}), at(1), at(2)));
  PhysicalPreflight preflight(topology, 15_s, 0.9);
  ASSERT_ERROR(preflight.tryReserve(flat::ChainId{1}, {{flat::TargetId{1}, 50}, {flat::TargetId{2}, 50}}, at(3)),
               CacheCode::kCapacityExceeded);
  EXPECT_EQ(preflight.locallyReserved(pressured), 0);
  EXPECT_EQ(preflight.locallyReserved(free), 0);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
