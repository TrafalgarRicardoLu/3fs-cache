#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "client/cli/admin/CacheOrchestration.h"
#include "client/cli/admin/CacheOriginCli.h"
#include "client/cli/admin/CachePhase2Rollout.h"
#include "client/cli/admin/registerAdminCommands.h"

namespace hf3fs::client::cli::test {
namespace {

cache_manager::GetPhase2CacheStatusRsp healthyStatus(const storage::PhysicalDiskId &diskId) {
  cache_manager::GetPhase2CacheStatusRsp status;
  status.capacityHighWatermark = 0.9;
  cache_manager::Phase2DiskStatus disk;
  disk.physicalDiskId = diskId;
  disk.role = storage::StorageRole::CACHE_ONLY;
  disk.enforcedHighWatermark = 0.9;
  disk.permitStoreHealthy = true;
  disk.eventJournalWritable = true;
  status.disks.push_back(disk);
  return status;
}

flat::RoutingInfo healthyRouting(const storage::PhysicalDiskId &diskId) {
  flat::RoutingInfo routing;
  auto table = flat::ChainTable::create(flat::ChainTableId{1},
                                        flat::ChainTableVersion{1},
                                        std::vector{flat::ChainId{1}},
                                        "cache",
                                        flat::ChainTableRole::CACHE_DATA,
                                        uint64_t{1024},
                                        flat::ChainTableChecksumType::CRC32C);
  routing.chainTables[table.chainTableId][table.chainTableVersion] = table;
  flat::ChainInfo chain;
  chain.chainId = flat::ChainId{1};
  chain.chainVersion = flat::ChainVersion{1};
  chain.targets.push_back(flat::ChainTargetInfo::create(flat::TargetId{1}, flat::PublicTargetState::SERVING));
  routing.chains[chain.chainId] = chain;
  flat::TargetInfo target;
  target.targetId = flat::TargetId{1};
  target.chainId = chain.chainId;
  target.publicState = flat::PublicTargetState::SERVING;
  target.physicalDiskId = diskId;
  target.storageRole = storage::StorageRole::CACHE_ONLY;
  routing.targets[target.targetId] = target;
  return routing;
}

TEST(CacheAdminCli, RegistersLifecycleCommands) {
  Dispatcher dispatcher;
  auto result = folly::coro::blockingWait(registerAdminCommands(dispatcher));
  ASSERT_FALSE(result.hasError()) << result.error().describe();

  const auto usages = dispatcher.getUsages();
  for (const auto *command : {"cache-import",
                              "cache-refresh-origin",
                              "cache-status",
                              "cache-list-blocks",
                              "cache-cleanup",
                              "cache-phase2-rollout",
                              "cache-prefetch",
                              "cache-pin"}) {
    EXPECT_TRUE(usages.contains(command)) << command;
  }
}

TEST(CacheAdminCli, ConvertsReadyRatioWithoutOverflow) {
  EXPECT_EQ(cacheReadyBps(0, 1), 0u);
  EXPECT_EQ(cacheReadyBps(1, 3), 3333u);
  EXPECT_EQ(cacheReadyBps(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()), 10000u);
}

TEST(CacheAdminCli, RefreshRequestIdIsStableAndIdentitySensitive) {
  const meta::PathAt path{meta::InodeId::root(), Path{"data/model"}};
  const cache::ImmutableObjectIdentity first{cache::OriginId{1},
                                             "bucket",
                                             "model",
                                             {cache::VersionSelectorType::VERSION_ID, "version-1"}};
  auto same = cache_admin::stableRefreshRequestId(path, meta::InodeId{101}, first);
  EXPECT_EQ(same, cache_admin::stableRefreshRequestId(path, meta::InodeId{101}, first));

  auto changed = first;
  changed.version.value = "version-2";
  EXPECT_NE(same, cache_admin::stableRefreshRequestId(path, meta::InodeId{101}, changed));
  EXPECT_NE(same, cache_admin::stableRefreshRequestId(path, meta::InodeId{102}, first));
}

TEST(CacheAdminCli, Phase2InventoryFailsClosed) {
  storage::PhysicalDiskId diskId{Uuid::random()};
  auto status = healthyStatus(diskId);
  EXPECT_TRUE(cachePhase2InventoryHealthy(0, status));

  status.eventBacklog = 1;
  EXPECT_FALSE(cachePhase2InventoryHealthy(0, status));
  status.eventBacklog = 0;
  status.disks.front().eventJournalWritable = false;
  EXPECT_FALSE(cachePhase2InventoryHealthy(0, status));
  status.disks.front().eventJournalWritable = true;
  status.disks.front().activeGenerations = 1;
  EXPECT_FALSE(cachePhase2InventoryHealthy(0, status));
  status.disks.front().activeGenerations = 0;
  EXPECT_FALSE(cachePhase2InventoryHealthy(1, status));
}

TEST(CacheAdminCli, Phase2RoutingRejectsMissingAndMixedDisks) {
  storage::PhysicalDiskId diskId{Uuid::random()};
  auto status = healthyStatus(diskId);
  auto routing = healthyRouting(diskId);
  EXPECT_TRUE(cachePhase2RoutingHealthy(routing, status));

  status.disks.front().physicalDiskId.uuid = Uuid::random();
  EXPECT_FALSE(cachePhase2RoutingHealthy(routing, status));
  status = healthyStatus(diskId);
  auto mixed = routing.targets.begin()->second;
  mixed.targetId = flat::TargetId{2};
  mixed.storageRole = storage::StorageRole::USER_DATA;
  routing.targets[mixed.targetId] = mixed;
  EXPECT_FALSE(cachePhase2RoutingHealthy(routing, status));
}

}  // namespace
}  // namespace hf3fs::client::cli::test
