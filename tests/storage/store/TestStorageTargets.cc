#include <folly/experimental/TestUtil.h>

#include "client/mgmtd/RoutingInfo.h"
#include "common/utils/CPUExecutorGroup.h"
#include "common/utils/SysResource.h"
#include "storage/store/StorageTargets.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage {
namespace {

StorageTargets::CreateConfig createConfig(std::vector<flat::TargetId::UnderlyingType> targetIds) {
  StorageTargets::CreateConfig config;
  config.set_chunk_size_list({1_MB});
  config.set_physical_file_count(8);
  config.set_allow_disk_without_uuid(true);
  config.set_target_ids(std::move(targetIds));
  return config;
}

std::shared_ptr<client::RoutingInfo> routingInfo(TargetId targetId, ChainId chainId, flat::ChainTableRole role) {
  auto raw = std::make_shared<flat::RoutingInfo>();
  raw->routingInfoVersion = flat::RoutingInfoVersion{1};

  flat::ChainInfo chain;
  chain.chainId = chainId;
  chain.chainVersion = flat::ChainVersion{1};
  flat::ChainTargetInfo chainTarget;
  chainTarget.targetId = targetId;
  chainTarget.publicState = flat::PublicTargetState::SERVING;
  chain.targets.push_back(chainTarget);
  raw->chains.emplace(chain.chainId, chain);

  flat::TargetInfo target;
  target.targetId = targetId;
  target.chainId = chainId;
  target.publicState = flat::PublicTargetState::SERVING;
  raw->targets.emplace(targetId, target);

  flat::ChainTable table;
  table.chainTableId = flat::ChainTableId{1};
  table.chainTableVersion = flat::ChainTableVersion{1};
  table.chains = {chainId};
  table.role = role;
  raw->chainTables[table.chainTableId][table.chainTableVersion] = table;
  return std::make_shared<client::RoutingInfo>(std::move(raw), SteadyClock::now());
}

TEST(TestStorageTargets, Normal) {
  folly::test::TemporaryDirectory tmpPath;

  StorageTargets::Config config;
  config.set_target_num_per_path(4);
  config.set_target_paths({tmpPath.path()});
  config.set_allow_disk_without_uuid(true);

  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_FALSE(targetMap.snapshot()->getTarget(TargetId{1}));
    ASSERT_FALSE(targetMap.snapshot()->getTarget(TargetId{5}));

    StorageTargets::CreateConfig createConfig;
    createConfig.set_chunk_size_list({1_MB});
    createConfig.set_physical_file_count(8);
    createConfig.set_allow_disk_without_uuid(true);
    createConfig.set_target_ids({1, 2, 3, 4});
    ASSERT_OK(targets.create(createConfig));
    ASSERT_OK(targetMap.snapshot()->getTarget(TargetId{1}));
    ASSERT_FALSE(targetMap.snapshot()->getTarget(TargetId{5}));
  }

  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_FALSE(targetMap.snapshot()->getTarget(TargetId{1}));
    ASSERT_FALSE(targetMap.snapshot()->getTarget(TargetId{5}));

    CPUExecutorGroup executor(8, "");
    ASSERT_OK(targets.load(executor));
    ASSERT_OK(targetMap.snapshot()->getTarget(TargetId{1}));
    ASSERT_FALSE(targetMap.snapshot()->getTarget(TargetId{5}));

    ASSERT_OK(targetMap.offlineTargets(tmpPath.path()));
    auto target = targetMap.snapshot()->getTarget(TargetId{1});
    ASSERT_OK(target);
    ASSERT_TRUE((*target)->diskError);

    auto result = targetMap.offlineTarget(TargetId{1});
    ASSERT_TRUE(result.hasError());
    ASSERT_EQ(result.error().code(), StorageCode::kTargetOffline);
  }
}

TEST(TestStorageTargets, PersistPhysicalDiskIdentityAndRole) {
  folly::test::TemporaryDirectory tmpPath;
  StorageTargets::Config config;
  config.set_target_num_per_path(2);
  config.set_target_paths({tmpPath.path()});
  config.set_disk_roles({StorageRole::CACHE_ONLY});
  config.set_allow_disk_without_uuid(true);

  PhysicalDiskId diskId;
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.create(createConfig({1, 2})));
    auto first = targetMap.snapshot()->getTarget(TargetId{1});
    auto second = targetMap.snapshot()->getTarget(TargetId{2});
    ASSERT_OK(first);
    ASSERT_OK(second);
    diskId = (*first)->physicalDiskId;
    ASSERT_NE(diskId.uuid, Uuid::zero());
    EXPECT_EQ((*first)->physicalDiskId, (*second)->physicalDiskId);
    EXPECT_EQ((*first)->storageRole, StorageRole::CACHE_ONLY);
    EXPECT_EQ((*second)->storageRole, StorageRole::CACHE_ONLY);
  }

  AtomicallyTargetMap targetMap;
  StorageTargets targets(config, targetMap);
  CPUExecutorGroup executor(2, "");
  ASSERT_OK(targets.load(executor));
  auto first = targetMap.snapshot()->getTarget(TargetId{1});
  ASSERT_OK(first);
  EXPECT_EQ((*first)->physicalDiskId, diskId);
  EXPECT_EQ((*first)->storageRole, StorageRole::CACHE_ONLY);
}

TEST(TestStorageTargets, RejectLegacyDiskWhenRolesEnabled) {
  folly::test::TemporaryDirectory tmpPath;
  StorageTargets::Config legacyConfig;
  legacyConfig.set_target_num_per_path(1);
  legacyConfig.set_target_paths({tmpPath.path()});
  legacyConfig.set_allow_disk_without_uuid(true);
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(legacyConfig, targetMap);
    ASSERT_OK(targets.create(createConfig({1})));
  }

  StorageTargets::Config roleConfig;
  roleConfig.set_target_num_per_path(1);
  roleConfig.set_target_paths({tmpPath.path()});
  roleConfig.set_disk_roles({StorageRole::USER_DATA});
  roleConfig.set_allow_disk_without_uuid(true);
  AtomicallyTargetMap targetMap;
  StorageTargets targets(roleConfig, targetMap);
  CPUExecutorGroup executor(1, "");
  ASSERT_ERROR(targets.load(executor), CacheCode::kRoleMismatch);
}

TEST(TestStorageTargets, RejectPersistedRoleChange) {
  folly::test::TemporaryDirectory tmpPath;
  StorageTargets::Config userConfig;
  userConfig.set_target_num_per_path(1);
  userConfig.set_target_paths({tmpPath.path()});
  userConfig.set_disk_roles({StorageRole::USER_DATA});
  userConfig.set_allow_disk_without_uuid(true);
  {
    AtomicallyTargetMap targetMap;
    StorageTargets targets(userConfig, targetMap);
    ASSERT_OK(targets.create(createConfig({1})));
  }

  StorageTargets::Config cacheConfig;
  cacheConfig.set_target_num_per_path(1);
  cacheConfig.set_target_paths({tmpPath.path()});
  cacheConfig.set_disk_roles({StorageRole::CACHE_ONLY});
  cacheConfig.set_allow_disk_without_uuid(true);
  AtomicallyTargetMap targetMap;
  StorageTargets targets(cacheConfig, targetMap);
  CPUExecutorGroup executor(1, "");
  ASSERT_ERROR(targets.load(executor), CacheCode::kRoleMismatch);
}

TEST(TestStorageTargets, RejectRoutingRoleMismatch) {
  auto verify = [](StorageRole diskRole, flat::ChainTableRole tableRole) {
    folly::test::TemporaryDirectory tmpPath;
    StorageTargets::Config config;
    config.set_target_num_per_path(1);
    config.set_target_paths({tmpPath.path()});
    config.set_disk_roles({diskRole});
    config.set_allow_disk_without_uuid(true);

    AtomicallyTargetMap targetMap;
    StorageTargets targets(config, targetMap);
    ASSERT_OK(targets.create(createConfig({1})));
    auto result = targetMap.updateRouting(routingInfo(TargetId{1}, ChainId{1}, tableRole));
    ASSERT_ERROR(result, CacheCode::kRoleMismatch);
    auto target = targetMap.snapshot()->getTarget(TargetId{1});
    ASSERT_OK(target);
    EXPECT_EQ((*target)->vChainId, VersionedChainId{});
  };

  verify(StorageRole::USER_DATA, flat::ChainTableRole::CACHE_DATA);
  verify(StorageRole::CACHE_ONLY, flat::ChainTableRole::USER_DATA);
}

TEST(TestStorageTargets, CalculatesCachePhysicalCapacityFromAllocationModel) {
  CacheTargetPhysicalUsage targets;
  targets.activeBytes = 100;
  targets.reservedBytes = 30;
  targets.unrecycledBytes = 20;
  auto capacity = calculateCacheDiskPhysicalCapacity(targets,
                                                     100,  // engine allocated
                                                     40,   // engine reserved
                                                     1000  // filesystem available
  );
  EXPECT_EQ(capacity.physicalUsedBytes, 180);
  EXPECT_EQ(capacity.reservedBytes, 70);
  EXPECT_EQ(capacity.allocatableBytes, 1000);
  EXPECT_EQ(capacity.capacityBytes, 1250);
}

TEST(TestStorageTargets, CacheFootprintUsesActualAllocationUnit) {
  const std::vector<Size> units{512_KB, 1_MB, 4_MB};
  EXPECT_EQ(*physicalFootprint(false, units, 1_MB, 1), 1_MB);
  EXPECT_EQ(*physicalFootprint(false, units, 1_MB, 1_MB), 1_MB);
  EXPECT_EQ(*physicalFootprint(true, units, 1_MB, 1), 64_KB);
  EXPECT_EQ(*physicalFootprint(true, units, 1_MB, 64_KB + 1), 128_KB);
  EXPECT_EQ(*physicalFootprint(true, units, 1_MB, 1_MB), 1_MB);
  EXPECT_TRUE(physicalFootprint(false, units, 1_MB, 1_MB + 1).hasError());
  EXPECT_TRUE(physicalFootprint(false, units, 2_MB, 1).hasError());
}

}  // namespace
}  // namespace hf3fs::storage
