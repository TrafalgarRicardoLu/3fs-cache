#include <array>
#include <atomic>
#include <folly/experimental/TestUtil.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <set>
#include <thread>

#include "client/mgmtd/RoutingInfo.h"
#include "common/utils/CPUExecutorGroup.h"
#include "common/utils/SysResource.h"
#include "kv/MemDBStore.h"
#include "storage/service/CachePermitCoordinator.h"
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

PermitIdentity permit(uint64_t attempt, uint64_t generation, uint64_t footprint = 100) {
  auto placement =
      PlacementIdentity::create({ChainId{1}, ChainVer{1}}, {TargetId{1}}, TargetId{1}, Uuid::from(0, attempt));
  return PermitIdentity{Uuid::from(9, 9), *placement, generation, {{TargetId{1}, footprint}}};
}

PermitIdentity replicatedPermit() {
  auto placement = PlacementIdentity::create({ChainId{1}, ChainVer{1}},
                                             {TargetId{1}, TargetId{2}, TargetId{3}},
                                             TargetId{1},
                                             Uuid::from(0, 11));
  return PermitIdentity{Uuid::from(9, 9), *placement, 1, {{TargetId{1}, 100}, {TargetId{2}, 100}, {TargetId{3}, 100}}};
}

struct FakePermitReplicas {
  PermitIdentity permit = replicatedPermit();
  std::map<int, CachePermitResult> active;
  std::set<int> rejectPrepare;
  std::set<int> rejectRelease;
  std::vector<int> releases;

  CoTryTask<CachePermitResult> query(int node) {
    auto record = active.find(node);
    if (record == active.end()) co_return makeError(CacheCode::kNotFound);
    co_return record->second;
  }

  CoTryTask<CachePermitResult> prepare(int node) {
    auto record = active.find(node);
    if (record != active.end()) co_return record->second;
    if (rejectPrepare.contains(node)) co_return makeError(CacheCode::kCapacityExceeded);
    CachePermitResult result{permit, cache::CachePermitState::RESERVED, 500};
    active.emplace(node, result);
    co_return result;
  }

  CoTryTask<Void> release(int node) {
    releases.push_back(node);
    if (rejectRelease.contains(node)) co_return makeError(CacheCode::kUnavailable);
    active.erase(node);
    co_return Void{};
  }
};

std::unique_ptr<CacheSpaceGate> memoryGate(const kv::KVStore::Config &config,
                                           PhysicalDiskId diskId,
                                           size_t maxRecords = 100,
                                           uint64_t maxBytes = 1_MB) {
  auto store = std::make_unique<CacheSpacePermitStore>(std::make_unique<kv::MemDBStore>(config), maxRecords, maxBytes);
  auto gate = std::make_unique<CacheSpaceGate>(diskId, std::move(store));
  EXPECT_TRUE(gate->init());
  return gate;
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

TEST(TestStorageTargets, CacheSpaceGateReservesRenewsAndReleases) {
  kv::KVStore::Config config;
  PhysicalDiskId diskId{Uuid::from(3, 4)};
  auto gate = memoryGate(config, diskId);
  CacheDiskPhysicalCapacity capacity{1000, 100, 800, 100};
  auto identity = permit(1, 1, 200);
  CachePermitRequestItem item{identity, 200};

  auto prepared = gate->prepare(item, 200, capacity, 0.8, 100);
  ASSERT_OK(prepared);
  EXPECT_EQ(prepared->state, cache::CachePermitState::RESERVED);
  EXPECT_EQ(*gate->reservedBytes(100), 200);
  EXPECT_EQ(gate->prepare(item, 200, capacity, 0.8, 100)->expiresAtNs, 200);

  item.expiresAtNs = 300;
  ASSERT_OK(gate->renew(item, 150));
  EXPECT_EQ(gate->query(identity, 250)->expiresAtNs, 300);
  ASSERT_OK(gate->release(identity, 250));
  ASSERT_OK(gate->release(identity, 250));
  EXPECT_EQ(*gate->reservedBytes(250), 0);
}

TEST(TestStorageTargets, CacheSpaceGateEnforcesHighWatermarkAndIdentity) {
  kv::KVStore::Config config;
  auto gate = memoryGate(config, PhysicalDiskId{Uuid::from(3, 5)});
  CacheDiskPhysicalCapacity capacity{1000, 100, 800, 100};
  auto first = permit(1, 1, 250);
  ASSERT_OK(gate->prepare({first, 500}, 250, capacity, 0.5, 100));
  ASSERT_ERROR(gate->prepare({permit(2, 1, 100), 500}, 100, capacity, 0.5, 100), CacheCode::kCapacityExceeded);
  ASSERT_ERROR(gate->prepare({first, 500}, 251, capacity, 0.9, 100), CacheCode::kPermitConflict);
}

TEST(TestStorageTargets, CacheSpaceGateExpiresButPinsExecutingPermit) {
  kv::KVStore::Config config;
  auto gate = memoryGate(config, PhysicalDiskId{Uuid::from(3, 6)});
  CacheDiskPhysicalCapacity capacity{1000, 0, 1000, 0};
  auto expired = permit(1, 1);
  ASSERT_OK(gate->prepare({expired, 200}, 100, capacity, 0.9, 100));
  EXPECT_EQ(*gate->reservedBytes(201), 0);
  ASSERT_ERROR(gate->query(expired, 201), CacheCode::kNotFound);

  auto executing = permit(2, 1);
  ASSERT_OK(gate->prepare({executing, 300}, 100, capacity, 0.9, 100));
  ASSERT_OK(gate->pin(executing, 150));
  EXPECT_EQ(*gate->reservedBytes(301), 100);
  ASSERT_ERROR(gate->release(executing, 301), CacheCode::kPermitConflict);
  ASSERT_OK(gate->consume(executing));
  EXPECT_EQ(*gate->reservedBytes(301), 0);
}

TEST(TestStorageTargets, CacheSpaceGateFailsClosedWhenStoreIsFull) {
  kv::KVStore::Config config;
  auto gate = memoryGate(config, PhysicalDiskId{Uuid::from(3, 7)}, 1);
  CacheDiskPhysicalCapacity capacity{1000, 0, 1000, 0};
  ASSERT_OK(gate->prepare({permit(1, 1), 500}, 100, capacity, 0.9, 100));
  ASSERT_ERROR(gate->prepare({permit(2, 1), 500}, 100, capacity, 0.9, 100), CacheCode::kJournalFull);
}

TEST(TestStorageTargets, CacheSpaceGateRecoversPermitsAfterRestart) {
  folly::test::TemporaryDirectory directory;
  kv::KVStore::Config config;
  config.set_type(kv::KVStore::Type::LevelDB);
  PhysicalDiskId diskId{Uuid::from(3, 8)};
  auto identity = permit(1, 1);
  auto open = [&](bool create) {
    kv::KVStore::Options options;
    options.type = kv::KVStore::Type::LevelDB;
    options.path = directory.path() / "permits";
    options.createIfMissing = create;
    auto store = std::make_unique<CacheSpacePermitStore>(kv::KVStore::create(config, options), 100, 1_MB);
    auto gate = std::make_unique<CacheSpaceGate>(diskId, std::move(store));
    EXPECT_TRUE(gate->init());
    return gate;
  };
  {
    auto gate = open(true);
    CacheDiskPhysicalCapacity capacity{1000, 0, 1000, 0};
    ASSERT_OK(gate->prepare({identity, 500}, 100, capacity, 0.9, 100));
  }
  auto recovered = open(false);
  ASSERT_OK(recovered->query(identity, 200));
  EXPECT_EQ(*recovered->reservedBytes(200), 100);
}

TEST(TestStorageTargets, CacheSpaceGateConcurrentPrepareCannotCrossHighWatermark) {
  kv::KVStore::Config config;
  auto gate = memoryGate(config, PhysicalDiskId{Uuid::from(3, 9)});
  CacheDiskPhysicalCapacity capacity{1000, 0, 1000, 0};
  std::atomic<uint32_t> accepted{0};
  std::vector<std::thread> threads;
  for (uint64_t index = 1; index <= 10; ++index) {
    threads.emplace_back([&, index] {
      if (gate->prepare({permit(index, 1), 500}, 100, capacity, 0.5, 100)) ++accepted;
    });
  }
  for (auto &thread : threads) thread.join();
  EXPECT_EQ(accepted, 5);
  EXPECT_EQ(*gate->reservedBytes(100), 500);
}

TEST(TestStorageTargets, CachePermitCoordinatorRollsBackPartialPrepare) {
  FakePermitReplicas replicas;
  replicas.rejectPrepare.insert(2);
  std::array nodes{1, 2, 3};
  auto result = folly::coro::blockingWait(CachePermitCoordinator::prepare<int>(
      nodes,
      [&](int node) { return replicas.query(node); },
      [&](int node) { return replicas.prepare(node); },
      [&](int node) { return replicas.release(node); }));
  ASSERT_ERROR(result, CacheCode::kCapacityExceeded);
  EXPECT_TRUE(replicas.active.empty());
  EXPECT_EQ(replicas.releases, (std::vector<int>{1, 2}));
}

TEST(TestStorageTargets, CachePermitCoordinatorRetryRecoversAfterCoordinatorCrash) {
  FakePermitReplicas replicas;
  replicas.active.emplace(1, CachePermitResult{replicas.permit, cache::CachePermitState::RESERVED, 500});
  std::array nodes{1, 2, 3};
  auto result = folly::coro::blockingWait(CachePermitCoordinator::prepare<int>(
      nodes,
      [&](int node) { return replicas.query(node); },
      [&](int node) { return replicas.prepare(node); },
      [&](int node) { return replicas.release(node); }));
  ASSERT_OK(result);
  EXPECT_EQ(replicas.active.size(), 3);
  EXPECT_TRUE(replicas.releases.empty());

  auto duplicate = folly::coro::blockingWait(CachePermitCoordinator::prepare<int>(
      nodes,
      [&](int node) { return replicas.query(node); },
      [&](int node) { return replicas.prepare(node); },
      [&](int node) { return replicas.release(node); }));
  ASSERT_OK(duplicate);
  EXPECT_EQ(replicas.active.size(), 3);
}

TEST(TestStorageTargets, CachePermitCoordinatorFindsAndCleansFailedRollback) {
  FakePermitReplicas replicas;
  replicas.rejectPrepare.insert(2);
  replicas.rejectRelease.insert(1);
  std::array nodes{1, 2, 3};
  auto prepared = folly::coro::blockingWait(CachePermitCoordinator::prepare<int>(
      nodes,
      [&](int node) { return replicas.query(node); },
      [&](int node) { return replicas.prepare(node); },
      [&](int node) { return replicas.release(node); }));
  ASSERT_ERROR(prepared, CacheCode::kCapacityExceeded);
  ASSERT_TRUE(replicas.active.contains(1));

  auto recovered = folly::coro::blockingWait(
      CachePermitCoordinator::query<int>(nodes, [&](int node) { return replicas.query(node); }));
  ASSERT_OK(recovered);
  EXPECT_EQ(recovered->permit.managerEpoch, replicas.permit.managerEpoch);

  replicas.rejectRelease.clear();
  auto released = folly::coro::blockingWait(
      CachePermitCoordinator::release<int>(nodes, [&](int node) { return replicas.release(node); }));
  ASSERT_OK(released);
  EXPECT_TRUE(replicas.active.empty());
}

}  // namespace
}  // namespace hf3fs::storage
