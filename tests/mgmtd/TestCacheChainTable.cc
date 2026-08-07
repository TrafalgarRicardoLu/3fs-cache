#include <gtest/gtest.h>

#include "common/serde/Serde.h"
#include "fbs/mgmtd/ChainTable.h"
#include "fbs/mgmtd/RoutingInfo.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::mgmtd::test {
namespace {

struct LegacyChainTable {
  SERDE_STRUCT_FIELD(chainTableId, flat::ChainTableId(0));
  SERDE_STRUCT_FIELD(chainTableVersion, flat::ChainTableVersion(0));
  SERDE_STRUCT_FIELD(chains, std::vector<flat::ChainId>{});
  SERDE_STRUCT_FIELD(desc, String{});
};

TEST(CacheChainTable, LegacyDefaultsToUserData) {
  LegacyChainTable legacy{flat::ChainTableId{1}, flat::ChainTableVersion{2}, {flat::ChainId{3}}, "legacy"};
  flat::ChainTable decoded;

  ASSERT_FALSE(serde::deserialize(decoded, serde::serialize(legacy)).hasError());
  EXPECT_EQ(decoded.role, flat::ChainTableRole::USER_DATA);
  EXPECT_EQ(decoded.logicalCapacity, uint64_t{0});
  EXPECT_EQ(decoded.checksumType, flat::ChainTableChecksumType::NONE);
  EXPECT_FALSE(decoded.valid().hasError());
}

TEST(CacheChainTable, ValidatesCacheProperties) {
  auto table = flat::ChainTable::create(flat::ChainTableId{1},
                                        flat::ChainTableVersion{1},
                                        std::vector{flat::ChainId{1}},
                                        "cache",
                                        flat::ChainTableRole::CACHE_DATA,
                                        uint64_t{1024},
                                        flat::ChainTableChecksumType::CRC32C);
  EXPECT_FALSE(table.valid().hasError());

  table.logicalCapacity = 0;
  EXPECT_TRUE(table.valid().hasError());
  table.logicalCapacity = 1024;
  table.checksumType = flat::ChainTableChecksumType::NONE;
  EXPECT_TRUE(table.valid().hasError());
}

TEST(CacheChainTable, WriteStagingIsDistinctFromUserAndCacheData) {
  auto staging = flat::ChainTable::create(flat::ChainTableId{2},
                                          flat::ChainTableVersion{1},
                                          std::vector{flat::ChainId{2}},
                                          "staging",
                                          flat::ChainTableRole::WRITE_STAGING,
                                          uint64_t{0},
                                          flat::ChainTableChecksumType::NONE);
  ASSERT_OK(staging.valid());
  EXPECT_TRUE(staging.isWriteStaging());
  EXPECT_FALSE(staging.isCacheData());
  staging.logicalCapacity = 1;
  ASSERT_ERROR(staging.valid(), StatusCode::kInvalidArg);
}

TEST(CacheChainTable, RequiresEveryActiveMetadataCapability) {
  flat::RoutingInfo routing;
  EXPECT_FALSE(routing.cacheFeatureEnabled(1, 1));

  auto capable = flat::NodeInfo{};
  capable.type = flat::NodeType::META;
  capable.status = flat::NodeStatus::HEARTBEAT_CONNECTED;
  capable.cacheSchemaVersion = 1;
  capable.cacheProtocolVersion = 1;
  routing.nodes[flat::NodeId{1}] = capable;
  EXPECT_TRUE(routing.cacheFeatureEnabled(1, 1));

  auto oldWriter = capable;
  oldWriter.cacheSchemaVersion = 0;
  routing.nodes[flat::NodeId{2}] = oldWriter;
  EXPECT_FALSE(routing.cacheFeatureEnabled(1, 1));

  routing.nodes[flat::NodeId{2}].status = flat::NodeStatus::DISABLED;
  EXPECT_TRUE(routing.cacheFeatureEnabled(1, 1));
}

TEST(CacheChainTable, PersistsTargetStorageIdentity) {
  flat::TargetInfo original;
  original.targetId = flat::TargetId{7};
  original.physicalDiskId.uuid = Uuid::random();
  original.storageRole = storage::StorageRole::CACHE_ONLY;

  flat::TargetInfo decoded;
  ASSERT_FALSE(serde::deserialize(decoded, serde::serialize(original)).hasError());
  EXPECT_EQ(decoded.physicalDiskId, original.physicalDiskId);
  EXPECT_EQ(decoded.storageRole, storage::StorageRole::CACHE_ONLY);
}

TEST(CacheChainTable, PersistsWriteStagingStorageIdentity) {
  flat::TargetInfo original;
  original.targetId = flat::TargetId{8};
  original.physicalDiskId.uuid = Uuid::random();
  original.storageRole = storage::StorageRole::WRITE_STAGING;

  flat::TargetInfo decoded;
  ASSERT_OK(serde::deserialize(decoded, serde::serialize(original)));
  EXPECT_EQ(decoded.storageRole, storage::StorageRole::WRITE_STAGING);
}

}  // namespace
}  // namespace hf3fs::mgmtd::test
