#include <gtest/gtest.h>

#include "fbs/meta/Utils.h"

namespace hf3fs::meta::test {
namespace {

flat::ChainTable makeTable(flat::ChainTableRole role) {
  return flat::ChainTable::create(flat::ChainTableId{1},
                                  flat::ChainTableVersion{1},
                                  std::vector{flat::ChainId{1}},
                                  "",
                                  role,
                                  role == flat::ChainTableRole::CACHE_DATA ? uint64_t{4096} : uint64_t{0},
                                  role == flat::ChainTableRole::CACHE_DATA ? flat::ChainTableChecksumType::CRC32C
                                                                           : flat::ChainTableChecksumType::NONE);
}

Inode makeInode(bool origin) {
  auto layout = Layout::newChainRange(flat::ChainTableId{1}, flat::ChainTableVersion{1}, 4096, 1, 0);
  if (!origin) return Inode{InodeId{1}, InodeData{File{layout}}};
  auto object = cache::ImmutableObjectIdentity{
      cache::OriginId{1},
      "bucket",
      "key",
      cache::VersionSelector{cache::VersionSelectorType::STRONG_ETAG, "etag"},
  };
  return Inode{InodeId{2}, InodeData{OriginFile{4096, layout, object}}};
}

TEST(CacheFeatureGate, EnforcesChainTableRole) {
  flat::RoutingInfo routing;
  routing.chainTables[flat::ChainTableId{1}][flat::ChainTableVersion{1}] = makeTable(flat::ChainTableRole::USER_DATA);

  EXPECT_TRUE(RoutingInfoChecker::checkRoutingInfo(makeInode(false), routing));
  EXPECT_FALSE(RoutingInfoChecker::checkRoutingInfo(makeInode(true), routing));

  routing.chainTables[flat::ChainTableId{1}][flat::ChainTableVersion{1}] = makeTable(flat::ChainTableRole::CACHE_DATA);
  EXPECT_FALSE(RoutingInfoChecker::checkRoutingInfo(makeInode(false), routing));
  EXPECT_TRUE(RoutingInfoChecker::checkRoutingInfo(makeInode(true), routing));

  routing.chainTables[flat::ChainTableId{1}][flat::ChainTableVersion{1}] =
      makeTable(flat::ChainTableRole::WRITE_STAGING);
  EXPECT_FALSE(RoutingInfoChecker::checkRoutingInfo(makeInode(false), routing));
  EXPECT_FALSE(RoutingInfoChecker::checkRoutingInfo(makeInode(true), routing));
}

}  // namespace
}  // namespace hf3fs::meta::test
