#include <gtest/gtest.h>

#include "common/serde/Serde.h"
#include "fbs/meta/Schema.h"

namespace hf3fs::meta::test {
namespace {

struct LegacyInodeData {
  SERDE_STRUCT_FIELD(type, (std::variant<File, Directory, Symlink>()));
  SERDE_STRUCT_FIELD(acl, Acl());
  SERDE_STRUCT_FIELD(nlink, uint16_t(1));
  SERDE_STRUCT_FIELD(atime, UtcTime{});
  SERDE_STRUCT_FIELD(ctime, UtcTime{});
  SERDE_STRUCT_FIELD(mtime, UtcTime{});
};

Inode makeOriginFile(uint64_t length) {
  auto layout = Layout::newChainList(4096, {flat::ChainId{1}});
  cache::ImmutableObjectIdentity object{
      cache::OriginId{1},
      "bucket",
      "key",
      cache::VersionSelector{cache::VersionSelectorType::STRONG_ETAG, "etag"},
  };
  return Inode{InodeId{100}, InodeData{OriginFile{length, layout, object}, Acl{}, 1, {}, {}, {}}};
}

TEST(OriginFileSchema, IsRegularFileLike) {
  auto inode = makeOriginFile(8192);

  ASSERT_FALSE(inode.valid().hasError());
  EXPECT_TRUE(inode.isOriginFile());
  EXPECT_TRUE(inode.isRegularFileLike());
  EXPECT_FALSE(inode.isFile());
  EXPECT_EQ(inode.fileLength(), uint64_t{8192});
  EXPECT_EQ(inode.getChunkId(inode.id, 4096)->chunk(), uint64_t{1});
}

TEST(OriginFileSchema, SerdeRoundTrip) {
  auto original = makeOriginFile(4096);
  Inode decoded;
  auto result = serde::deserialize(decoded, serde::serialize(original));

  ASSERT_FALSE(result.hasError());
  EXPECT_EQ(decoded, original);
}

TEST(OriginFileSchema, LegacyReaderFailsClosed) {
  auto original = makeOriginFile(4096);
  LegacyInodeData decoded;
  auto result = serde::deserialize(decoded, serde::serialize(original.data()));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code(), StatusCode::kSerdeVariantIndexExceeded);
}

}  // namespace
}  // namespace hf3fs::meta::test
