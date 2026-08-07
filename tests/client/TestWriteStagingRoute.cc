#include <gtest/gtest.h>

#include "fuse/WriteStaging.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::fuse::test {
namespace {

TEST(WriteStagingRoute, OnlyAcceptsTheNextSequentialOffset) {
  ASSERT_OK(checkSequentialWrite(0, 0, 4096));
  ASSERT_OK(checkSequentialWrite(4096, 4096, 0));
  ASSERT_ERROR(checkSequentialWrite(4096, 0, 1), StatusCode::kInvalidArg);
  ASSERT_ERROR(checkSequentialWrite(4096, 8192, 1), StatusCode::kInvalidArg);
  ASSERT_ERROR(checkSequentialWrite(0, -1, 1), StatusCode::kInvalidArg);
  ASSERT_ERROR(checkSequentialWrite(INT64_MAX, INT64_MAX, UINT64_MAX), StatusCode::kInvalidArg);
}

TEST(WriteStagingRoute, BuildsDeterministicNormalizedObjectKey) {
  auto jobId = Uuid::from(1, 2);
  auto first = writeStagingObjectKey("uploads/", Path("/datasets/./llama/../train.bin"), jobId);
  auto retry = writeStagingObjectKey("uploads", Path("/datasets/train.bin"), jobId);
  EXPECT_EQ(first, retry);
  EXPECT_EQ(first, "uploads/datasets/train.bin/01000000-0000-0000-0200-000000000000");

  EXPECT_EQ(writeStagingObjectKey("", Path("/file"), jobId), "file/01000000-0000-0000-0200-000000000000");
}

TEST(WriteStagingRoute, IdentifiesOnlyConfiguredRegularStagingFiles) {
  auto staging = meta::server::Inode::newFile(meta::InodeId{10},
                                              meta::Acl{},
                                              meta::Layout::newEmpty(flat::ChainTableId{7}, 4096, 1),
                                              UtcClock::now());
  EXPECT_TRUE(isWriteStagingInode(staging, flat::ChainTableId{7}));
  EXPECT_FALSE(isWriteStagingInode(staging, flat::ChainTableId{8}));
  EXPECT_FALSE(isWriteStagingInode(staging, flat::ChainTableId{}));
}

}  // namespace
}  // namespace hf3fs::fuse::test
