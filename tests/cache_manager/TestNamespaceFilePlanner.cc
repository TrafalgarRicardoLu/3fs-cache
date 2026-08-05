#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/planner/NamespaceFilePlanner.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

meta::Inode originInode(uint64_t length, meta::InodeId id = meta::InodeId{7}) {
  auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                               "bucket",
                                               "object",
                                               {cache::VersionSelectorType::VERSION_ID, "version-1"}};
  return meta::Inode{
      id,
      meta::InodeData{meta::OriginFile{length, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), object}}};
}

class FakeResolver : public NamespaceFileResolver {
 public:
  CoTryTask<meta::Inode> stat(std::string_view path) override {
    ++calls;
    seenPath = path;
    if (error) co_return makeError(*error);
    co_return inode;
  }

  meta::Inode inode = originInode(3 * 4096 - 1);
  std::optional<Status> error;
  size_t calls{0};
  std::string seenPath;
};

PlannerContext context(uint32_t pageLimit = 2) { return {cache::PrefetchJobId{Uuid::from(1, 2)}, 4, 9, pageLimit, {}}; }

NamespaceFilePlanner planner(std::shared_ptr<FakeResolver> resolver, uint32_t pageLimit = 2) {
  return {std::move(resolver), cache::NamespacePathSource{"/dataset/file", false}, context(pageLimit), 4096};
}

TEST(TestNamespaceFilePlanner, PlansRealBlockLengthsAcrossFrozenCursor) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto resolver = std::make_shared<FakeResolver>();
    auto firstPlanner = planner(resolver);
    auto first = co_await firstPlanner.nextPage("");
    CO_ASSERT_OK(first);
    CO_ASSERT_FALSE(first->done);
    CO_ASSERT_EQ(first->entries.size(), size_t{2});
    CO_ASSERT_EQ(first->entries[0].key, (cache::CacheBlockKey{7, cache::CacheBlockIndex{0}}));
    CO_ASSERT_EQ(first->entries[0].blockLength, uint64_t{4096});
    CO_ASSERT_EQ(first->entries[1].blockLength, uint64_t{4096});
    CO_ASSERT_EQ(resolver->seenPath, "/dataset/file");

    resolver->inode = originInode(4096, meta::InodeId{8});
    auto resumedPlanner = planner(resolver);
    auto second = co_await resumedPlanner.nextPage(first->nextCursor);
    CO_ASSERT_OK(second);
    CO_ASSERT_TRUE(second->done);
    CO_ASSERT_EQ(second->entries.size(), size_t{1});
    CO_ASSERT_EQ(second->entries[0].key, (cache::CacheBlockKey{7, cache::CacheBlockIndex{2}}));
    CO_ASSERT_EQ(second->entries[0].blockLength, uint64_t{4095});
    CO_ASSERT_EQ(resolver->calls, size_t{1});
  }());
}

TEST(TestNamespaceFilePlanner, EmptyFileProducesEmptyCompletedPage) {
  auto resolver = std::make_shared<FakeResolver>();
  resolver->inode = originInode(0);
  auto filePlanner = planner(resolver);
  auto page = folly::coro::blockingWait(filePlanner.nextPage(""));
  ASSERT_OK(page);
  EXPECT_TRUE(page->done);
  EXPECT_TRUE(page->entries.empty());
  EXPECT_TRUE(page->nextCursor.empty());
}

TEST(TestNamespaceFilePlanner, RejectsOrdinarySupersededAndInvalidLayoutFiles) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto resolver = std::make_shared<FakeResolver>();
    resolver->inode = meta::Inode{meta::InodeId{7}, meta::InodeData{meta::File{}}};
    auto filePlanner = planner(resolver);
    CO_ASSERT_ERROR(co_await filePlanner.nextPage(""), MetaCode::kNotFile);

    resolver->inode = originInode(4096);
    resolver->inode.asOriginFile().superseded = true;
    filePlanner = planner(resolver);
    CO_ASSERT_ERROR(co_await filePlanner.nextPage(""), CacheCode::kStateConflict);

    resolver->inode = originInode(4096);
    resolver->inode.asOriginFile().layout.chunkSize = 0;
    filePlanner = planner(resolver);
    CO_ASSERT_ERROR(co_await filePlanner.nextPage(""), MetaCode::kInvalidFileLayout);
  }());
}

TEST(TestNamespaceFilePlanner, PropagatesPermissionFailureAndRejectsForeignCursor) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto denied = std::make_shared<FakeResolver>();
    denied->error = Status(MetaCode::kNoPermission, "denied");
    auto deniedPlanner = planner(denied);
    CO_ASSERT_ERROR(co_await deniedPlanner.nextPage(""), MetaCode::kNoPermission);

    auto resolver = std::make_shared<FakeResolver>();
    auto firstPlanner = planner(resolver);
    auto first = co_await firstPlanner.nextPage("");
    CO_ASSERT_OK(first);
    auto foreignContext = context();
    foreignContext.sourceIndex += 1;
    NamespaceFilePlanner foreign{resolver, cache::NamespacePathSource{"/other", false}, foreignContext, 4096};
    CO_ASSERT_ERROR(co_await foreign.nextPage(first->nextCursor), StatusCode::kInvalidArg);
  }());
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
