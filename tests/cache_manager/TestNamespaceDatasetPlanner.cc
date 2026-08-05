#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>

#include "cache_manager/planner/NamespaceDatasetPlanner.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

meta::Inode origin(uint64_t id, uint64_t length = 4096) {
  auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                               "bucket",
                                               "object-" + std::to_string(id),
                                               {cache::VersionSelectorType::VERSION_ID, "version-1"}};
  return {meta::InodeId{id},
          meta::InodeData{
              meta::OriginFile{length, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), std::move(object)}}};
}

meta::Inode directory(uint64_t id) { return {meta::InodeId{id}, meta::InodeData{meta::Directory{}}}; }

class TreeResolver : public NamespaceFileResolver {
 public:
  CoTryTask<meta::Inode> stat(std::string_view path) override {
    ++statCalls;
    auto found = paths.find(std::string(path));
    if (found == paths.end()) co_return makeError(MetaCode::kNotFound);
    co_return found->second;
  }

  CoTryTask<NamespaceListPage> list(meta::InodeId inode, std::string_view after, uint32_t limit) override {
    ++listCalls;
    auto entries = children[inode.u64()];
    std::sort(entries.begin(), entries.end(), [](const auto &lhs, const auto &rhs) { return lhs.name < rhs.name; });
    NamespaceListPage result;
    for (auto &entry : entries) {
      if (entry.name <= after) continue;
      if (result.entries.size() == limit) {
        result.more = true;
        break;
      }
      result.entries.push_back(std::move(entry));
    }
    co_return result;
  }

  std::map<std::string, meta::Inode> paths;
  std::map<uint64_t, std::vector<NamespaceEntry>> children;
  size_t statCalls{0};
  size_t listCalls{0};
};

PlannerContext datasetContext(uint32_t pageLimit = 1, CancellationToken cancellation = {}) {
  return {cache::PrefetchJobId{Uuid::from(1, 3)}, 2, 11, pageLimit, std::move(cancellation)};
}

cache::DatasetSource namespaceSource(bool recursive = true) {
  return cache::DatasetSource{cache::NamespacePathSource{"/root", recursive}};
}

TEST(TestNamespaceDatasetPlanner, RecursesDepthFirstAndResumesFrozenFile) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto resolver = std::make_shared<TreeResolver>();
    resolver->paths["/root"] = directory(100);
    resolver->children[100] = {{"a", directory(101)}, {"z", origin(9)}};
    resolver->children[101] = {{"b", directory(102)}};
    resolver->children[102] = {{"x", origin(7, 8191)}};

    NamespaceDatasetPlanner firstPlanner{resolver, namespaceSource(), datasetContext(), 4096};
    auto first = co_await firstPlanner.nextPage("");
    CO_ASSERT_OK(first);
    CO_ASSERT_FALSE(first->done);
    CO_ASSERT_EQ(first->entries.size(), size_t{1});
    CO_ASSERT_EQ(first->entries[0].key, (cache::CacheBlockKey{7, cache::CacheBlockIndex{0}}));

    resolver->children[102][0].inode = origin(8);
    NamespaceDatasetPlanner resumedPlanner{resolver, namespaceSource(), datasetContext(), 4096};
    auto second = co_await resumedPlanner.nextPage(first->nextCursor);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->entries[0].key, (cache::CacheBlockKey{7, cache::CacheBlockIndex{1}}));
    CO_ASSERT_EQ(second->entries[0].blockLength, uint64_t{4095});
    CO_ASSERT_EQ(resolver->statCalls, size_t{1});

    NamespaceDatasetPlanner finalPlanner{resolver, namespaceSource(), datasetContext(4), 4096};
    auto final = co_await finalPlanner.nextPage(second->nextCursor);
    CO_ASSERT_OK(final);
    CO_ASSERT_TRUE(final->done);
    CO_ASSERT_EQ(final->entries.size(), size_t{1});
    CO_ASSERT_EQ(final->entries[0].key.inode, uint64_t{9});
  }());
}

TEST(TestNamespaceDatasetPlanner, PathListPreservesOverlapForPlanStoreDeduplication) {
  auto resolver = std::make_shared<TreeResolver>();
  resolver->paths["/first"] = origin(7);
  resolver->paths["/same"] = origin(7);
  cache::DatasetSource source{cache::PathListSource{{"/first", "/same"}}};
  NamespaceDatasetPlanner planner{resolver, source, datasetContext(8), 4096};
  auto result = folly::coro::blockingWait(planner.nextPage(""));
  ASSERT_OK(result);
  ASSERT_TRUE(result->done);
  ASSERT_EQ(result->entries.size(), size_t{2});
  EXPECT_EQ(result->entries[0].key, result->entries[1].key);
  EXPECT_EQ(resolver->statCalls, size_t{2});
}

TEST(TestNamespaceDatasetPlanner, RejectsNonRecursiveDirectoryAndListWithoutProgress) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto resolver = std::make_shared<TreeResolver>();
    resolver->paths["/root"] = directory(100);
    NamespaceDatasetPlanner flat{resolver, namespaceSource(false), datasetContext(), 4096};
    CO_ASSERT_ERROR(co_await flat.nextPage(""), MetaCode::kNotFile);

    class StuckResolver final : public TreeResolver {
     public:
      CoTryTask<NamespaceListPage> list(meta::InodeId, std::string_view, uint32_t) override {
        co_return NamespaceListPage{{}, true};
      }
    };
    auto stuck = std::make_shared<StuckResolver>();
    stuck->paths["/root"] = directory(100);
    NamespaceDatasetPlanner recursive{stuck, namespaceSource(), datasetContext(), 4096};
    CO_ASSERT_ERROR(co_await recursive.nextPage(""), CacheCode::kInvalidResponse);
  }());
}

TEST(TestNamespaceDatasetPlanner, HonorsCancellationBeforeTraversal) {
  auto resolver = std::make_shared<TreeResolver>();
  resolver->paths["/root"] = origin(7);
  CancellationSource cancellation;
  cancellation.requestCancellation();
  NamespaceDatasetPlanner planner{resolver, namespaceSource(), datasetContext(1, cancellation.getToken()), 4096};
  EXPECT_THROW(folly::coro::blockingWait(planner.nextPage("")), OperationCancelled);
  EXPECT_EQ(resolver->statCalls, size_t{0});
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
