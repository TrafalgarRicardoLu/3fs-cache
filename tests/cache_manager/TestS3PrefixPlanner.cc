#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>

#include "cache_manager/planner/S3PrefixPlanner.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::origin::ObjectMetadata object(std::string key, uint64_t size, std::string version = "v1") {
  return {{cache::OriginId{1}, "bucket", std::move(key), {cache::VersionSelectorType::VERSION_ID, std::move(version)}},
          size};
}

class PrefixStore : public cache::origin::ObjectStore {
 public:
  CoTryTask<cache::origin::ObjectMetadata> head(const cache::ObjectRef &) override {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange) override {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<cache::origin::ListObjectsPage> listObjects(const cache::origin::ListObjectsRequest &request) override {
    continuations.push_back(request.continuation);
    auto found = pages.find(request.continuation);
    if (found == pages.end()) co_return makeError(CacheCode::kInvalidResponse);
    co_return found->second;
  }

  std::map<std::string, cache::origin::ListObjectsPage> pages;
  std::vector<std::string> continuations;
};

class PrefixImporter : public OriginFileImporter {
 public:
  CoTryTask<meta::Inode> import(std::string_view path,
                                const cache::origin::ObjectMetadata &metadata,
                                const PrefixImportLayout &) override {
    paths.emplace_back(path);
    objects.push_back(metadata.identity);
    if (failNext) {
      failNext = false;
      co_return makeError(CacheCode::kTimeout, "import timeout");
    }
    auto id = metadata.identity.version.value == "v2" ? uint64_t{9} : nextId++;
    co_return meta::Inode{meta::InodeId{id},
                          meta::InodeData{meta::OriginFile{metadata.size,
                                                           meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1),
                                                           metadata.identity}}};
  }

  bool failNext{false};
  uint64_t nextId{7};
  std::vector<std::string> paths;
  std::vector<cache::ImmutableObjectIdentity> objects;
};

S3PrefixPlannerConfig prefixConfig() { return {{flat::ChainTableId{2}, 4096, 1, meta::Permission{0444}}, 16}; }

cache::S3PrefixSource prefixSource() { return {cache::OriginId{1}, "bucket", "prefix/", "/destination"}; }

PlannerContext prefixContext() { return {cache::PrefetchJobId{Uuid::from(1, 5)}, 4, 8, 1, {}}; }

TEST(TestS3PrefixPlanner, PaginatesImportsAndResumesCurrentObjectBeforeListingMore) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto store = std::make_shared<PrefixStore>();
    auto importer = std::make_shared<PrefixImporter>();
    store->pages[""] = {{object("prefix/a", 8192)}, "t1", false};
    store->pages["t1"] = {{object("prefix/space name", 4096, "v2")}, {}, true};

    S3PrefixPlanner planner{store, importer, prefixSource(), prefixConfig(), prefixContext(), 4096};
    auto first = co_await planner.nextPage("");
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->entries[0].key, (cache::CacheBlockKey{7, cache::CacheBlockIndex{0}}));
    CO_ASSERT_EQ(store->continuations, (std::vector<std::string>{""}));

    S3PrefixPlanner resumed{store, importer, prefixSource(), prefixConfig(), prefixContext(), 4096};
    auto second = co_await resumed.nextPage(first->nextCursor);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->entries[0].key, (cache::CacheBlockKey{7, cache::CacheBlockIndex{1}}));
    CO_ASSERT_EQ(store->continuations, (std::vector<std::string>{""}));

    auto third = co_await resumed.nextPage(second->nextCursor);
    CO_ASSERT_OK(third);
    CO_ASSERT_TRUE(third->done);
    CO_ASSERT_EQ(third->entries[0].key.inode, uint64_t{9});
    CO_ASSERT_EQ(store->continuations, (std::vector<std::string>{"", "t1"}));
    CO_ASSERT_EQ(importer->paths, (std::vector<std::string>{"/destination/a", "/destination/space name"}));
  }());
}

TEST(TestS3PrefixPlanner, ReplaysSameObjectAfterImportOrPageCommitTimeout) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto store = std::make_shared<PrefixStore>();
    auto importer = std::make_shared<PrefixImporter>();
    store->pages[""] = {{object("prefix/a", 4096)}, {}, true};
    importer->failNext = true;
    S3PrefixPlanner planner{store, importer, prefixSource(), prefixConfig(), prefixContext(), 4096};
    CO_ASSERT_ERROR(co_await planner.nextPage(""), CacheCode::kTimeout);
    auto retried = co_await planner.nextPage("");
    CO_ASSERT_OK(retried);
    CO_ASSERT_TRUE(retried->done);

    auto repeated = co_await planner.nextPage("");
    CO_ASSERT_OK(repeated);
    CO_ASSERT_EQ(repeated->entries[0].key.block, cache::CacheBlockIndex{0});
    CO_ASSERT_EQ(store->continuations, (std::vector<std::string>{"", "", ""}));
  }());
}

TEST(TestS3PrefixPlanner, RejectsUnsafeObjectKeyMapping) {
  auto store = std::make_shared<PrefixStore>();
  auto importer = std::make_shared<PrefixImporter>();
  store->pages[""] = {{object("prefix/../escape", 4096)}, {}, true};
  S3PrefixPlanner planner{store, importer, prefixSource(), prefixConfig(), prefixContext(), 4096};
  auto result = folly::coro::blockingWait(planner.nextPage(""));
  ASSERT_ERROR(result, StatusCode::kInvalidArg);
  EXPECT_TRUE(importer->paths.empty());
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
