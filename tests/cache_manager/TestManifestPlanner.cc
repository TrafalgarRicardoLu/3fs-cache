#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <map>

#include "cache_manager/planner/ManifestPlanner.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

cache::ImmutableObjectIdentity identity(std::string version = "manifest-v1") {
  return {cache::OriginId{1}, "bucket", "manifest", {cache::VersionSelectorType::VERSION_ID, std::move(version)}};
}

meta::Inode origin(uint64_t id, uint64_t length, cache::ImmutableObjectIdentity object = identity("data-v1")) {
  return {meta::InodeId{id},
          meta::InodeData{
              meta::OriginFile{length, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), std::move(object)}}};
}

class ManifestResolver : public NamespaceFileResolver {
 public:
  CoTryTask<meta::Inode> stat(std::string_view path) override {
    auto found = inodes.find(std::string(path));
    if (found == inodes.end()) co_return makeError(MetaCode::kNotFound);
    co_return found->second;
  }

  std::map<std::string, meta::Inode> inodes;
};

class ManifestStore : public cache::origin::ObjectStore {
 public:
  CoTryTask<cache::origin::ObjectMetadata> head(const cache::ObjectRef &) override {
    co_return makeError(StatusCode::kNotImplemented);
  }

  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &object,
                                           cache::ByteRange range) override {
    objects.push_back(object);
    ranges.push_back(range);
    if (range.offset + range.length > body.size()) co_return makeError(StatusCode::kInvalidArg);
    co_return std::vector<uint8_t>(body.begin() + range.offset, body.begin() + range.offset + range.length);
  }

  std::vector<uint8_t> body;
  std::vector<cache::ImmutableObjectIdentity> objects;
  std::vector<cache::ByteRange> ranges;
};

PlannerContext manifestContext(uint32_t pageLimit = 2) {
  return {cache::PrefetchJobId{Uuid::from(1, 4)}, 3, 5, pageLimit, {}};
}

ManifestPlannerConfig manifestConfig() { return {1024, 128, 32, 16, 3}; }

TEST(TestManifestPlanner, ParsesSegmentedUtf8CrlfCommentsAndDuplicatePaths) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto resolver = std::make_shared<ManifestResolver>();
    auto store = std::make_shared<ManifestStore>();
    const std::string body = "# comment\r\n\n/data/\xCE\xB1\r\n/data/b\n/data/\xCE\xB1";
    store->body.assign(body.begin(), body.end());
    resolver->inodes["/manifest"] = origin(10, body.size(), identity());
    resolver->inodes["/data/\xCE\xB1"] = origin(7, 4097);
    resolver->inodes["/data/b"] = origin(8, 4096);

    ManifestPlanner first{resolver,
                          store,
                          cache::ManifestPathSource{"/manifest"},
                          manifestConfig(),
                          manifestContext(),
                          4096};
    auto page = co_await first.nextPage("");
    CO_ASSERT_OK(page);
    CO_ASSERT_EQ(page->entries.size(), size_t{2});
    CO_ASSERT_EQ(page->entries[0].key.inode, uint64_t{7});
    CO_ASSERT_EQ(page->entries[1].blockLength, uint64_t{1});

    resolver->inodes["/manifest"] = origin(11, body.size(), identity("manifest-v2"));
    std::vector<uint64_t> remaining;
    while (!page->done) {
      ManifestPlanner resumed{resolver,
                              store,
                              cache::ManifestPathSource{"/manifest"},
                              manifestConfig(),
                              manifestContext(),
                              4096};
      page = co_await resumed.nextPage(page->nextCursor);
      CO_ASSERT_OK(page);
      for (const auto &entry : page->entries) remaining.push_back(entry.key.inode);
    }
    CO_ASSERT_EQ(remaining, (std::vector<uint64_t>{8, 7, 7}));
    CO_ASSERT_FALSE(store->objects.empty());
    for (const auto &object : store->objects) CO_ASSERT_EQ(object, identity());
  }());
}

TEST(TestManifestPlanner, RejectsInvalidTextAndConfiguredLimits) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    for (const std::string &body :
         {std::string{"relative\n"}, std::string{"/bad\0path\n", 10}, std::string{"/bad\xFF\n", 6}}) {
      auto resolver = std::make_shared<ManifestResolver>();
      auto store = std::make_shared<ManifestStore>();
      store->body.assign(body.begin(), body.end());
      resolver->inodes["/manifest"] = origin(10, body.size(), identity());
      ManifestPlanner planner{resolver,
                              store,
                              cache::ManifestPathSource{"/manifest"},
                              manifestConfig(),
                              manifestContext(),
                              4096};
      CO_ASSERT_ERROR(co_await planner.nextPage(""), StatusCode::kInvalidArg);
    }

    auto resolver = std::make_shared<ManifestResolver>();
    auto store = std::make_shared<ManifestStore>();
    const std::string body = "/data/long\n";
    store->body.assign(body.begin(), body.end());
    resolver->inodes["/manifest"] = origin(10, body.size(), identity());
    auto config = manifestConfig();
    config.maxLineBytes = 4;
    ManifestPlanner longLine{resolver, store, cache::ManifestPathSource{"/manifest"}, config, manifestContext(), 4096};
    CO_ASSERT_ERROR(co_await longLine.nextPage(""), CacheCode::kRequestTooLarge);
  }());
}

TEST(TestManifestPlanner, EnforcesExpandedBlockLimit) {
  auto resolver = std::make_shared<ManifestResolver>();
  auto store = std::make_shared<ManifestStore>();
  const std::string body = "/data/file\n";
  store->body.assign(body.begin(), body.end());
  resolver->inodes["/manifest"] = origin(10, body.size(), identity());
  resolver->inodes["/data/file"] = origin(7, 8192);
  auto config = manifestConfig();
  config.maxExpandedBlocks = 1;
  ManifestPlanner planner{resolver, store, cache::ManifestPathSource{"/manifest"}, config, manifestContext(), 4096};
  auto result = folly::coro::blockingWait(planner.nextPage(""));
  ASSERT_ERROR(result, CacheCode::kRequestTooLarge);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
