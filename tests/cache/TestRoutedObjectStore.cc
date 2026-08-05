#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache/origin/RoutedObjectStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache::origin {
namespace {

class RecordingStore final : public ObjectStore {
 public:
  CoTryTask<ObjectMetadata> head(const ObjectRef &object) override {
    lastOrigin = object.originId;
    co_return ObjectMetadata{ImmutableObjectIdentity{object.originId,
                                                     object.bucket,
                                                     object.key,
                                                     VersionSelector{VersionSelectorType::VERSION_ID, "v1"}},
                             3};
  }

  CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) override {
    lastOrigin = object.originId;
    lastRange = range;
    co_return std::vector<uint8_t>(range.length, static_cast<uint8_t>(object.originId.toUnderType()));
  }

  CoTryTask<ListObjectsPage> listObjects(const ListObjectsRequest &request) override {
    lastOrigin = request.originId;
    co_return ListObjectsPage{
        {ObjectMetadata{
            {request.originId, request.bucket, request.prefix + "key", {VersionSelectorType::STRONG_ETAG, "etag"}},
            5}},
        {},
        true};
  }

  OriginId lastOrigin{};
  ByteRange lastRange{};
};

TEST(RoutedObjectStore, RoutesHeadAndRangeByOriginId) {
  auto first = std::make_shared<RecordingStore>();
  auto second = std::make_shared<RecordingStore>();
  RoutedObjectStore store({{1, first}, {2, second}});

  auto head = folly::coro::blockingWait(store.head(ObjectRef{OriginId{2}, "bucket", "key"}));
  ASSERT_OK(head);
  EXPECT_EQ(head->size, uint64_t{3});
  EXPECT_EQ(second->lastOrigin, OriginId{2});
  EXPECT_EQ(first->lastOrigin, OriginId{});

  ImmutableObjectIdentity object{OriginId{1}, "bucket", "key", VersionSelector{VersionSelectorType::VERSION_ID, "v1"}};
  auto range = folly::coro::blockingWait(store.getRange(object, ByteRange{4, 2}));
  ASSERT_OK(range);
  EXPECT_EQ(*range, (std::vector<uint8_t>{1, 1}));
  EXPECT_EQ(first->lastRange, (ByteRange{4, 2}));

  auto listed = folly::coro::blockingWait(store.listObjects({OriginId{2}, "bucket", "prefix/", {}, 10}));
  ASSERT_OK(listed);
  ASSERT_EQ(listed->objects[0].identity.key, "prefix/key");
  EXPECT_EQ(second->lastOrigin, OriginId{2});
}

TEST(RoutedObjectStore, RejectsUnknownOriginWithoutFallingThrough) {
  auto backend = std::make_shared<RecordingStore>();
  RoutedObjectStore store({{1, backend}});
  ImmutableObjectIdentity object{OriginId{9}, "bucket", "key", VersionSelector{VersionSelectorType::VERSION_ID, "v1"}};

  auto result = folly::coro::blockingWait(store.getRange(object, ByteRange{0, 1}));
  ASSERT_ERROR(result, StatusCode::kInvalidConfig);
  EXPECT_EQ(backend->lastOrigin, OriginId{});
}

}  // namespace
}  // namespace hf3fs::cache::origin
