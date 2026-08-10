#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "cache/origin/RoutedObjectStore.h"
#include "common/utils/RequestInfo.h"
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

  CoTryTask<MultipartUpload> createMultipartUpload(const CreateMultipartUploadRequest &request) override {
    lastOrigin = request.destination.originId;
    ++multipartCalls;
    co_return MultipartUpload{request.destination, "upload-id"};
  }

  CoTryTask<UploadPartResult> uploadPart(UploadPartRequest request) override {
    lastOrigin = request.upload.destination.originId;
    ++multipartCalls;
    co_return UploadPartResult{
        {request.partNumber, request.body.size(), malformedPart ? "" : "etag", request.checksum}};
  }

  CoTryTask<ObjectMetadata> completeMultipartUpload(CompleteMultipartUploadRequest request) override {
    lastOrigin = request.upload.destination.originId;
    ++multipartCalls;
    if (multipartError) co_return makeError(*multipartError, "injected multipart error");
    co_return ObjectMetadata{{request.upload.destination.originId,
                              request.upload.destination.bucket,
                              request.upload.destination.key,
                              {VersionSelectorType::VERSION_ID, "version"}},
                             request.expectedSize};
  }

  CoTryTask<Void> abortMultipartUpload(const AbortMultipartUploadRequest &request) override {
    lastOrigin = request.upload.destination.originId;
    ++multipartCalls;
    co_return Void{};
  }

  CoTryTask<ObjectMetadata> headCompletedUpload(const HeadCompletedUploadRequest &request) override {
    lastOrigin = request.destination.originId;
    ++multipartCalls;
    co_return ObjectMetadata{{request.destination.originId,
                              request.destination.bucket,
                              request.destination.key,
                              {VersionSelectorType::STRONG_ETAG, "etag"}},
                             request.expectedSize};
  }

  CoTryTask<Void> deleteObject(const DeleteObjectRequest &request) override {
    lastOrigin = request.object.originId;
    ++deleteCalls;
    co_return Void{};
  }

  OriginId lastOrigin{};
  ByteRange lastRange{};
  size_t multipartCalls{0};
  size_t deleteCalls{0};
  std::optional<status_code_t> multipartError;
  bool malformedPart{false};
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
  ASSERT_OK(folly::coro::blockingWait(store.deleteObject({object})));
  EXPECT_EQ(first->deleteCalls, size_t{1});
}

TEST(RoutedObjectStore, RejectsUnknownOriginWithoutFallingThrough) {
  auto backend = std::make_shared<RecordingStore>();
  RoutedObjectStore store({{1, backend}});
  ImmutableObjectIdentity object{OriginId{9}, "bucket", "key", VersionSelector{VersionSelectorType::VERSION_ID, "v1"}};

  auto result = folly::coro::blockingWait(store.getRange(object, ByteRange{0, 1}));
  ASSERT_ERROR(result, StatusCode::kInvalidConfig);
  EXPECT_EQ(backend->lastOrigin, OriginId{});
}

TEST(RoutedObjectStore, RoutesTheMultipartLifecycleAndPreservesErrors) {
  auto backend = std::make_shared<RecordingStore>();
  RoutedObjectStore store({{2, backend}});
  ObjectRef destination{OriginId{2}, "bucket", "key"};

  auto created = folly::coro::blockingWait(store.createMultipartUpload({destination}));
  ASSERT_OK(created);
  auto uploaded = folly::coro::blockingWait(
      store.uploadPart(UploadPartRequest{*created, 1, std::vector<uint8_t>{1, 2, 3}, "crc32c"}));
  ASSERT_OK(uploaded);
  ASSERT_EQ(uploaded->part.size, uint64_t{3});

  CompleteMultipartUploadRequest complete{*created, {uploaded->part}, 3};
  auto completed = folly::coro::blockingWait(store.completeMultipartUpload(complete));
  ASSERT_OK(completed);
  EXPECT_EQ(completed->identity.version.type, VersionSelectorType::VERSION_ID);
  ASSERT_OK(folly::coro::blockingWait(store.headCompletedUpload({destination, 3})));
  ASSERT_OK(folly::coro::blockingWait(store.abortMultipartUpload({*created})));
  EXPECT_EQ(backend->multipartCalls, size_t{5});

  backend->multipartError = CacheCode::kAccessDenied;
  auto denied = folly::coro::blockingWait(store.completeMultipartUpload(std::move(complete)));
  ASSERT_ERROR(denied, CacheCode::kAccessDenied);

  backend->malformedPart = true;
  auto malformed =
      folly::coro::blockingWait(store.uploadPart(UploadPartRequest{*created, 1, std::vector<uint8_t>{1}, "checksum"}));
  ASSERT_ERROR(malformed, CacheCode::kInvalidResponse);
}

class CancelledRequest final : public RequestInfo {
 public:
  std::string describe() const override { return "cancelled test request"; }
  bool canceled() const override { return true; }
};

TEST(RoutedObjectStore, RejectsCancelledMultipartBeforeDispatch) {
  auto backend = std::make_shared<RecordingStore>();
  RoutedObjectStore store({{1, backend}});
  folly::ShallowCopyRequestContextScopeGuard guard(RequestInfo::token(), std::make_unique<CancelledRequest>());

  auto result =
      folly::coro::blockingWait(store.createMultipartUpload({ObjectRef{OriginId{1}, "bucket", "cancelled-key"}}));
  ASSERT_ERROR(result, StatusCode::kInterrupted);
  EXPECT_EQ(backend->multipartCalls, size_t{0});
}

MultipartUpload multipartUpload() { return {{OriginId{1}, "bucket", "key"}, "upload-id"}; }

CompletedUploadPart completedPart(uint32_t number, uint64_t size) { return {number, size, "etag", "checksum"}; }

TEST(MultipartObjectStore, ValidatesIdentifiersPartsAndSizes) {
  ASSERT_OK((CreateMultipartUploadRequest{{OriginId{1}, "bucket", "key"}}.valid()));
  ASSERT_OK(multipartUpload().valid());
  ASSERT_ERROR((MultipartUpload{{OriginId{1}, "bucket", "key"}, ""}.valid()), StatusCode::kInvalidArg);

  ASSERT_OK((UploadPartRequest{multipartUpload(), 1, {1}, "checksum"}.valid()));
  ASSERT_OK((UploadPartRequest{multipartUpload(), 1, {}, {}}.valid()));
  ASSERT_ERROR((UploadPartRequest{multipartUpload(), 0, {1}, {}}.valid()), StatusCode::kInvalidArg);

  ASSERT_OK((CompleteMultipartUploadRequest{multipartUpload(), {completedPart(1, 4), completedPart(2, 2)}, 6}.valid()));
  ASSERT_ERROR((CompleteMultipartUploadRequest{multipartUpload(), {completedPart(2, 6)}, 6}.valid()),
               StatusCode::kInvalidArg);
  ASSERT_ERROR((CompleteMultipartUploadRequest{multipartUpload(), {completedPart(1, 6)}, 7}.valid()),
               StatusCode::kInvalidArg);
  ASSERT_ERROR(
      (CompleteMultipartUploadRequest{multipartUpload(),
                                      {completedPart(1, std::numeric_limits<uint64_t>::max()), completedPart(2, 1)},
                                      0}
           .valid()),
      StatusCode::kInvalidArg);
}

TEST(MultipartObjectStore, DefaultBackendFailsClosed) {
  class ReadOnlyStore final : public ObjectStore {
   public:
    CoTryTask<ObjectMetadata> head(const ObjectRef &) override { co_return makeError(CacheCode::kNotFound); }
    CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &, ByteRange) override {
      co_return makeError(CacheCode::kNotFound);
    }
  } store;

  auto result = folly::coro::blockingWait(store.createMultipartUpload({{OriginId{1}, "bucket", "key"}}));
  ASSERT_ERROR(result, StatusCode::kNotImplemented);
}

}  // namespace
}  // namespace hf3fs::cache::origin
