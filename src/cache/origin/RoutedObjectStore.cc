#include "cache/origin/RoutedObjectStore.h"

#include "common/utils/RequestInfo.h"

namespace hf3fs::cache::origin {

namespace {
Result<Void> checkCancellation() {
  auto request = RequestInfo::get();
  if (request && request->canceled()) return makeError(StatusCode::kInterrupted, "object store request was cancelled");
  return Void{};
}
}  // namespace

Result<std::shared_ptr<ObjectStore>> RoutedObjectStore::find(OriginId originId) const {
  auto it = stores_.find(originId.toUnderType());
  if (it == stores_.end() || !it->second) {
    return makeError(StatusCode::kInvalidConfig, "origin {} is not configured", originId.toUnderType());
  }
  return it->second;
}

CoTryTask<ObjectMetadata> RoutedObjectStore::head(const ObjectRef &object) {
  auto store = find(object.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->head(object);
}

CoTryTask<std::vector<uint8_t>> RoutedObjectStore::getRange(const ImmutableObjectIdentity &object, ByteRange range) {
  auto store = find(object.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->getRange(object, range);
}

CoTryTask<ListObjectsPage> RoutedObjectStore::listObjects(const ListObjectsRequest &request) {
  auto store = find(request.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->listObjects(request);
}

CoTryTask<MultipartUpload> RoutedObjectStore::createMultipartUpload(const CreateMultipartUploadRequest &request) {
  CO_RETURN_ON_ERROR(request.valid());
  CO_RETURN_ON_ERROR(checkCancellation());
  auto store = find(request.destination.originId);
  CO_RETURN_ON_ERROR(store);
  auto result = co_await (*store)->createMultipartUpload(request);
  CO_RETURN_ON_ERROR(result);
  auto valid = result->valid();
  if (valid.hasError()) co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
  if (result->destination != request.destination) {
    co_return makeError(CacheCode::kInvalidResponse, "multipart create changed the destination");
  }
  co_return std::move(*result);
}

CoTryTask<UploadPartResult> RoutedObjectStore::uploadPart(UploadPartRequest request) {
  CO_RETURN_ON_ERROR(request.valid());
  CO_RETURN_ON_ERROR(checkCancellation());
  auto store = find(request.upload.destination.originId);
  CO_RETURN_ON_ERROR(store);
  auto expectedPart = request.partNumber;
  auto expectedSize = request.body.size();
  auto expectedChecksum = request.checksum;
  auto result = co_await (*store)->uploadPart(std::move(request));
  CO_RETURN_ON_ERROR(result);
  auto valid = result->valid();
  if (valid.hasError()) co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
  if (result->part.partNumber != expectedPart || result->part.size != expectedSize ||
      (!expectedChecksum.empty() && result->part.checksum != expectedChecksum)) {
    co_return makeError(CacheCode::kInvalidResponse, "multipart upload returned the wrong part identity");
  }
  co_return std::move(*result);
}

CoTryTask<ObjectMetadata> RoutedObjectStore::completeMultipartUpload(CompleteMultipartUploadRequest request) {
  CO_RETURN_ON_ERROR(request.valid());
  CO_RETURN_ON_ERROR(checkCancellation());
  auto store = find(request.upload.destination.originId);
  CO_RETURN_ON_ERROR(store);
  auto destination = request.upload.destination;
  auto expectedSize = request.expectedSize;
  auto result = co_await (*store)->completeMultipartUpload(std::move(request));
  CO_RETURN_ON_ERROR(result);
  auto valid = result->identity.valid();
  if (valid.hasError()) co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
  if (result->size != expectedSize || result->identity.originId != destination.originId ||
      result->identity.bucket != destination.bucket || result->identity.key != destination.key) {
    co_return makeError(CacheCode::kInvalidResponse, "multipart completion returned the wrong object identity");
  }
  co_return std::move(*result);
}

CoTryTask<Void> RoutedObjectStore::abortMultipartUpload(const AbortMultipartUploadRequest &request) {
  CO_RETURN_ON_ERROR(request.valid());
  CO_RETURN_ON_ERROR(checkCancellation());
  auto store = find(request.upload.destination.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->abortMultipartUpload(request);
}

CoTryTask<ObjectMetadata> RoutedObjectStore::headCompletedUpload(const HeadCompletedUploadRequest &request) {
  CO_RETURN_ON_ERROR(request.valid());
  CO_RETURN_ON_ERROR(checkCancellation());
  auto store = find(request.destination.originId);
  CO_RETURN_ON_ERROR(store);
  auto result = co_await (*store)->headCompletedUpload(request);
  CO_RETURN_ON_ERROR(result);
  auto valid = result->identity.valid();
  if (valid.hasError()) co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
  if (result->size != request.expectedSize || result->identity.originId != request.destination.originId ||
      result->identity.bucket != request.destination.bucket || result->identity.key != request.destination.key) {
    co_return makeError(CacheCode::kInvalidResponse, "completed multipart HEAD returned the wrong object identity");
  }
  co_return std::move(*result);
}

CoTryTask<Void> RoutedObjectStore::deleteObject(const DeleteObjectRequest &request) {
  CO_RETURN_ON_ERROR(request.valid());
  CO_RETURN_ON_ERROR(checkCancellation());
  auto store = find(request.object.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->deleteObject(request);
}

}  // namespace hf3fs::cache::origin
