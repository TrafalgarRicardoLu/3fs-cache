#include "cache/origin/ObjectStore.h"

#include <limits>

namespace hf3fs::cache::origin {

Result<Void> ListObjectsRequest::valid() const {
  if (originId == OriginId{} || bucket.empty() || prefix.empty()) {
    return makeError(StatusCode::kInvalidArg, "invalid object list origin, bucket, or prefix");
  }
  if (maxKeys == 0 || maxKeys > 1000) {
    return makeError(CacheCode::kRequestTooLarge, "invalid object list page limit");
  }
  return Void{};
}

namespace {
Result<Void> validUploadId(std::string_view uploadId) {
  if (uploadId.empty() || uploadId.size() > kMaxMultipartUploadIdBytes ||
      uploadId.find('\0') != std::string_view::npos) {
    return makeError(StatusCode::kInvalidArg, "invalid multipart upload id");
  }
  return Void{};
}
}  // namespace

Result<Void> CreateMultipartUploadRequest::valid() const { return destination.valid(); }

Result<Void> MultipartUpload::valid() const {
  RETURN_ON_ERROR(destination.valid());
  return validUploadId(uploadId);
}

Result<Void> UploadPartRequest::valid() const {
  RETURN_ON_ERROR(upload.valid());
  if (partNumber == 0 || partNumber > kMaxUploadParts || checksum.size() > kMaxCompletedPartTagBytes ||
      checksum.find('\0') != std::string::npos) {
    return makeError(StatusCode::kInvalidArg, "invalid multipart upload part");
  }
  return Void{};
}

Result<Void> UploadPartResult::valid() const { return part.valid(); }

Result<Void> CompleteMultipartUploadRequest::valid() const {
  RETURN_ON_ERROR(upload.valid());
  if (parts.empty() || parts.size() > kMaxUploadParts) {
    return makeError(StatusCode::kInvalidArg, "invalid multipart completion part count");
  }
  uint64_t total = 0;
  for (size_t index = 0; index < parts.size(); ++index) {
    RETURN_ON_ERROR(parts[index].valid());
    if (parts[index].partNumber != index + 1 || parts[index].size > std::numeric_limits<uint64_t>::max() - total) {
      return makeError(StatusCode::kInvalidArg, "invalid multipart completion part sequence or size");
    }
    total += parts[index].size;
  }
  if (total != expectedSize) return makeError(StatusCode::kInvalidArg, "multipart completion size mismatch");
  return Void{};
}

Result<Void> AbortMultipartUploadRequest::valid() const { return upload.valid(); }

Result<Void> HeadCompletedUploadRequest::valid() const { return destination.valid(); }

CoTryTask<ListObjectsPage> ObjectStore::listObjects(const ListObjectsRequest &) {
  co_return makeError(StatusCode::kNotImplemented, "object listing is not implemented");
}

CoTryTask<MultipartUpload> ObjectStore::createMultipartUpload(const CreateMultipartUploadRequest &) {
  co_return makeError(StatusCode::kNotImplemented, "multipart create is not implemented");
}

CoTryTask<UploadPartResult> ObjectStore::uploadPart(UploadPartRequest) {
  co_return makeError(StatusCode::kNotImplemented, "multipart part upload is not implemented");
}

CoTryTask<ObjectMetadata> ObjectStore::completeMultipartUpload(CompleteMultipartUploadRequest) {
  co_return makeError(StatusCode::kNotImplemented, "multipart completion is not implemented");
}

CoTryTask<Void> ObjectStore::abortMultipartUpload(const AbortMultipartUploadRequest &) {
  co_return makeError(StatusCode::kNotImplemented, "multipart abort is not implemented");
}

CoTryTask<ObjectMetadata> ObjectStore::headCompletedUpload(const HeadCompletedUploadRequest &) {
  co_return makeError(StatusCode::kNotImplemented, "completed multipart HEAD is not implemented");
}

}  // namespace hf3fs::cache::origin
