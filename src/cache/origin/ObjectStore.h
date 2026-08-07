#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache::origin {

struct ObjectMetadata {
  ImmutableObjectIdentity identity;
  uint64_t size{0};
};

struct ListObjectsRequest {
  OriginId originId;
  std::string bucket;
  std::string prefix;
  std::string continuation;
  uint32_t maxKeys{1000};

  Result<Void> valid() const;
};

struct ListObjectsPage {
  std::vector<ObjectMetadata> objects;
  std::string nextContinuation;
  bool done{false};
};

struct CreateMultipartUploadRequest {
  ObjectRef destination;

  Result<Void> valid() const;
};

struct MultipartUpload {
  ObjectRef destination;
  std::string uploadId;

  Result<Void> valid() const;
};

struct UploadPartRequest {
  MultipartUpload upload;
  uint32_t partNumber{0};
  std::vector<uint8_t> body;
  std::string checksum;

  Result<Void> valid() const;
};

struct UploadPartResult {
  CompletedUploadPart part;

  Result<Void> valid() const;
};

struct CompleteMultipartUploadRequest {
  MultipartUpload upload;
  std::vector<CompletedUploadPart> parts;
  uint64_t expectedSize{0};

  Result<Void> valid() const;
};

struct AbortMultipartUploadRequest {
  MultipartUpload upload;

  Result<Void> valid() const;
};

struct HeadCompletedUploadRequest {
  ObjectRef destination;
  uint64_t expectedSize{0};

  Result<Void> valid() const;
};

class ObjectStore {
 public:
  virtual ~ObjectStore() = default;

  virtual CoTryTask<ObjectMetadata> head(const ObjectRef &object) = 0;
  virtual CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) = 0;
  virtual CoTryTask<ListObjectsPage> listObjects(const ListObjectsRequest &request);
  virtual CoTryTask<MultipartUpload> createMultipartUpload(const CreateMultipartUploadRequest &request);
  virtual CoTryTask<UploadPartResult> uploadPart(UploadPartRequest request);
  virtual CoTryTask<ObjectMetadata> completeMultipartUpload(CompleteMultipartUploadRequest request);
  virtual CoTryTask<Void> abortMultipartUpload(const AbortMultipartUploadRequest &request);
  virtual CoTryTask<ObjectMetadata> headCompletedUpload(const HeadCompletedUploadRequest &request);
};

}  // namespace hf3fs::cache::origin
