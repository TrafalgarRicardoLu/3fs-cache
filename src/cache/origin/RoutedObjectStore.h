#pragma once

#include <memory>
#include <unordered_map>

#include "cache/origin/ObjectStore.h"

namespace hf3fs::cache::origin {

class RoutedObjectStore final : public ObjectStore {
 public:
  using Stores = std::unordered_map<uint32_t, std::shared_ptr<ObjectStore>>;

  explicit RoutedObjectStore(Stores stores)
      : stores_(std::move(stores)) {}

  CoTryTask<ObjectMetadata> head(const ObjectRef &object) override;
  CoTryTask<std::vector<uint8_t>> getRange(const ImmutableObjectIdentity &object, ByteRange range) override;
  CoTryTask<ListObjectsPage> listObjects(const ListObjectsRequest &request) override;
  CoTryTask<MultipartUpload> createMultipartUpload(const CreateMultipartUploadRequest &request) override;
  CoTryTask<UploadPartResult> uploadPart(UploadPartRequest request) override;
  CoTryTask<ObjectMetadata> completeMultipartUpload(CompleteMultipartUploadRequest request) override;
  CoTryTask<Void> abortMultipartUpload(const AbortMultipartUploadRequest &request) override;
  CoTryTask<ObjectMetadata> headCompletedUpload(const HeadCompletedUploadRequest &request) override;
  CoTryTask<Void> deleteObject(const DeleteObjectRequest &request) override;

 private:
  Result<std::shared_ptr<ObjectStore>> find(OriginId originId) const;

  Stores stores_;
};

}  // namespace hf3fs::cache::origin
