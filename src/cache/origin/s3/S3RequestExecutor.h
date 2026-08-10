#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "fbs/cache/Common.h"

namespace hf3fs::cache::origin::s3 {

enum class S3FailureKind : uint8_t {
  TIMEOUT,
  THROTTLED,
  SERVER,
  NOT_FOUND,
  NO_SUCH_UPLOAD,
  VERSION_MISMATCH,
  AUTHENTICATION,
  INVALID_RESPONSE,
  OTHER,
};

struct S3Failure {
  S3FailureKind kind{S3FailureKind::OTHER};
  int httpStatus{0};
  std::string message;
};

struct HeadRequest {
  ObjectRef object;
};

struct HeadResponse {
  uint64_t size{0};
  std::optional<std::string> versionId;
  std::optional<std::string> etag;
};

struct GetRangeRequest {
  ImmutableObjectIdentity object;
  ByteRange range;
};

struct GetRangeResponse {
  int httpStatus{0};
  std::string contentRange;
  std::optional<std::string> versionId;
  std::optional<std::string> etag;
  std::vector<uint8_t> body;
};

struct ListRequest {
  std::string bucket;
  std::string prefix;
  std::string continuation;
  uint32_t maxKeys{0};
};

struct ListedObject {
  std::string key;
  uint64_t size{0};
  std::optional<std::string> versionId;
  std::optional<std::string> etag;
};

struct ListResponse {
  std::vector<ListedObject> objects;
  std::string nextContinuation;
  bool truncated{false};
};

struct S3CreateMultipartRequest {
  std::string bucket;
  std::string key;
};

struct S3CreateMultipartResponse {
  std::string uploadId;
};

struct S3UploadPartRequest {
  std::string bucket;
  std::string key;
  std::string uploadId;
  uint32_t partNumber{0};
  std::vector<uint8_t> body;
  std::string checksum;
};

struct S3UploadPartResponse {
  std::string etag;
};

struct S3CompleteMultipartRequest {
  std::string bucket;
  std::string key;
  std::string uploadId;
  std::vector<CompletedUploadPart> parts;
};

struct S3CompleteMultipartResponse {
  std::string bucket;
  std::string key;
  std::optional<std::string> versionId;
  std::optional<std::string> etag;
};

struct S3AbortMultipartRequest {
  std::string bucket;
  std::string key;
  std::string uploadId;
};

struct S3AbortMultipartResponse {};

struct S3DeleteObjectRequest {
  std::string bucket;
  std::string key;
  std::optional<std::string> versionId;
};

struct S3DeleteObjectResponse {};

template <typename T>
using S3Outcome = std::variant<T, S3Failure>;

class S3RequestExecutor {
 public:
  virtual ~S3RequestExecutor() = default;

  virtual S3Outcome<HeadResponse> head(const HeadRequest &request) = 0;
  virtual S3Outcome<GetRangeResponse> getRange(const GetRangeRequest &request) = 0;
  virtual S3Outcome<ListResponse> listObjects(const ListRequest &request) = 0;
  virtual S3Outcome<S3CreateMultipartResponse> createMultipartUpload(const S3CreateMultipartRequest &request) = 0;
  virtual S3Outcome<S3UploadPartResponse> uploadPart(const S3UploadPartRequest &request) = 0;
  virtual S3Outcome<S3CompleteMultipartResponse> completeMultipartUpload(const S3CompleteMultipartRequest &request) = 0;
  virtual S3Outcome<S3AbortMultipartResponse> abortMultipartUpload(const S3AbortMultipartRequest &request) = 0;
  virtual S3Outcome<S3DeleteObjectResponse> deleteObject(const S3DeleteObjectRequest &request) = 0;
};

}  // namespace hf3fs::cache::origin::s3
