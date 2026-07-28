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

template <typename T>
using S3Outcome = std::variant<T, S3Failure>;

class S3RequestExecutor {
 public:
  virtual ~S3RequestExecutor() = default;

  virtual S3Outcome<HeadResponse> head(const HeadRequest &request) = 0;
  virtual S3Outcome<GetRangeResponse> getRange(const GetRangeRequest &request) = 0;
};

}  // namespace hf3fs::cache::origin::s3
