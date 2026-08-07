#pragma once

#include <string>
#include <string_view>

#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

class UploadJobKey {
 public:
  static std::string prefix();
  static std::string job(cache::UploadJobId jobId);
  static Result<cache::UploadJobId> unpack(std::string_view key);
};

}  // namespace hf3fs::meta::server
