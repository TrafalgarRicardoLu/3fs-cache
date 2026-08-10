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
  static std::string activePrefix();
  static std::string active(cache::UploadJobId jobId);
  static std::string openLeasePrefix();
  static std::string openLease(uint64_t expiresAtMs, cache::UploadJobId jobId);
  static std::string openLeaseTime(uint64_t expiresAtMs);
  static std::string statePrefix(cache::UploadJobState state);
  static std::string state(cache::UploadJobState state, cache::UploadJobId jobId);
  static std::string stateIndexMarker();
  static Result<cache::UploadJobId> unpack(std::string_view key);
  static Result<cache::UploadJobId> unpackActive(std::string_view key);
  static Result<std::pair<uint64_t, cache::UploadJobId>> unpackOpenLease(std::string_view key);
};

}  // namespace hf3fs::meta::server
