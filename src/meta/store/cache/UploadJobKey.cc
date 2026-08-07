#include "meta/store/cache/UploadJobKey.h"

#include "common/kv/KeyPrefix.h"
#include "common/utils/SerDeser.h"

namespace hf3fs::meta::server {

std::string UploadJobKey::prefix() { return Serializer::serRawArgs(kv::KeyPrefix::UploadJob); }

std::string UploadJobKey::job(cache::UploadJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadJob, jobId.toUnderType());
}

Result<cache::UploadJobId> UploadJobKey::unpack(std::string_view key) {
  kv::KeyPrefix prefix;
  Uuid jobId;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, jobId));
  auto result = cache::UploadJobId{jobId};
  if (prefix != kv::KeyPrefix::UploadJob || result == cache::UploadJobId{}) {
    return makeError(StatusCode::kDataCorruption, "invalid upload job key");
  }
  return result;
}

}  // namespace hf3fs::meta::server
