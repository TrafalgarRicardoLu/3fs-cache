#include "meta/store/cache/UploadJobKey.h"

#include "common/kv/KeyPrefix.h"
#include "common/utils/SerDeser.h"

namespace hf3fs::meta::server {

std::string UploadJobKey::prefix() { return Serializer::serRawArgs(kv::KeyPrefix::UploadJob); }

std::string UploadJobKey::job(cache::UploadJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadJob, jobId.toUnderType());
}

std::string UploadJobKey::activePrefix() { return Serializer::serRawArgs(kv::KeyPrefix::UploadActiveJob); }

std::string UploadJobKey::active(cache::UploadJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadActiveJob, jobId.toUnderType());
}

std::string UploadJobKey::openLeasePrefix() { return Serializer::serRawArgs(kv::KeyPrefix::UploadOpenLease); }

std::string UploadJobKey::openLease(uint64_t expiresAtMs, cache::UploadJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadOpenLease, expiresAtMs, jobId.toUnderType());
}

std::string UploadJobKey::openLeaseTime(uint64_t expiresAtMs) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadOpenLease, expiresAtMs);
}

std::string UploadJobKey::statePrefix(cache::UploadJobState state) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadStateJob, static_cast<uint8_t>(state));
}

std::string UploadJobKey::state(cache::UploadJobState state, cache::UploadJobId jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadStateJob, static_cast<uint8_t>(state), jobId.toUnderType());
}

std::string UploadJobKey::stateIndexMarker() {
  return Serializer::serRawArgs(kv::KeyPrefix::UploadMigration, uint8_t{1});
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

Result<cache::UploadJobId> UploadJobKey::unpackActive(std::string_view key) {
  kv::KeyPrefix prefix;
  Uuid jobId;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, jobId));
  auto result = cache::UploadJobId{jobId};
  if (prefix != kv::KeyPrefix::UploadActiveJob || result == cache::UploadJobId{}) {
    return makeError(StatusCode::kDataCorruption, "invalid active upload job key");
  }
  return result;
}

Result<std::pair<uint64_t, cache::UploadJobId>> UploadJobKey::unpackOpenLease(std::string_view key) {
  kv::KeyPrefix prefix;
  uint64_t expiresAtMs;
  Uuid jobId;
  RETURN_ON_ERROR(Deserializer::deserRawArgs(key, prefix, expiresAtMs, jobId));
  auto result = cache::UploadJobId{jobId};
  if (prefix != kv::KeyPrefix::UploadOpenLease || expiresAtMs == 0 || result == cache::UploadJobId{}) {
    return makeError(StatusCode::kDataCorruption, "invalid open upload lease key");
  }
  return std::pair{expiresAtMs, result};
}

}  // namespace hf3fs::meta::server
