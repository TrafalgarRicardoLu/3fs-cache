#include "fbs/cache_manager/Service.h"

#include <limits>

namespace hf3fs::cache_manager {

Result<Void> PinDatasetReq::valid() const {
  if (pinId == cache::PinOwnerId{}) return makeError(StatusCode::kInvalidArg, "pin id not set");
  RETURN_ON_ERROR(cache::validateDatasetSources(sources));
  if (priority > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return makeError(StatusCode::kInvalidArg, "pin priority is out of range");
  }
  if (ttlMs == 0 || ttlMs > cache::kMaxPinTtlMs) {
    return makeError(StatusCode::kInvalidArg, "invalid pin TTL");
  }
  return Void{};
}

}  // namespace hf3fs::cache_manager
