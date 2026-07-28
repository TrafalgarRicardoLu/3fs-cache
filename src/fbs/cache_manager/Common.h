#pragma once

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "fbs/cache/Common.h"
#include "fbs/core/user/User.h"
#include "fbs/meta/Common.h"

namespace hf3fs::cache_manager {

inline constexpr uint32_t kMaxCacheManagerBlocks = 1000;

struct ServiceIdentity {
  SERDE_STRUCT_FIELD(name, String{});
  SERDE_STRUCT_FIELD(token, String{});

 public:
  Result<Void> valid() const {
    if (name.empty() || token.empty()) return makeError(StatusCode::kInvalidArg, "invalid service identity");
    return Void{};
  }
};

enum class EnsureReason : uint8_t {
  FOREGROUND_MISS,
  PREFETCH,
  RECOVERY,
};

enum class EnsureCachedStatus : uint8_t {
  ACCEPTED,
  ATTACHED,
  BYPASSED,
};

enum class InvalidReason : uint8_t {
  NOT_FOUND,
  GENERATION_MISMATCH,
  CHECKSUM_MISMATCH,
};

enum class CleanupBlockStatus : uint8_t {
  CLEANED,
  RETRYING,
  NOT_FOUND,
  CONFLICT,
};

}  // namespace hf3fs::cache_manager
