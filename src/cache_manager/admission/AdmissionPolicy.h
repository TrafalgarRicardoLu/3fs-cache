#pragma once

#include <memory>
#include <string_view>

#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache_manager {

enum class AdmissionAction : uint8_t { ADMIT, BYPASS };
enum class AdmissionReason : uint8_t { FIRST_MISS, SECOND_MISS };

struct AdmissionContext {
  cache::CacheBlockKey key;
  uint64_t managerReceiveTimeNs{0};
};

struct AdmissionDecision {
  AdmissionAction action{AdmissionAction::BYPASS};
  AdmissionReason reason{AdmissionReason::FIRST_MISS};
};

class AdmissionPolicy {
 public:
  virtual ~AdmissionPolicy() = default;
  virtual AdmissionDecision evaluate(const AdmissionContext &context) = 0;
};

Result<std::unique_ptr<AdmissionPolicy>> createAdmissionPolicy(std::string_view name,
                                                               uint64_t secondMissWindowNs,
                                                               uint32_t secondMissMaxEntries);

}  // namespace hf3fs::cache_manager
