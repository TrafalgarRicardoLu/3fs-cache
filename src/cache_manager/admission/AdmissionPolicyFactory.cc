#include "cache_manager/admission/AdmissionPolicy.h"
#include "cache_manager/admission/SecondMissAdmissionPolicy.h"

namespace hf3fs::cache_manager {

Result<std::unique_ptr<AdmissionPolicy>> createAdmissionPolicy(std::string_view name,
                                                               uint64_t secondMissWindowNs,
                                                               uint32_t secondMissMaxEntries) {
  if (name != "second_miss") return makeError(StatusCode::kInvalidConfig, "unknown cache admission policy");
  if (secondMissWindowNs == 0 || secondMissMaxEntries == 0) {
    return makeError(StatusCode::kInvalidConfig, "invalid second-miss admission policy limits");
  }
  return std::make_unique<SecondMissAdmissionPolicy>(secondMissWindowNs, secondMissMaxEntries);
}

}  // namespace hf3fs::cache_manager
