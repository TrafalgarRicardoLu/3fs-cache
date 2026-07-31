#include "cache_manager/admission/SecondMissAdmissionPolicy.h"

#include <algorithm>

namespace hf3fs::cache_manager {

AdmissionDecision SecondMissAdmissionPolicy::evaluate(const AdmissionContext &context) {
  std::scoped_lock lock(mutex_);
  auto found = firstMisses_.find(context.key);
  if (found != firstMisses_.end()) {
    if (context.managerReceiveTimeNs > found->second && context.managerReceiveTimeNs - found->second <= windowNs_) {
      firstMisses_.erase(found);
      return {AdmissionAction::ADMIT, AdmissionReason::SECOND_MISS};
    }
    if (context.managerReceiveTimeNs > found->second) found->second = context.managerReceiveTimeNs;
    return {AdmissionAction::BYPASS, AdmissionReason::FIRST_MISS};
  }
  if (firstMisses_.size() >= maxEntries_) evictOldest();
  firstMisses_.emplace(context.key, context.managerReceiveTimeNs);
  return {AdmissionAction::BYPASS, AdmissionReason::FIRST_MISS};
}

void SecondMissAdmissionPolicy::evictOldest() {
  auto oldest = std::min_element(firstMisses_.begin(), firstMisses_.end(), [](const auto &lhs, const auto &rhs) {
    if (lhs.second != rhs.second) return lhs.second < rhs.second;
    return CacheBlockKeyLess{}(lhs.first, rhs.first);
  });
  if (oldest != firstMisses_.end()) firstMisses_.erase(oldest);
}

size_t SecondMissAdmissionPolicy::size() const {
  std::scoped_lock lock(mutex_);
  return firstMisses_.size();
}

}  // namespace hf3fs::cache_manager
