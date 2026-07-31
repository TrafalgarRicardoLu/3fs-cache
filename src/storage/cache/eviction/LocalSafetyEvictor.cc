#include "storage/cache/eviction/LocalSafetyEvictor.h"

#include <cmath>
#include <limits>

namespace hf3fs::storage {
namespace {

uint64_t threshold(uint64_t capacity, double ratio) {
  return static_cast<uint64_t>(std::floor(static_cast<long double>(capacity) * ratio));
}

uint64_t saturatingAdd(uint64_t a, uint64_t b) { return a + std::min(b, std::numeric_limits<uint64_t>::max() - a); }

}  // namespace

Result<LocalSafetyRunResult> LocalSafetyEvictor::runOnce(uint64_t nowNs) {
  if (!backend_ || !policy_ || !std::isfinite(highWatermark_) || !std::isfinite(lowWatermark_) || lowWatermark_ <= 0 ||
      lowWatermark_ >= highWatermark_ || highWatermark_ >= 1)
    return makeError(StatusCode::kInvalidConfig, "invalid local safety eviction configuration");
  LocalSafetyRunResult result;
  CHECK_RESULT(recovered, backend_->recoverPrepared());
  result.retired += recovered;
  CHECK_RESULT(disks, backend_->diskSpace());
  for (const auto &disk : disks) {
    if (disk.capacityBytes == 0) continue;
    auto usage = saturatingAdd(disk.physicalUsedBytes, disk.reservedBytes);
    auto high = threshold(disk.capacityBytes, highWatermark_);
    auto low = threshold(disk.capacityBytes, lowWatermark_);
    if (pressured_.contains(disk.diskId)) {
      if (usage < low) pressured_.erase(disk.diskId);
    } else if (usage >= high) {
      pressured_.insert(disk.diskId);
    }
    if (!pressured_.contains(disk.diskId) || usage < low) continue;

    CHECK_RESULT(candidates, backend_->candidates(disk.diskId));
    result.candidates += candidates.size();
    LocalEvictionContext context{usage - low + 1, nowNs, protectionPeriodNs_};
    CHECK_RESULT(selected, policy_->select(candidates, context));
    std::set<size_t> unique;
    for (auto index : selected) {
      if (index >= candidates.size() || !unique.emplace(index).second)
        return makeError(StatusCode::kDataCorruption, "local eviction policy returned an invalid index");
      ++result.selected;
      CHECK_RESULT(retired, backend_->retire(disk.diskId, candidates[index], Uuid::random()));
      if (!retired) continue;
      ++result.retired;
      result.releasedBytes = saturatingAdd(result.releasedBytes, candidates[index].footprintBytes);
      usage = usage > candidates[index].footprintBytes ? usage - candidates[index].footprintBytes : 0;
      if (usage < low) break;
    }
    if (usage < low) pressured_.erase(disk.diskId);
  }
  result.pressuredDisks = pressured_;
  return result;
}

}  // namespace hf3fs::storage
