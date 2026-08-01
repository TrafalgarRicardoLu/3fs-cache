#include "cache_manager/eviction/EvictionController.h"

#include <cmath>
#include <folly/logging/xlog.h>
#include <limits>

#include "cache/metrics/CacheMetrics.h"

namespace hf3fs::cache_manager {
namespace {

uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs) {
  return lhs + std::min(rhs, std::numeric_limits<uint64_t>::max() - lhs);
}

uint64_t watermarkBytes(uint64_t capacity, double watermark) {
  auto value = std::floor(static_cast<long double>(capacity) * static_cast<long double>(watermark));
  if (value < 1.0L) return 1;
  return value >= static_cast<long double>(std::numeric_limits<uint64_t>::max()) ? std::numeric_limits<uint64_t>::max()
                                                                                 : static_cast<uint64_t>(value);
}

bool deficitsMet(const std::map<storage::PhysicalDiskId, uint64_t> &released,
                 const std::map<storage::PhysicalDiskId, uint64_t> &deficits) {
  for (const auto &[disk, deficit] : deficits) {
    auto found = released.find(disk);
    if (found == released.end() || found->second < deficit) return false;
  }
  return true;
}

}  // namespace

Result<Void> EvictionControllerConfig::valid() const {
  if (!std::isfinite(highWatermark) || !std::isfinite(lowWatermark) || lowWatermark <= 0.0 ||
      lowWatermark >= highWatermark || highWatermark >= 1.0 || snapshotMaxAge <= 0_ns || protectionPeriod < 0_ns ||
      candidatePageSize == 0 || candidatePageSize > cache::kMaxPhase2BatchItems || batchSize == 0 ||
      batchSize > cache::kMaxPhase2BatchItems) {
    return makeError(StatusCode::kInvalidConfig, "invalid cache eviction controller configuration");
  }
  return Void{};
}

EvictionController::EvictionController(std::shared_ptr<CacheManagerBackend> backend,
                                       PhysicalTopology &topology,
                                       EvictionPolicy &policy,
                                       EvictionPressureState &pressure,
                                       EvictionControllerConfig config)
    : backend_(std::move(backend)),
      topology_(topology),
      policy_(policy),
      pressure_(pressure),
      config_(config),
      candidateSource_(backend_, topology_, config_.candidatePageSize) {}

Result<std::map<storage::PhysicalDiskId, uint64_t>> EvictionController::updatePressure(
    const std::map<storage::PhysicalDiskId, DiskSpaceSnapshot> &snapshots) {
  auto pressured = pressure_.snapshot();
  std::map<storage::PhysicalDiskId, uint64_t> deficits;
  for (const auto &[disk, snapshot] : snapshots) {
    const auto &space = snapshot.space;
    auto usage = saturatingAdd(space.physicalUsedBytes, space.reservedBytes);
    auto high = watermarkBytes(space.capacityBytes, config_.highWatermark);
    auto low = watermarkBytes(space.capacityBytes, config_.lowWatermark);
    if (pressured.contains(disk)) {
      if (usage < low) pressured.erase(disk);
    } else if (usage >= high) {
      pressured.insert(disk);
    }
    if (pressured.contains(disk) && usage >= low) {
      deficits.emplace(disk, usage - (low - 1));
    }
  }
  pressure_.replace(std::move(pressured));
  return deficits;
}

CoTryTask<std::vector<EvictionCandidate>> EvictionController::collectCandidates(
    const std::set<storage::PhysicalDiskId> &pressuredDisks,
    UtcTime wallNow) {
  std::vector<EvictionCandidate> candidates;
  std::optional<cache::CacheBlockKey> after;
  bool more = true;
  EvictionContext protectionContext{{}, wallNow, config_.protectionPeriod};
  while (more && candidates.size() < config_.batchSize) {
    auto page = co_await candidateSource_.next(after, pressuredDisks);
    CO_RETURN_ON_ERROR(page);
    for (auto &candidate : page->candidates) {
      if (protectedFromEviction(candidate, protectionContext)) continue;
      candidates.push_back(std::move(candidate));
      if (candidates.size() == config_.batchSize) break;
    }
    more = page->more;
    after = page->nextAfter;
  }
  co_return candidates;
}

CoTryTask<EvictionRunResult> EvictionController::runOnce(SteadyTime steadyNow, UtcTime wallNow) {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_) co_return makeError(StatusCode::kInvalidConfig, "cache eviction backend is unavailable");
  auto snapshots = topology_.freshDiskSnapshots(steadyNow, config_.snapshotMaxAge, config_.highWatermark);
  CO_RETURN_ON_ERROR(snapshots);

  EvictionRunResult result;
  auto deficits = updatePressure(*snapshots);
  CO_RETURN_ON_ERROR(deficits);
  result.pressuredDisks = pressure_.snapshot();
  result.deficits = *deficits;
  if (result.deficits.empty()) {
    result.targetMet = true;
    cache::metrics::recordCount(cache::metrics::Event::MANAGER_EVICTION_RESULT, 1, {.reason = "not_pressured"});
    co_return result;
  }

  auto candidates = co_await collectCandidates(result.pressuredDisks, wallNow);
  CO_RETURN_ON_ERROR(candidates);
  result.candidates = candidates->size();
  EvictionContext context{result.deficits, wallNow, config_.protectionPeriod};
  auto selected = policy_.select(*candidates, context);
  CO_RETURN_ON_ERROR(selected);
  CO_RETURN_ON_ERROR(validateEvictionSelection(*candidates, context, *selected, config_.batchSize));
  result.selected = selected->size();

  std::vector<meta::BeginEvictCacheBlockItem> items;
  items.reserve(selected->size());
  for (auto index : *selected) {
    const auto &candidate = candidates->at(index);
    items.push_back({candidate.key, candidate.ready, cache::EvictionReason::CAPACITY_WATERMARK});
  }
  if (items.empty()) {
    XLOGF(WARN, "Cache eviction candidates cannot satisfy {} pressured disks", result.deficits.size());
    cache::metrics::recordCount(cache::metrics::Event::MANAGER_EVICTION_RESULT, 1, {.reason = "no_candidates"});
    co_return result;
  }

  auto response = co_await backend_->beginEvict(std::move(items));
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != result.selected) {
    co_return makeError(CacheCode::kInvalidResponse, "invalid BeginEvict result count");
  }
  std::map<storage::PhysicalDiskId, uint64_t> scheduledRelease;
  for (size_t i = 0; i < response->results.size(); ++i) {
    const auto &item = response->results[i];
    if (!item.hasError()) {
      CO_RETURN_ON_ERROR(item->valid());
      ++result.begun;
      for (const auto &[disk, bytes] : candidates->at(selected->at(i)).physicalFootprintByDisk) {
        if (result.deficits.contains(disk)) scheduledRelease[disk] = saturatingAdd(scheduledRelease[disk], bytes);
      }
    } else if (item.error().code() == CacheCode::kStateConflict) {
      ++result.conflicts;
    } else {
      co_return makeError(item.error());
    }
  }
  result.targetMet = deficitsMet(scheduledRelease, result.deficits);
  if (!result.targetMet) {
    XLOGF(WARN,
          "Cache eviction selected {} candidates but did not cover {} physical disk deficits",
          result.selected,
          result.deficits.size());
  }
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_EVICTION_RESULT,
                              result.begun,
                              {.reason = result.targetMet ? "target_met" : "target_not_met"});
  co_return result;
}

}  // namespace hf3fs::cache_manager
