#include "cache_manager/reconcile/MetadataToStorageChecker.h"

#include <algorithm>
#include <folly/logging/xlog.h>

namespace hf3fs::cache_manager {

bool MetadataToStorageChecker::matches(const meta::ReconcileCacheBlockStatus &status,
                                       const CacheReplicaObservation &observation) {
  const auto &generation = observation.generation;
  const auto &descriptor = observation.descriptor;
  return status.ready && status.placement && generation.cacheGeneration == status.ready->cacheGeneration &&
         !generation.retired && generation.length == status.blockLength &&
         generation.checksum.type == static_cast<storage::ChecksumType>(status.ready->checksumType) &&
         generation.checksum.value == status.ready->checksumValue && descriptor.logicalKey == status.key &&
         descriptor.generation == status.ready->cacheGeneration && descriptor.placement == *status.placement &&
         std::binary_search(status.placement->expectedReplicaTargets.begin(),
                            status.placement->expectedReplicaTargets.end(),
                            descriptor.targetId);
}

bool MetadataToStorageChecker::definitelyMissing(const Status &error) {
  return error.code() == CacheCode::kNotFound || error.code() == StorageClientCode::kChunkNotFound;
}

bool MetadataToStorageChecker::definitelyMismatched(const Status &error) {
  return error.code() == CacheCode::kPlacementMismatch || error.code() == CacheCode::kInvalidResponse ||
         error.code() == StorageClientCode::kChecksumMismatch;
}

CoTask<void> MetadataToStorageChecker::repair(const meta::ReconcileCacheBlockStatus &status,
                                              std::optional<cache::CacheGeneration> observedGeneration,
                                              MetadataToStorageResult &result) {
  if (control_) {
    auto decision = control_->requestMutation();
    if (decision != ReconcileMutationDecision::ALLOW) {
      ++result.deferred;
      if (decision == ReconcileMutationDecision::STOP) result.stopped = true;
      co_return;
    }
  }
  meta::BeginCleanCacheBlockItem item{status.key,
                                      status.ready,
                                      observedGeneration,
                                      cache::CleanupTerminalState::REENQUEUE};
  auto repaired = co_await cleanup_.clean(std::move(item));
  if (repaired.hasValue()) {
    ++result.repaired;
  } else if (repaired.error().code() == CacheCode::kStateConflict || repaired.error().code() == CacheCode::kNotFound) {
    ++result.conflicts;
  } else {
    ++result.retryable;
    XLOGF(WARN,
          "Cache reconcile repair failed for inode {} block {}: {}",
          status.key.inode,
          status.key.block,
          repaired.error());
  }
}

CoTask<void> MetadataToStorageChecker::replay(const meta::ReconcileCacheBlockStatus &status,
                                              MetadataToStorageResult &result) {
  if (control_) {
    auto decision = control_->requestMutation();
    if (decision != ReconcileMutationDecision::ALLOW) {
      ++result.deferred;
      if (decision == ReconcileMutationDecision::STOP) result.stopped = true;
      co_return;
    }
  }
  if (status.state == cache::CacheBlockState::EVICTING) {
    meta::CacheEvictionIdentity identity{status.key,
                                         *status.ready,
                                         *status.placement,
                                         status.evictionEpoch,
                                         status.retireOperationId,
                                         status.evictionReason};
    auto replayed = co_await backend_->coordinateRetire(identity);
    if (replayed.hasValue()) {
      *replayed ? ++result.repaired : ++result.matched;
    } else if (replayed.error().code() == CacheCode::kStateConflict ||
               replayed.error().code() == CacheCode::kNotFound) {
      ++result.conflicts;
    } else {
      ++result.retryable;
    }
    co_return;
  }

  meta::BeginCleanCacheBlockItem item{
      status.key,
      status.ready,
      status.deleteGeneration == cache::CacheGeneration{} ? std::nullopt : std::optional{status.deleteGeneration},
      status.terminalState};
  auto replayed = co_await cleanup_.clean(std::move(item));
  if (replayed.hasValue()) {
    ++result.repaired;
  } else if (replayed.error().code() == CacheCode::kStateConflict || replayed.error().code() == CacheCode::kNotFound) {
    ++result.conflicts;
  } else {
    ++result.retryable;
  }
}

CoTryTask<MetadataToStorageResult> MetadataToStorageChecker::run() {
  if (!backend_ || pageSize_ == 0 || pageSize_ > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid metadata-to-storage reconcile configuration");
  }

  MetadataToStorageResult result;
  std::optional<cache::CacheBlockKey> after;
  bool more = true;
  while (more) {
    if (control_ && control_->shouldStop()) {
      result.stopped = true;
      break;
    }
    auto page = co_await backend_->listReconcileCacheBlocks(after, pageSize_);
    CO_RETURN_ON_ERROR(page);
    if (page->more && page->items.empty()) {
      co_return makeError(CacheCode::kInvalidResponse, "empty cache reconcile page has continuation");
    }
    for (const auto &status : page->items) {
      if (control_ && control_->shouldStop()) {
        result.stopped = true;
        break;
      }
      auto valid = status.valid();
      if (valid.hasError()) co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
      ++result.scanned;
      if (status.state == cache::CacheBlockState::EVICTING || status.state == cache::CacheBlockState::CLEANING) {
        co_await replay(status, result);
        continue;
      }
      if (status.state != cache::CacheBlockState::READY) {
        co_return makeError(CacheCode::kInvalidResponse, "metadata returned a non-reconcile cache state");
      }
      auto observation = co_await backend_->queryReconcile(status);
      if (observation.hasError()) {
        if (definitelyMissing(observation.error())) {
          ++result.missing;
          co_await repair(status, std::nullopt, result);
        } else if (definitelyMismatched(observation.error())) {
          ++result.mismatched;
          co_await repair(status, std::nullopt, result);
        } else {
          ++result.retryable;
        }
        continue;
      }
      if (matches(status, *observation)) {
        ++result.matched;
        continue;
      }
      ++result.mismatched;
      co_await repair(status, observation->generation.cacheGeneration, result);
    }
    more = page->more;
    if (result.stopped) break;
    if (more) after = page->items.back().key;
  }
  co_return result;
}

}  // namespace hf3fs::cache_manager
