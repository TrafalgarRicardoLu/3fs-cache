#include "cache_manager/reconcile/StorageToMetadataChecker.h"

#include <algorithm>

namespace hf3fs::cache_manager {

bool StorageToMetadataChecker::forwardOwned(cache::CacheBlockState state) {
  return state == cache::CacheBlockState::LOADING || state == cache::CacheBlockState::READY ||
         state == cache::CacheBlockState::EVICTING || state == cache::CacheBlockState::CLEANING;
}

CoTask<void> StorageToMetadataChecker::retire(const storage::CacheInventoryEntry &entry,
                                              StorageToMetadataResult &result) {
  if (control_) {
    auto decision = control_->requestMutation();
    if (decision != ReconcileMutationDecision::ALLOW) {
      ++result.deferred;
      if (decision == ReconcileMutationDecision::STOP) result.stopped = true;
      co_return;
    }
  } else if (dryRun_) {
    ++result.deferred;
    co_return;
  }
  storage::RetireCacheReplicaItem item;
  item.key = entry.key;
  item.targetId = entry.targetId;
  item.expectedGeneration = entry.generation.cacheGeneration;
  item.placement = entry.descriptor.placement;
  item.evictionEpoch = cache::EvictionEpoch{1};
  item.operationId = Uuid::random();
  auto response = co_await backend_->retireCacheReplicas({item});
  if (response.hasError()) {
    if (response.error().code() == CacheCode::kGenerationAdvanced ||
        response.error().code() == CacheCode::kStateConflict) {
      ++result.conflicts;
    } else {
      ++result.retryable;
    }
    co_return;
  }
  if (response->results.size() != 1) {
    ++result.retryable;
    co_return;
  }
  const auto &retired = response->results.front();
  if (retired.hasError()) {
    if (retired.error().code() == CacheCode::kGenerationAdvanced ||
        retired.error().code() == CacheCode::kStateConflict) {
      ++result.conflicts;
    } else {
      ++result.retryable;
    }
  } else if (retired->operationId != item.operationId || !retired->durableRetired) {
    ++result.retryable;
  } else {
    ++result.retired;
  }
}

CoTryTask<StorageToMetadataResult> StorageToMetadataChecker::run(std::span<const TargetCacheInventory> inventories) {
  if (!backend_ || batchSize_ == 0 || batchSize_ > meta::kMaxCacheBatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid storage-to-metadata reconcile configuration");
  }

  std::vector<const storage::CacheInventoryEntry *> entries;
  for (const auto &inventory : inventories) {
    for (const auto &entry : inventory.entries) {
      auto valid = entry.valid();
      if (valid.hasError() || entry.targetId != inventory.targetId) {
        co_return makeError(CacheCode::kInvalidResponse, "invalid cache inventory entry");
      }
      entries.push_back(&entry);
    }
  }

  StorageToMetadataResult result;
  for (size_t begin = 0; begin < entries.size();) {
    if (control_ && control_->shouldStop()) {
      result.stopped = true;
      break;
    }
    std::vector<cache::CacheBlockKey> keys;
    size_t end = begin;
    while (end < entries.size() && keys.size() < batchSize_) {
      const auto &key = entries[end]->descriptor.logicalKey;
      if (std::find(keys.begin(), keys.end(), key) == keys.end()) keys.push_back(key);
      ++end;
    }
    auto response = co_await backend_->reconcileCacheBlocks(keys);
    CO_RETURN_ON_ERROR(response);
    if (response->results.size() != keys.size()) {
      co_return makeError(CacheCode::kInvalidResponse, "cache reconcile result count mismatch");
    }
    for (size_t index = begin; index < end; ++index) {
      if (control_ && control_->shouldStop()) {
        result.stopped = true;
        break;
      }
      const auto &entry = *entries[index];
      ++result.scanned;
      auto statusIndex = std::find(keys.begin(), keys.end(), entry.descriptor.logicalKey) - keys.begin();
      const auto &statusResult = response->results[statusIndex];
      if (statusResult.hasError()) {
        ++result.retryable;
        continue;
      }
      const auto &status = *statusResult;
      auto valid = status.valid();
      if (valid.hasError() || status.key != entry.descriptor.logicalKey) {
        co_return makeError(CacheCode::kInvalidResponse, "invalid Metadata cache reconcile status");
      }
      const auto storageGeneration = entry.generation.cacheGeneration;
      const auto metadataGeneration = status.cacheGeneration;
      if (status.state == cache::CacheBlockState::NONE || status.state == cache::CacheBlockState::FAILED) {
        ++result.orphans;
        co_await retire(entry, result);
      } else if (storageGeneration < metadataGeneration) {
        ++result.older;
        co_await retire(entry, result);
      } else if (storageGeneration > metadataGeneration) {
        ++result.newer;
        ++result.conflicts;
      } else if (forwardOwned(status.state)) {
        ++result.delegated;
      } else {
        ++result.conflicts;
      }
    }
    if (result.stopped) break;
    begin = end;
  }
  co_return result;
}

}  // namespace hf3fs::cache_manager
