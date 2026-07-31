#include "storage/cache/eviction/LocalSafetyEvictor.h"
#include "storage/service/Components.h"

namespace hf3fs::storage {
namespace {

class StorageLocalSafetyBackend final : public LocalSafetyBackend {
 public:
  explicit StorageLocalSafetyBackend(Components &components)
      : components_(components) {}

  Result<std::vector<LocalSafetyDisk>> diskSpace() final {
    CHECK_RESULT(infos, components_.storageTargets.spaceInfos(true));
    std::vector<LocalSafetyDisk> result;
    for (const auto &info : infos) {
      if (info.storageRole == StorageRole::CACHE_ONLY)
        result.push_back(
            {info.physicalDiskId, info.cacheCapacityBytes, info.cachePhysicalUsedBytes, info.cacheReservedBytes});
    }
    return result;
  }

  Result<std::vector<LocalEvictionCandidate>> candidates(const PhysicalDiskId &diskId) final {
    std::vector<LocalEvictionCandidate> result;
    auto targets = components_.targetMap.snapshot();
    for (const auto &[_, target] : targets->getTargets()) {
      if (target.storageRole != StorageRole::CACHE_ONLY || target.physicalDiskId != diskId || !target.storageTarget)
        continue;
      CHECK_RESULT(items, target.storageTarget->listActiveCacheChunks());
      result.insert(result.end(), std::make_move_iterator(items.begin()), std::make_move_iterator(items.end()));
    }
    return result;
  }

  Result<bool> retire(const PhysicalDiskId &diskId, const LocalEvictionCandidate &candidate, Uuid operationId) final {
    CacheEventIntent intent;
    intent.type = cache::CacheStorageEventType::EMERGENCY_EVICTED;
    intent.storageOperationId = operationId;
    intent.logicalKey = candidate.descriptor.logicalKey;
    intent.storageKey = {candidate.descriptor.placement.versionedChain, candidate.chunkId};
    intent.storageTargetId = candidate.descriptor.targetId;
    intent.generation = candidate.descriptor.generation;
    intent.placement = candidate.descriptor.placement;
    intent.diskId = diskId;
    intent.timestamp = UtcClock::now();
    return retireIntent(intent, true);
  }

  Result<size_t> recoverPrepared() final {
    size_t recovered = 0;
    CHECK_RESULT(infos, components_.storageTargets.spaceInfos(false));
    for (const auto &info : infos) {
      auto journal = components_.storageTargets.cacheEventJournal(info.physicalDiskId);
      if (!journal) continue;
      CHECK_RESULT(records, journal->prepared());
      for (const auto &record : records) {
        if (record.intent.type != cache::CacheStorageEventType::EMERGENCY_EVICTED) continue;
        CHECK_RESULT(done, retireIntent(record.intent, false));
        if (done) ++recovered;
      }
    }
    return recovered;
  }

 private:
  Result<bool> retireIntent(const CacheEventIntent &intent, bool prepare) {
    auto journal = components_.storageTargets.cacheEventJournal(intent.diskId);
    if (!journal) return makeError(CacheCode::kUnavailable, "local eviction event journal is unavailable");
    if (prepare) RETURN_ON_ERROR(journal->prepare(intent));
    CHECK_RESULT(target, components_.targetMap.getByTargetId(intent.storageTargetId));
    if (!target->storageTarget || target->storageRole != StorageRole::CACHE_ONLY ||
        target->physicalDiskId != intent.diskId)
      return makeError(CacheCode::kPlacementMismatch, "local eviction target identity changed");
    RetireCacheChunkItem item{intent.storageKey, intent.generation, intent.storageOperationId};
    auto retired = target->storageTarget->retireCacheChunkDurable(item);
    if (retired.hasError() && retired.error().code() != CacheCode::kGenerationAdvanced &&
        retired.error().code() != CacheCode::kNotFound)
      return makeError(std::move(retired.error()));
    RETURN_ON_ERROR(journal->markDeliverable(intent.storageOperationId));
    return true;
  }

  Components &components_;
};

}  // namespace

std::shared_ptr<LocalSafetyBackend> createStorageLocalSafetyBackend(Components &components) {
  return std::make_shared<StorageLocalSafetyBackend>(components);
}

}  // namespace hf3fs::storage
