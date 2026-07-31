#include "storage/cache/retire/RetireOperationStore.h"

#include <algorithm>
#include <limits>

namespace hf3fs::storage {
namespace {

constexpr std::string_view kRetireOperationPrefix = "phase2/cache-retire/operation/";

uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs) {
  return lhs + std::min(rhs, std::numeric_limits<uint64_t>::max() - lhs);
}

}  // namespace

Result<Void> RetireOperation::valid() const {
  RETURN_ON_ERROR(item.valid());
  RETURN_ON_ERROR(diskId.valid());
  if (eventTimestamp.isZero()) return makeError(StatusCode::kDataCorruption, "retire event timestamp is zero");
  if (!std::is_sorted(durableRetiredTargets.begin(), durableRetiredTargets.end()) ||
      std::adjacent_find(durableRetiredTargets.begin(), durableRetiredTargets.end()) != durableRetiredTargets.end()) {
    return makeError(StatusCode::kDataCorruption, "retire acknowledgements are not sorted and unique");
  }
  for (auto targetId : durableRetiredTargets) {
    if (!std::binary_search(item.placement.expectedReplicaTargets.begin(),
                            item.placement.expectedReplicaTargets.end(),
                            targetId)) {
      return makeError(StatusCode::kDataCorruption, "retire acknowledgement is outside recorded placement");
    }
  }
  if (state != RetireOperationState::PREPARED && state != RetireOperationState::COMPLETED) {
    return makeError(StatusCode::kDataCorruption, "invalid retire operation state");
  }
  if (state == RetireOperationState::COMPLETED && !allReplicasDurableRetired()) {
    return makeError(StatusCode::kDataCorruption, "completed retire operation is missing acknowledgements");
  }
  if (accountedBytes == 0 || accountedBytes < RetireOperationStore::estimateAccountedBytes(*this)) {
    return makeError(StatusCode::kDataCorruption, "retire operation reservation is too small");
  }
  return Void{};
}

bool RetireOperation::sameIdentity(const CoordinateCacheRetireItem &other, const PhysicalDiskId &otherDiskId) const {
  return item.logicalKey == other.logicalKey && item.key.vChainId == other.key.vChainId &&
         item.key.chunkId == other.key.chunkId && item.expectedGeneration == other.expectedGeneration &&
         item.placement == other.placement && item.evictionEpoch == other.evictionEpoch &&
         item.operationId == other.operationId && diskId == otherDiskId;
}

bool RetireOperation::allReplicasDurableRetired() const {
  return durableRetiredTargets == item.placement.expectedReplicaTargets;
}

std::string RetireOperationStore::operationKey(Uuid operationId) {
  return std::string{kRetireOperationPrefix} + std::string{operationId.asStringView()};
}

CacheEventIntent RetireOperationStore::eventIntent(const CoordinateCacheRetireItem &item,
                                                   const PhysicalDiskId &diskId,
                                                   UtcTime timestamp) {
  CacheEventIntent intent;
  intent.type = cache::CacheStorageEventType::DELETED;
  intent.storageOperationId = item.operationId;
  intent.logicalRetireOperationId = item.operationId;
  intent.logicalKey = item.logicalKey;
  intent.storageKey = item.key;
  intent.generation = item.expectedGeneration;
  intent.placement = item.placement;
  intent.evictionEpoch = item.evictionEpoch;
  intent.diskId = diskId;
  intent.timestamp = timestamp;
  return intent;
}

uint64_t RetireOperationStore::estimateAccountedBytes(RetireOperation operation) {
  operation.durableRetiredTargets = operation.item.placement.expectedReplicaTargets;
  operation.state = RetireOperationState::COMPLETED;
  operation.accountedBytes = std::numeric_limits<uint64_t>::max();
  return saturatingAdd(operationKey(operation.item.operationId).size(), serde::serializeBytes(operation).size());
}

Result<Void> RetireOperationStore::init() {
  auto lock = std::unique_lock(journal_.mutex_);
  if (initialized_) return Void{};
  if (!journal_.initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  operations_.clear();
  journal_.externalRecords_ = 0;
  journal_.externalBytes_ = 0;
  auto limit = static_cast<uint32_t>(
      std::min<size_t>(journal_.maxRecords_ < SIZE_MAX ? journal_.maxRecords_ + 1 : journal_.maxRecords_, UINT32_MAX));
  auto loaded = journal_.store_->iterateKeysWithPrefix(
      kRetireOperationPrefix,
      limit,
      [&](std::string_view key, std::string_view value) -> Result<Void> {
        RetireOperation operation;
        RETURN_ON_ERROR(serde::deserialize(operation, value));
        RETURN_ON_ERROR(operation.valid());
        if (key != operationKey(operation.item.operationId) ||
            !operations_.emplace(operation.item.operationId, operation).second) {
          return makeError(StatusCode::kDataCorruption, "invalid or duplicate retire operation");
        }
        auto event = journal_.operations_.find(operation.item.operationId);
        if (operation.state == RetireOperationState::PREPARED &&
            (event == journal_.operations_.end() || event->second.state != cache::CacheStorageEventState::PREPARED)) {
          return makeError(StatusCode::kDataCorruption, "prepared retire operation has no prepared event");
        }
        if (operation.state == RetireOperationState::COMPLETED && event != journal_.operations_.end() &&
            event->second.state != cache::CacheStorageEventState::DELIVERABLE) {
          return makeError(StatusCode::kDataCorruption, "completed retire operation has a non-deliverable event");
        }
        ++journal_.externalRecords_;
        journal_.externalBytes_ = saturatingAdd(journal_.externalBytes_, operation.accountedBytes);
        return Void{};
      });
  RETURN_ON_ERROR(loaded);
  if (journal_.operations_.size() + journal_.externalRecords_ > journal_.maxRecords_ ||
      saturatingAdd(journal_.accountedBytes_, journal_.externalBytes_) > journal_.maxBytes_) {
    return makeError(CacheCode::kJournalFull, "retire operation store limit exceeded during recovery");
  }
  journal_.journalFull_ = journal_.operations_.size() + journal_.externalRecords_ >= journal_.maxRecords_ ||
                          saturatingAdd(journal_.accountedBytes_, journal_.externalBytes_) >= journal_.maxBytes_;
  initialized_ = true;
  return Void{};
}

Result<RetireOperation> RetireOperationStore::prepare(const CoordinateCacheRetireItem &item,
                                                      const PhysicalDiskId &diskId,
                                                      UtcTime timestamp) {
  RETURN_ON_ERROR(item.valid());
  RETURN_ON_ERROR(diskId.valid());
  if (timestamp.isZero()) return makeError(StatusCode::kInvalidArg, "retire timestamp is zero");
  auto intent = eventIntent(item, diskId, timestamp);
  RETURN_ON_ERROR(intent.valid());

  auto lock = std::unique_lock(journal_.mutex_);
  if (!initialized_ || !journal_.initialized_)
    return makeError(CacheCode::kUnavailable, "retire operation store is not initialized");
  auto existing = operations_.find(item.operationId);
  if (existing != operations_.end()) {
    if (!existing->second.sameIdentity(item, diskId)) {
      return makeError(CacheCode::kStateConflict, "retire operation identity was reused");
    }
    return existing->second;
  }
  if (journal_.maxRecords_ < 2 || journal_.operations_.size() + journal_.externalRecords_ > journal_.maxRecords_ - 2 ||
      journal_.journalFull_) {
    return makeError(CacheCode::kJournalFull, "retire operation store is full");
  }
  if (journal_.operations_.find(item.operationId) != journal_.operations_.end()) {
    return makeError(CacheCode::kStateConflict, "retire event operation identity was reused");
  }
  auto accounted = CacheEventJournal::estimateAccountedBytes(journal_.header_.sourceId, intent);
  RetireOperation operation{item, diskId, timestamp, {}, RetireOperationState::PREPARED, 0};
  operation.accountedBytes = estimateAccountedBytes(operation);
  auto totalReservation = saturatingAdd(accounted, operation.accountedBytes);
  auto usedBytes = saturatingAdd(journal_.accountedBytes_, journal_.externalBytes_);
  if (totalReservation > journal_.maxBytes_ - std::min(usedBytes, journal_.maxBytes_)) {
    journal_.journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "retire event reservation limit reached");
  }

  CacheEventJournalRecord event{intent, cache::CacheStorageEventState::PREPARED, 0, accounted};
  RETURN_ON_ERROR(operation.valid());
  RETURN_ON_ERROR(event.valid());
  auto batch = journal_.store_->createBatchOps();
  if (!batch) {
    journal_.journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "retire prepare batch is unavailable");
  }
  batch->put(operationKey(item.operationId), serde::serializeBytes(operation));
  batch->put(CacheEventJournal::operationKey(item.operationId), serde::serializeBytes(event));
  RETURN_ON_ERROR(journal_.commitLocked(*batch));
  operations_.emplace(item.operationId, operation);
  ++journal_.externalRecords_;
  journal_.externalBytes_ = saturatingAdd(journal_.externalBytes_, operation.accountedBytes);
  journal_.operations_.emplace(item.operationId, event);
  journal_.accountedBytes_ += accounted;
  journal_.journalFull_ = journal_.operations_.size() + journal_.externalRecords_ >= journal_.maxRecords_ ||
                          saturatingAdd(journal_.accountedBytes_, journal_.externalBytes_) >= journal_.maxBytes_;
  return operation;
}

Result<RetireOperation> RetireOperationStore::acknowledge(Uuid operationId, TargetId targetId) {
  auto lock = std::unique_lock(journal_.mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "retire operation store is not initialized");
  auto existing = operations_.find(operationId);
  if (existing == operations_.end()) return makeError(CacheCode::kNotFound, "retire operation is not prepared");
  if (!std::binary_search(existing->second.item.placement.expectedReplicaTargets.begin(),
                          existing->second.item.placement.expectedReplicaTargets.end(),
                          targetId)) {
    return makeError(CacheCode::kPlacementMismatch, "retire ACK is outside recorded placement");
  }
  if (std::binary_search(existing->second.durableRetiredTargets.begin(),
                         existing->second.durableRetiredTargets.end(),
                         targetId)) {
    return existing->second;
  }
  if (existing->second.state == RetireOperationState::COMPLETED) {
    return makeError(StatusCode::kDataCorruption, "completed retire operation is missing a persisted ACK");
  }
  auto updated = existing->second;
  updated.durableRetiredTargets.insert(
      std::lower_bound(updated.durableRetiredTargets.begin(), updated.durableRetiredTargets.end(), targetId),
      targetId);
  RETURN_ON_ERROR(updated.valid());
  auto persisted = journal_.store_->put(operationKey(operationId), serde::serializeBytes(updated), true);
  if (persisted.hasError()) {
    journal_.journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "retire ACK persistence failed");
  }
  existing->second = updated;
  return updated;
}

Result<RetireOperation> RetireOperationStore::complete(Uuid operationId) {
  auto lock = std::unique_lock(journal_.mutex_);
  if (!initialized_ || !journal_.initialized_)
    return makeError(CacheCode::kUnavailable, "retire operation store is not initialized");
  auto existing = operations_.find(operationId);
  if (existing == operations_.end()) return makeError(CacheCode::kNotFound, "retire operation is not prepared");
  if (existing->second.state == RetireOperationState::COMPLETED) return existing->second;
  if (!existing->second.allReplicasDurableRetired()) {
    return makeError(CacheCode::kUnavailable, "recorded replicas are not all durably retired");
  }
  auto event = journal_.operations_.find(operationId);
  if (event == journal_.operations_.end() || event->second.state != cache::CacheStorageEventState::PREPARED) {
    return makeError(StatusCode::kDataCorruption, "retire completion has no prepared event");
  }
  if (journal_.header_.nextSequence == std::numeric_limits<uint64_t>::max()) {
    journal_.journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "cache event sequence exhausted");
  }

  auto completed = existing->second;
  completed.state = RetireOperationState::COMPLETED;
  auto deliverable = event->second;
  deliverable.state = cache::CacheStorageEventState::DELIVERABLE;
  deliverable.sequence = journal_.header_.nextSequence;
  CacheEventEnvelope envelope{journal_.header_.sourceId, deliverable.sequence, deliverable.intent};
  auto nextHeader = journal_.header_;
  ++nextHeader.nextSequence;
  RETURN_ON_ERROR(completed.valid());
  RETURN_ON_ERROR(deliverable.valid());
  RETURN_ON_ERROR(envelope.valid());

  auto batch = journal_.store_->createBatchOps();
  if (!batch) {
    journal_.journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "retire completion batch is unavailable");
  }
  batch->put(operationKey(operationId), serde::serializeBytes(completed));
  batch->put(CacheEventJournal::operationKey(operationId), serde::serializeBytes(deliverable));
  batch->put(CacheEventJournal::deliveryKey(envelope.sequence), serde::serializeBytes(envelope));
  batch->put("phase2/cache-event/header", serde::serializeBytes(nextHeader));
  RETURN_ON_ERROR(journal_.commitLocked(*batch));
  existing->second = completed;
  event->second = deliverable;
  journal_.deliveries_.emplace(envelope.sequence, envelope);
  journal_.header_ = nextHeader;
  return completed;
}

Result<RetireOperation> RetireOperationStore::get(Uuid operationId) const {
  auto lock = std::unique_lock(journal_.mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "retire operation store is not initialized");
  auto existing = operations_.find(operationId);
  if (existing == operations_.end()) return makeError(CacheCode::kNotFound);
  return existing->second;
}

Result<std::vector<RetireOperation>> RetireOperationStore::pending() const {
  auto lock = std::unique_lock(journal_.mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "retire operation store is not initialized");
  std::vector<RetireOperation> result;
  for (const auto &[_, operation] : operations_) {
    if (operation.state == RetireOperationState::PREPARED) result.push_back(operation);
  }
  return result;
}

}  // namespace hf3fs::storage
