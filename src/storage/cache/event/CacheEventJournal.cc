#include "storage/cache/event/CacheEventJournal.h"

#include <fmt/format.h>
#include <limits>

namespace hf3fs::storage {
namespace {

constexpr std::string_view kHeaderKey = "phase2/cache-event/header";
constexpr std::string_view kOperationPrefix = "phase2/cache-event/operation/";
constexpr std::string_view kDeliveryPrefix = "phase2/cache-event/delivery/";

uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs) {
  return lhs + std::min(rhs, std::numeric_limits<uint64_t>::max() - lhs);
}

Status journalError(const Status &status, std::string_view action) {
  return Status(CacheCode::kJournalFull, fmt::format("{}: {}", action, status));
}

}  // namespace

Result<Void> CacheEventIntent::valid() const {
  RETURN_ON_ERROR(logicalKey.valid());
  RETURN_ON_ERROR(storageKey.valid());
  RETURN_ON_ERROR(placement.valid());
  RETURN_ON_ERROR(diskId.valid());
  if (storageOperationId == Uuid::zero() || storageTargetId == TargetId{} ||
      !std::binary_search(placement.expectedReplicaTargets.begin(),
                          placement.expectedReplicaTargets.end(),
                          storageTargetId) ||
      generation == cache::CacheGeneration{} || timestamp.isZero()) {
    return makeError(StatusCode::kInvalidArg, "invalid cache event intent identity");
  }
  if (type == cache::CacheStorageEventType::DELETED) {
    if (!logicalRetireOperationId || *logicalRetireOperationId == Uuid::zero() || !evictionEpoch ||
        *evictionEpoch == cache::EvictionEpoch{}) {
      return makeError(StatusCode::kInvalidArg, "logical deletion event is missing its eviction identity");
    }
  } else if (logicalRetireOperationId || evictionEpoch) {
    return makeError(StatusCode::kInvalidArg, "local cache event carries a logical retire identity");
  }
  return Void{};
}

Result<Void> CacheEventJournalRecord::valid() const {
  RETURN_ON_ERROR(intent.valid());
  if (accountedBytes == 0) return makeError(StatusCode::kDataCorruption, "cache event has no journal reservation");
  if (state == cache::CacheStorageEventState::PREPARED && sequence == 0) return Void{};
  if (state == cache::CacheStorageEventState::DELIVERABLE && sequence != 0) return Void{};
  return makeError(StatusCode::kDataCorruption, "invalid cache event journal state");
}

Result<Void> CacheEventEnvelope::valid() const {
  RETURN_ON_ERROR(sourceId.valid());
  RETURN_ON_ERROR(intent.valid());
  if (sequence == 0) return makeError(StatusCode::kDataCorruption, "cache event delivery sequence is zero");
  return Void{};
}

Result<Void> CacheEventJournal::Header::valid() const {
  RETURN_ON_ERROR(sourceId.valid());
  if (nextSequence == 0 || acknowledgedSequence >= nextSequence) {
    return makeError(StatusCode::kDataCorruption, "invalid cache event journal sequence header");
  }
  return Void{};
}

CacheEventJournal::CacheEventJournal(std::unique_ptr<kv::KVStore> store, size_t maxRecords, uint64_t maxBytes)
    : store_(std::move(store)),
      maxRecords_(maxRecords),
      maxBytes_(maxBytes) {}

std::string CacheEventJournal::operationKey(Uuid operationId) {
  return std::string{kOperationPrefix} + std::string{operationId.asStringView()};
}

std::string CacheEventJournal::deliveryKey(uint64_t sequence) {
  return std::string{kDeliveryPrefix} + fmt::format("{:020}", sequence);
}

uint64_t CacheEventJournal::estimateAccountedBytes(const PhysicalDiskId &sourceId, const CacheEventIntent &intent) {
  CacheEventJournalRecord record{intent,
                                 cache::CacheStorageEventState::DELIVERABLE,
                                 std::numeric_limits<uint64_t>::max(),
                                 std::numeric_limits<uint64_t>::max()};
  CacheEventEnvelope envelope{sourceId, std::numeric_limits<uint64_t>::max(), intent};
  auto bytes = saturatingAdd(operationKey(intent.storageOperationId).size(), serde::serializeBytes(record).size());
  bytes = saturatingAdd(bytes, deliveryKey(std::numeric_limits<uint64_t>::max()).size());
  return saturatingAdd(bytes, serde::serializeBytes(envelope).size());
}

Result<Void> CacheEventJournal::commitLocked(kv::KVStore::BatchOperations &batch) {
  auto result = batch.commit();
  if (result.hasError()) {
    journalFull_ = true;
    return makeError(journalError(result.error(), "cache event journal commit failed"));
  }
  return Void{};
}

Result<Void> CacheEventJournal::loadLocked() {
  operations_.clear();
  deliveries_.clear();
  accountedBytes_ = 0;

  auto operationLimit =
      static_cast<uint32_t>(std::min<size_t>(maxRecords_ < SIZE_MAX ? maxRecords_ + 1 : maxRecords_, UINT32_MAX));
  auto loaded = store_->iterateKeysWithPrefix(
      kOperationPrefix,
      operationLimit,
      [&](std::string_view key, std::string_view value) -> Result<Void> {
        CacheEventJournalRecord record;
        RETURN_ON_ERROR(serde::deserialize(record, value));
        RETURN_ON_ERROR(record.valid());
        if (key != operationKey(record.intent.storageOperationId)) {
          return makeError(StatusCode::kDataCorruption, "cache event operation key differs from its record");
        }
        if (record.accountedBytes < estimateAccountedBytes(header_.sourceId, record.intent)) {
          return makeError(StatusCode::kDataCorruption, "cache event journal reservation is too small");
        }
        if (!operations_.emplace(record.intent.storageOperationId, record).second) {
          return makeError(StatusCode::kDataCorruption, "duplicate cache event operation");
        }
        accountedBytes_ = saturatingAdd(accountedBytes_, record.accountedBytes);
        return Void{};
      });
  if (loaded.hasError()) return makeError(loaded.error());
  if (operations_.size() > maxRecords_ || accountedBytes_ > maxBytes_) {
    return makeError(CacheCode::kJournalFull, "cache event journal limit exceeded during recovery");
  }

  auto deliveryLimit =
      static_cast<uint32_t>(std::min<size_t>(maxRecords_ < SIZE_MAX ? maxRecords_ + 1 : maxRecords_, UINT32_MAX));
  loaded = store_->iterateKeysWithPrefix(
      kDeliveryPrefix,
      deliveryLimit,
      [&](std::string_view key, std::string_view value) -> Result<Void> {
        CacheEventEnvelope envelope;
        RETURN_ON_ERROR(serde::deserialize(envelope, value));
        RETURN_ON_ERROR(envelope.valid());
        if (key != deliveryKey(envelope.sequence) || envelope.sourceId != header_.sourceId ||
            !deliveries_.emplace(envelope.sequence, envelope).second) {
          return makeError(StatusCode::kDataCorruption, "invalid cache event delivery source or sequence");
        }
        return Void{};
      });
  if (loaded.hasError()) return makeError(loaded.error());

  for (uint64_t sequence = header_.acknowledgedSequence + 1; sequence < header_.nextSequence; ++sequence) {
    auto delivery = deliveries_.find(sequence);
    if (delivery == deliveries_.end()) return makeError(CacheCode::kEventGap, "cache event outbox has a sequence gap");
    auto operation = operations_.find(delivery->second.intent.storageOperationId);
    if (operation == operations_.end() || operation->second.state != cache::CacheStorageEventState::DELIVERABLE ||
        operation->second.sequence != sequence || operation->second.intent != delivery->second.intent) {
      return makeError(StatusCode::kDataCorruption, "cache event outbox differs from operation journal");
    }
  }
  for (const auto &[sequence, delivery] : deliveries_) {
    if (sequence <= header_.acknowledgedSequence || sequence >= header_.nextSequence) {
      return makeError(StatusCode::kDataCorruption, "cache event delivery is outside the persisted sequence range");
    }
    auto operation = operations_.find(delivery.intent.storageOperationId);
    if (operation == operations_.end() || operation->second.sequence != sequence) {
      return makeError(StatusCode::kDataCorruption, "cache event delivery has no matching operation");
    }
  }
  for (const auto &[_, operation] : operations_) {
    if (operation.state == cache::CacheStorageEventState::PREPARED) continue;
    auto delivery = deliveries_.find(operation.sequence);
    if (operation.sequence <= header_.acknowledgedSequence || operation.sequence >= header_.nextSequence ||
        delivery == deliveries_.end() || delivery->second.intent != operation.intent) {
      return makeError(StatusCode::kDataCorruption, "deliverable cache operation has no matching outbox entry");
    }
  }
  return Void{};
}

Result<Void> CacheEventJournal::init() {
  auto lock = std::unique_lock(mutex_);
  if (initialized_) return Void{};
  if (!store_ || maxRecords_ == 0 || maxBytes_ == 0) {
    return makeError(CacheCode::kJournalFull, "cache event journal is unavailable");
  }
  auto header = store_->get(kHeaderKey);
  if (header.hasError()) {
    if (header.error().code() != StatusCode::kKVStoreNotFound) {
      return makeError(journalError(header.error(), "cache event header load failed"));
    }
    header_.sourceId = PhysicalDiskId{Uuid::random()};
    auto persisted = store_->put(kHeaderKey, serde::serializeBytes(header_), true);
    if (persisted.hasError()) return makeError(journalError(persisted.error(), "cache event header create failed"));
  } else {
    auto decoded = serde::deserialize(header_, *header);
    if (decoded.hasError()) return makeError(journalError(decoded.error(), "cache event header decode failed"));
  }
  RETURN_ON_ERROR(header_.valid());
  RETURN_ON_ERROR(loadLocked());
  journalFull_ = operations_.size() + externalRecords_ >= maxRecords_ ||
                 saturatingAdd(accountedBytes_, externalBytes_) >= maxBytes_;
  initialized_ = true;
  return Void{};
}

Result<CacheEventJournalRecord> CacheEventJournal::prepare(const CacheEventIntent &intent) {
  RETURN_ON_ERROR(intent.valid());
  auto lock = std::unique_lock(mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  auto existing = operations_.find(intent.storageOperationId);
  if (existing != operations_.end()) {
    if (existing->second.intent != intent)
      return makeError(CacheCode::kStateConflict, "cache event operation identity was reused");
    return existing->second;
  }
  if (journalFull_) return makeError(CacheCode::kJournalFull, "cache event journal is fail-closed");
  auto accounted = estimateAccountedBytes(header_.sourceId, intent);
  auto usedBytes = saturatingAdd(accountedBytes_, externalBytes_);
  if (operations_.size() + externalRecords_ >= maxRecords_ || accounted > maxBytes_ - std::min(usedBytes, maxBytes_)) {
    journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "cache event journal reservation limit reached");
  }
  CacheEventJournalRecord record{intent, cache::CacheStorageEventState::PREPARED, 0, accounted};
  auto persisted = store_->put(operationKey(intent.storageOperationId), serde::serializeBytes(record), true);
  if (persisted.hasError()) {
    journalFull_ = true;
    return makeError(journalError(persisted.error(), "cache event prepare failed"));
  }
  operations_.emplace(intent.storageOperationId, record);
  accountedBytes_ += accounted;
  journalFull_ = operations_.size() + externalRecords_ >= maxRecords_ ||
                 saturatingAdd(accountedBytes_, externalBytes_) >= maxBytes_;
  return record;
}

Result<CacheEventEnvelope> CacheEventJournal::markDeliverable(Uuid storageOperationId) {
  if (storageOperationId == Uuid::zero()) return makeError(StatusCode::kInvalidArg, "cache event operation is zero");
  auto lock = std::unique_lock(mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  auto existing = operations_.find(storageOperationId);
  if (existing == operations_.end()) return makeError(CacheCode::kNotFound, "cache event operation is not prepared");
  if (existing->second.state == cache::CacheStorageEventState::DELIVERABLE) {
    return deliveries_.at(existing->second.sequence);
  }
  if (header_.nextSequence == std::numeric_limits<uint64_t>::max()) {
    journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "cache event sequence exhausted");
  }

  auto updated = existing->second;
  updated.state = cache::CacheStorageEventState::DELIVERABLE;
  updated.sequence = header_.nextSequence;
  CacheEventEnvelope envelope{header_.sourceId, updated.sequence, updated.intent};
  RETURN_ON_ERROR(updated.valid());
  RETURN_ON_ERROR(envelope.valid());
  auto nextHeader = header_;
  ++nextHeader.nextSequence;
  auto batch = store_->createBatchOps();
  if (!batch) {
    journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "cache event journal batch is unavailable");
  }
  batch->put(operationKey(storageOperationId), serde::serializeBytes(updated));
  batch->put(deliveryKey(envelope.sequence), serde::serializeBytes(envelope));
  batch->put(kHeaderKey, serde::serializeBytes(nextHeader));
  RETURN_ON_ERROR(commitLocked(*batch));
  existing->second = updated;
  deliveries_.emplace(envelope.sequence, envelope);
  header_ = nextHeader;
  return envelope;
}

Result<std::vector<CacheEventJournalRecord>> CacheEventJournal::prepared() const {
  auto lock = std::unique_lock(mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  std::vector<CacheEventJournalRecord> result;
  for (const auto &[_, record] : operations_) {
    if (record.state == cache::CacheStorageEventState::PREPARED) result.push_back(record);
  }
  return result;
}

Result<std::vector<CacheEventEnvelope>> CacheEventJournal::deliveryBatch(uint32_t limit) const {
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems)
    return makeError(CacheCode::kRequestTooLarge, "invalid cache event delivery batch limit");
  auto lock = std::unique_lock(mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  std::vector<CacheEventEnvelope> result;
  result.reserve(std::min<size_t>(limit, deliveries_.size()));
  auto sequence = header_.acknowledgedSequence + 1;
  while (result.size() < limit && sequence < header_.nextSequence) {
    auto found = deliveries_.find(sequence);
    if (found == deliveries_.end()) return makeError(CacheCode::kEventGap, "cache event delivery gap");
    result.push_back(found->second);
    ++sequence;
  }
  return result;
}

Result<Void> CacheEventJournal::acknowledge(uint64_t sequence) {
  auto lock = std::unique_lock(mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  if (sequence <= header_.acknowledgedSequence) return Void{};
  if (sequence >= header_.nextSequence) return makeError(CacheCode::kEventGap, "cache event ACK exceeds delivery");
  auto batch = store_->createBatchOps();
  if (!batch) {
    journalFull_ = true;
    return makeError(CacheCode::kJournalFull, "cache event ACK batch is unavailable");
  }
  std::vector<Uuid> operations;
  for (uint64_t current = header_.acknowledgedSequence + 1; current <= sequence; ++current) {
    auto delivery = deliveries_.find(current);
    if (delivery == deliveries_.end()) return makeError(CacheCode::kEventGap, "cache event ACK crosses a gap");
    operations.push_back(delivery->second.intent.storageOperationId);
    batch->remove(deliveryKey(current));
    batch->remove(operationKey(operations.back()));
  }
  auto nextHeader = header_;
  nextHeader.acknowledgedSequence = sequence;
  batch->put(kHeaderKey, serde::serializeBytes(nextHeader));
  RETURN_ON_ERROR(commitLocked(*batch));
  for (uint64_t current = header_.acknowledgedSequence + 1; current <= sequence; ++current) {
    deliveries_.erase(current);
  }
  for (const auto &operationId : operations) {
    auto operation = operations_.find(operationId);
    if (operation != operations_.end()) {
      accountedBytes_ =
          operation->second.accountedBytes > accountedBytes_ ? 0 : accountedBytes_ - operation->second.accountedBytes;
      operations_.erase(operation);
    }
  }
  header_ = nextHeader;
  journalFull_ = operations_.size() + externalRecords_ >= maxRecords_ ||
                 saturatingAdd(accountedBytes_, externalBytes_) >= maxBytes_;
  return Void{};
}

Result<Void> CacheEventJournal::requireWritable() const {
  auto lock = std::unique_lock(mutex_);
  if (!initialized_) return makeError(CacheCode::kUnavailable, "cache event journal is not initialized");
  if (journalFull_ || operations_.size() + externalRecords_ >= maxRecords_ ||
      saturatingAdd(accountedBytes_, externalBytes_) >= maxBytes_) {
    return makeError(CacheCode::kJournalFull, "cache event journal cannot reserve another operation");
  }
  return Void{};
}

CacheEventJournalStats CacheEventJournal::stats() const {
  auto lock = std::unique_lock(mutex_);
  CacheEventJournalStats result{header_.sourceId,
                                header_.nextSequence,
                                header_.acknowledgedSequence,
                                0,
                                deliveries_.size(),
                                saturatingAdd(accountedBytes_, externalBytes_),
                                initialized_ && !journalFull_ && operations_.size() + externalRecords_ < maxRecords_ &&
                                    saturatingAdd(accountedBytes_, externalBytes_) < maxBytes_};
  for (const auto &[_, record] : operations_)
    if (record.state == cache::CacheStorageEventState::PREPARED) ++result.prepared;
  return result;
}

}  // namespace hf3fs::storage
