#pragma once

#include <map>
#include <memory>
#include <mutex>

#include "fbs/storage/Cache.h"
#include "kv/KVStore.h"

namespace hf3fs::storage {

struct CacheEventIntent {
  SERDE_STRUCT_FIELD(type, cache::CacheStorageEventType::DELETED);
  SERDE_STRUCT_FIELD(storageOperationId, Uuid::zero());
  SERDE_STRUCT_FIELD(logicalRetireOperationId, std::optional<Uuid>{});
  SERDE_STRUCT_FIELD(logicalKey, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(storageKey, CacheChunkKey{});
  SERDE_STRUCT_FIELD(generation, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(placement, PlacementIdentity{});
  SERDE_STRUCT_FIELD(evictionEpoch, std::optional<cache::EvictionEpoch>{});
  SERDE_STRUCT_FIELD(diskId, PhysicalDiskId{});
  SERDE_STRUCT_FIELD(timestamp, UtcTime{});

 public:
  Result<Void> valid() const;
  bool operator==(const CacheEventIntent &other) const {
    return type == other.type && storageOperationId == other.storageOperationId &&
           logicalRetireOperationId == other.logicalRetireOperationId && logicalKey == other.logicalKey &&
           storageKey.vChainId == other.storageKey.vChainId && storageKey.chunkId == other.storageKey.chunkId &&
           generation == other.generation && placement == other.placement && evictionEpoch == other.evictionEpoch &&
           diskId == other.diskId && timestamp == other.timestamp;
  }
};

struct CacheEventJournalRecord {
  SERDE_STRUCT_FIELD(intent, CacheEventIntent{});
  SERDE_STRUCT_FIELD(state, cache::CacheStorageEventState::INVALID);
  SERDE_STRUCT_FIELD(sequence, uint64_t{0});
  SERDE_STRUCT_FIELD(accountedBytes, uint64_t{0});

 public:
  Result<Void> valid() const;
  bool operator==(const CacheEventJournalRecord &) const = default;
};

struct CacheEventEnvelope {
  SERDE_STRUCT_FIELD(sourceId, PhysicalDiskId{});
  SERDE_STRUCT_FIELD(sequence, uint64_t{0});
  SERDE_STRUCT_FIELD(intent, CacheEventIntent{});

 public:
  Result<Void> valid() const;
  bool operator==(const CacheEventEnvelope &) const = default;
};

struct CacheEventJournalStats {
  PhysicalDiskId sourceId;
  uint64_t nextSequence{0};
  uint64_t acknowledgedSequence{0};
  size_t prepared{0};
  size_t deliverable{0};
  uint64_t accountedBytes{0};
  bool writable{false};
};

class CacheEventJournal {
 public:
  CacheEventJournal(std::unique_ptr<kv::KVStore> store, size_t maxRecords, uint64_t maxBytes);

  Result<Void> init();
  Result<CacheEventJournalRecord> prepare(const CacheEventIntent &intent);
  Result<CacheEventEnvelope> markDeliverable(Uuid storageOperationId);
  Result<std::vector<CacheEventJournalRecord>> prepared() const;
  Result<std::vector<CacheEventEnvelope>> deliveryBatch(uint32_t limit) const;
  Result<Void> acknowledge(uint64_t sequence);
  Result<Void> requireWritable() const;
  CacheEventJournalStats stats() const;

 private:
  struct Header {
    SERDE_STRUCT_FIELD(sourceId, PhysicalDiskId{});
    SERDE_STRUCT_FIELD(nextSequence, uint64_t{1});
    SERDE_STRUCT_FIELD(acknowledgedSequence, uint64_t{0});

   public:
    Result<Void> valid() const;
  };

  static std::string operationKey(Uuid operationId);
  static std::string deliveryKey(uint64_t sequence);
  static uint64_t estimateAccountedBytes(const PhysicalDiskId &sourceId, const CacheEventIntent &intent);
  Result<Void> loadLocked();
  Result<Void> commitLocked(kv::KVStore::BatchOperations &batch);

  std::unique_ptr<kv::KVStore> store_;
  const size_t maxRecords_;
  const uint64_t maxBytes_;
  mutable std::mutex mutex_;
  Header header_;
  std::map<Uuid, CacheEventJournalRecord> operations_;
  std::map<uint64_t, CacheEventEnvelope> deliveries_;
  uint64_t accountedBytes_{0};
  bool initialized_{false};
  bool journalFull_{false};
};

}  // namespace hf3fs::storage
