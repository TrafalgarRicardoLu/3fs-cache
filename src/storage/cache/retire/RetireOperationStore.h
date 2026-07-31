#pragma once

#include <map>

#include "storage/cache/event/CacheEventJournal.h"

namespace hf3fs::storage {

enum class RetireOperationState : uint8_t {
  PREPARED = 1,
  COMPLETED = 2,
};

struct RetireOperation {
  SERDE_STRUCT_FIELD(item, CoordinateCacheRetireItem{});
  SERDE_STRUCT_FIELD(diskId, PhysicalDiskId{});
  SERDE_STRUCT_FIELD(eventTimestamp, UtcTime{});
  SERDE_STRUCT_FIELD(durableRetiredTargets, std::vector<TargetId>{});
  SERDE_STRUCT_FIELD(state, RetireOperationState::PREPARED);
  SERDE_STRUCT_FIELD(accountedBytes, uint64_t{0});

 public:
  Result<Void> valid() const;
  bool sameIdentity(const CoordinateCacheRetireItem &other, const PhysicalDiskId &otherDiskId) const;
  bool allReplicasDurableRetired() const;
};

class RetireOperationStore {
 public:
  explicit RetireOperationStore(CacheEventJournal &journal)
      : journal_(journal) {}

  Result<Void> init();
  Result<RetireOperation> prepare(const CoordinateCacheRetireItem &item,
                                  const PhysicalDiskId &diskId,
                                  UtcTime timestamp);
  Result<RetireOperation> acknowledge(Uuid operationId, TargetId targetId);
  Result<RetireOperation> complete(Uuid operationId);
  Result<RetireOperation> get(Uuid operationId) const;
  Result<std::vector<RetireOperation>> pending() const;
  static uint64_t estimateAccountedBytes(RetireOperation operation);

 private:
  static std::string operationKey(Uuid operationId);
  static CacheEventIntent eventIntent(const CoordinateCacheRetireItem &item,
                                      const PhysicalDiskId &diskId,
                                      UtcTime timestamp);
  CacheEventJournal &journal_;
  std::map<Uuid, RetireOperation> operations_;
  bool initialized_{false};
};

}  // namespace hf3fs::storage
