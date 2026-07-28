#pragma once

#include "common/kv/ITransaction.h"
#include "common/serde/Serde.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

struct CacheCapacityRecord {
  SERDE_STRUCT_FIELD(logicalCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(usedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(reservedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(committedBytes, uint64_t{0});

 public:
  Result<Void> valid() const;
};

class CacheCapacityStore {
 public:
  static CoTryTask<CacheCapacityRecord> snapshotLoad(kv::IReadOnlyTransaction &txn);
  static CoTryTask<CacheCapacityRecord> load(kv::IReadWriteTransaction &txn);
  static CoTryTask<Void> setLogicalCapacity(kv::IReadWriteTransaction &txn, uint64_t logicalCapacity);
  static CoTryTask<Void> reserve(kv::IReadWriteTransaction &txn, uint64_t bytes);
  static CoTryTask<Void> commit(kv::IReadWriteTransaction &txn, uint64_t bytes);
  static CoTryTask<Void> release(kv::IReadWriteTransaction &txn, cache::ChargeKind kind, uint64_t bytes);

  static std::string key();

 private:
  static CoTryTask<Void> store(kv::IReadWriteTransaction &txn, const CacheCapacityRecord &record);
};

}  // namespace hf3fs::meta::server
