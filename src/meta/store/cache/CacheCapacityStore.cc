#include "meta/store/cache/CacheCapacityStore.h"

#include <limits>

#include "common/kv/KeyPrefix.h"
#include "common/utils/SerDeser.h"

namespace hf3fs::meta::server {
namespace {

template <typename Transaction>
CoTryTask<CacheCapacityRecord> loadCapacity(Transaction &txn, bool snapshot) {
  auto value =
      snapshot ? co_await txn.snapshotGet(CacheCapacityStore::key()) : co_await txn.get(CacheCapacityStore::key());
  CO_RETURN_ON_ERROR(value);
  CacheCapacityRecord record;
  if (value->has_value()) {
    auto deserialized = serde::deserialize(record, **value);
    if (deserialized.hasError()) co_return makeError(StatusCode::kDataCorruption, "invalid cache capacity record");
    auto valid = record.valid();
    if (valid.hasError()) co_return makeError(StatusCode::kDataCorruption, valid.error().message());
  }
  co_return record;
}

bool addOverflows(uint64_t lhs, uint64_t rhs) { return rhs > std::numeric_limits<uint64_t>::max() - lhs; }

}  // namespace

Result<Void> CacheCapacityRecord::valid() const {
  if (addOverflows(reservedBytes, committedBytes) || reservedBytes + committedBytes != usedBytes) {
    return makeError(StatusCode::kInvalidArg, "cache capacity counters are inconsistent");
  }
  if (usedBytes > logicalCapacity) return makeError(StatusCode::kInvalidArg, "cache capacity is overcommitted");
  return Void{};
}

std::string CacheCapacityStore::key() { return Serializer::serRawArgs(kv::KeyPrefix::CacheCapacity); }

CoTryTask<CacheCapacityRecord> CacheCapacityStore::snapshotLoad(kv::IReadOnlyTransaction &txn) {
  co_return co_await loadCapacity(txn, true);
}

CoTryTask<CacheCapacityRecord> CacheCapacityStore::load(kv::IReadWriteTransaction &txn) {
  co_return co_await loadCapacity(txn, false);
}

CoTryTask<Void> CacheCapacityStore::store(kv::IReadWriteTransaction &txn, const CacheCapacityRecord &record) {
  CO_RETURN_ON_ERROR(record.valid());
  co_return co_await txn.set(key(), serde::serialize(record));
}

CoTryTask<Void> CacheCapacityStore::setLogicalCapacity(kv::IReadWriteTransaction &txn, uint64_t logicalCapacity) {
  auto record = co_await load(txn);
  CO_RETURN_ON_ERROR(record);
  if (logicalCapacity < record->usedBytes) co_return makeError(CacheCode::kCapacityExceeded, "capacity below usage");
  record->logicalCapacity = logicalCapacity;
  co_return co_await store(txn, *record);
}

CoTryTask<Void> CacheCapacityStore::reserve(kv::IReadWriteTransaction &txn, uint64_t bytes) {
  if (bytes == 0) co_return makeError(StatusCode::kInvalidArg, "cannot reserve zero bytes");
  auto record = co_await load(txn);
  CO_RETURN_ON_ERROR(record);
  if (addOverflows(record->usedBytes, bytes) || record->usedBytes + bytes > record->logicalCapacity) {
    co_return makeError(CacheCode::kCapacityExceeded, "cache capacity exceeded");
  }
  record->usedBytes += bytes;
  record->reservedBytes += bytes;
  co_return co_await store(txn, *record);
}

CoTryTask<Void> CacheCapacityStore::commit(kv::IReadWriteTransaction &txn, uint64_t bytes) {
  auto record = co_await load(txn);
  CO_RETURN_ON_ERROR(record);
  if (bytes == 0 || record->reservedBytes < bytes || addOverflows(record->committedBytes, bytes)) {
    co_return makeError(CacheCode::kStateConflict, "invalid reserved charge commit");
  }
  record->reservedBytes -= bytes;
  record->committedBytes += bytes;
  co_return co_await store(txn, *record);
}

CoTryTask<Void> CacheCapacityStore::release(kv::IReadWriteTransaction &txn, cache::ChargeKind kind, uint64_t bytes) {
  if (kind == cache::ChargeKind::NONE || bytes == 0) {
    co_return makeError(CacheCode::kStateConflict, "invalid cache charge release");
  }
  auto record = co_await load(txn);
  CO_RETURN_ON_ERROR(record);
  auto &counter = kind == cache::ChargeKind::RESERVED ? record->reservedBytes : record->committedBytes;
  if (counter < bytes || record->usedBytes < bytes) {
    co_return makeError(CacheCode::kStateConflict, "cache charge already released");
  }
  counter -= bytes;
  record->usedBytes -= bytes;
  co_return co_await store(txn, *record);
}

}  // namespace hf3fs::meta::server
