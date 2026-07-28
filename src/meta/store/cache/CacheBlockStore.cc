#include "meta/store/cache/CacheBlockStore.h"

#include <limits>

#include "common/kv/KeyPrefix.h"
#include "common/serde/Serde.h"
#include "common/utils/SerDeser.h"
#include "meta/store/cache/CacheCapacityStore.h"

namespace hf3fs::meta::server {
namespace {

template <typename Transaction>
CoTryTask<std::optional<CacheBlockRecord>> loadRecord(Transaction &txn,
                                                      const cache::CacheBlockKey &key,
                                                      bool snapshot) {
  CO_RETURN_ON_ERROR(key.valid());
  auto packedKey = CacheBlockStore::recordKey(key);
  auto value = snapshot ? co_await txn.snapshotGet(packedKey) : co_await txn.get(packedKey);
  CO_RETURN_ON_ERROR(value);
  if (!value->has_value()) co_return std::nullopt;

  CacheBlockRecord record;
  auto deserialized = serde::deserialize(record, **value);
  if (deserialized.hasError()) co_return makeError(StatusCode::kDataCorruption, "invalid cache block record");
  auto valid = record.valid();
  if (valid.hasError() || record.key != key) {
    co_return makeError(StatusCode::kDataCorruption, "cache block record key or value mismatch");
  }
  co_return record;
}

}  // namespace

std::string CacheBlockStore::recordKey(const cache::CacheBlockKey &key) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{0}, key.inode, key.block.toUnderType());
}

std::string CacheBlockStore::generationKey(const cache::CacheBlockKey &key) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{1}, key.inode, key.block.toUnderType());
}

CoTryTask<std::optional<CacheBlockRecord>> CacheBlockStore::snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                         const cache::CacheBlockKey &key) {
  co_return co_await loadRecord(txn, key, true);
}

CoTryTask<std::optional<CacheBlockRecord>> CacheBlockStore::load(kv::IReadWriteTransaction &txn,
                                                                 const cache::CacheBlockKey &key) {
  co_return co_await loadRecord(txn, key, false);
}

CoTryTask<Void> CacheBlockStore::store(kv::IReadWriteTransaction &txn, const CacheBlockRecord &record) {
  CO_RETURN_ON_ERROR(record.valid());
  co_return co_await txn.set(recordKey(record.key), serde::serialize(record));
}

CoTryTask<Void> CacheBlockStore::remove(kv::IReadWriteTransaction &txn, const cache::CacheBlockKey &key) {
  CO_RETURN_ON_ERROR(key.valid());
  co_return co_await txn.clear(recordKey(key));
}

CoTryTask<CacheBlockRecord> CacheBlockStore::enqueue(kv::IReadWriteTransaction &txn,
                                                     const cache::CacheBlockKey &key,
                                                     flat::ChainId chainId,
                                                     uint64_t blockLength) {
  if (blockLength == 0) co_return makeError(StatusCode::kInvalidArg, "block length is zero");
  auto existing = co_await load(txn, key);
  CO_RETURN_ON_ERROR(existing);
  if (existing->has_value() && (*existing)->state != cache::CacheBlockState::FAILED) {
    if ((*existing)->chainId != chainId || (*existing)->blockLength != blockLength) {
      co_return makeError(CacheCode::kStateConflict, "cache block layout changed");
    }
    co_return **existing;
  }

  CO_RETURN_ON_ERROR(co_await CacheCapacityStore::reserve(txn, blockLength));
  CacheBlockRecord record;
  record.key = key;
  record.state = cache::CacheBlockState::QUEUED;
  record.chainId = chainId;
  record.blockLength = blockLength;
  record.chargeKind = cache::ChargeKind::RESERVED;
  record.chargedBytes = blockLength;
  CO_RETURN_ON_ERROR(co_await store(txn, record));
  co_return record;
}

CoTryTask<cache::CacheGeneration> CacheBlockStore::allocateGeneration(kv::IReadWriteTransaction &txn,
                                                                      const cache::CacheBlockKey &key) {
  CO_RETURN_ON_ERROR(key.valid());
  auto packedKey = generationKey(key);
  auto value = co_await txn.get(packedKey);
  CO_RETURN_ON_ERROR(value);
  cache::CacheGeneration generation;
  if (value->has_value()) {
    auto deserialized = serde::deserialize(generation, **value);
    if (deserialized.hasError()) co_return makeError(StatusCode::kDataCorruption, "invalid cache generation");
  }
  if (generation.toUnderType() == std::numeric_limits<uint64_t>::max()) {
    co_return makeError(CacheCode::kStateConflict, "cache generation exhausted");
  }
  ++generation;
  CO_RETURN_ON_ERROR(co_await txn.set(packedKey, serde::serialize(generation)));
  co_return generation;
}

CoTryTask<CacheBlockRecord> CacheBlockStore::commitCharge(kv::IReadWriteTransaction &txn,
                                                          const CacheBlockRecord &desiredRecord) {
  auto loaded = co_await load(txn, desiredRecord.key);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kStateConflict, "cache block not found");
  auto record = std::move(**loaded);
  if (record.chargeKind == cache::ChargeKind::COMMITTED) co_return record;
  if (record.chargeKind != cache::ChargeKind::RESERVED || record.chargedBytes == 0) {
    co_return makeError(CacheCode::kStateConflict, "cache block has no reserved charge");
  }
  if (desiredRecord.state != cache::CacheBlockState::READY || desiredRecord.blockLength != record.blockLength ||
      desiredRecord.chainId != record.chainId || desiredRecord.chargedBytes != record.chargedBytes) {
    co_return makeError(CacheCode::kStateConflict, "invalid committed cache block record");
  }
  CO_RETURN_ON_ERROR(co_await CacheCapacityStore::commit(txn, record.chargedBytes));
  auto committed = desiredRecord;
  committed.chargeKind = cache::ChargeKind::COMMITTED;
  CO_RETURN_ON_ERROR(co_await store(txn, committed));
  co_return committed;
}

CoTryTask<Void> CacheBlockStore::finishClean(kv::IReadWriteTransaction &txn,
                                             const cache::CacheBlockKey &key,
                                             cache::CleanupTerminalState terminalState) {
  if (terminalState != cache::CleanupTerminalState::NONE && terminalState != cache::CleanupTerminalState::FAILED &&
      terminalState != cache::CleanupTerminalState::REENQUEUE) {
    co_return makeError(StatusCode::kInvalidArg, "invalid cleanup terminal state");
  }
  auto loaded = co_await load(txn, key);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) {
    if (terminalState == cache::CleanupTerminalState::NONE) co_return Void{};
    co_return makeError(CacheCode::kStateConflict, "cache block not found");
  }
  auto record = std::move(**loaded);
  if (record.state == cache::CacheBlockState::FAILED && terminalState == cache::CleanupTerminalState::FAILED) {
    co_return Void{};
  }
  if (record.state == cache::CacheBlockState::QUEUED && terminalState == cache::CleanupTerminalState::REENQUEUE) {
    co_return Void{};
  }
  if (record.state != cache::CacheBlockState::CLEANING) {
    co_return makeError(CacheCode::kStateConflict, "cache block is not cleaning");
  }
  auto chainId = record.chainId;
  auto blockLength = record.blockLength;
  if (record.chargeKind != cache::ChargeKind::NONE) {
    CO_RETURN_ON_ERROR(co_await CacheCapacityStore::release(txn, record.chargeKind, record.chargedBytes));
  }
  record.chargeKind = cache::ChargeKind::NONE;
  record.chargedBytes = 0;
  record.ready.reset();
  record.loaderId = Uuid::zero();
  record.terminalState = terminalState;

  if (terminalState == cache::CleanupTerminalState::NONE) {
    co_return co_await remove(txn, record.key);
  }

  record.state = cache::CacheBlockState::FAILED;
  CO_RETURN_ON_ERROR(co_await store(txn, record));
  if (terminalState == cache::CleanupTerminalState::REENQUEUE) {
    auto requeued = co_await enqueue(txn, key, chainId, blockLength);
    CO_RETURN_ON_ERROR(requeued);
  }
  co_return Void{};
}

}  // namespace hf3fs::meta::server
