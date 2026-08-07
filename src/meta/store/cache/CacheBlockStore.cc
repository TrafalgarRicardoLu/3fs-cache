#include "meta/store/cache/CacheBlockStore.h"

#include <folly/experimental/coro/Collect.h>
#include <folly/logging/xlog.h>
#include <limits>

#include "cache/metrics/CacheMetrics.h"
#include "common/kv/KeyPrefix.h"
#include "common/serde/Serde.h"
#include "common/utils/MagicEnum.hpp"
#include "common/utils/SerDeser.h"
#include "meta/store/cache/CacheCapacityStore.h"

namespace hf3fs::meta::server {
namespace {

constexpr int32_t kRecoverableScanBatch = 1000;

Result<CacheBlockRecord> decodeRecord(const kv::IReadOnlyTransaction::KeyValue &kv) {
  CacheBlockRecord record;
  auto deserialized = serde::deserialize(record, kv.value);
  if (deserialized.hasError() || record.valid().hasError() || CacheBlockStore::recordKey(record.key) != kv.key) {
    return makeError(StatusCode::kDataCorruption, "invalid cache block record in range");
  }
  return record;
}

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

std::string CacheBlockStore::evictionEpochKey(const cache::CacheBlockKey &key) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{2}, key.inode, key.block.toUnderType());
}

CoTryTask<std::optional<CacheBlockRecord>> CacheBlockStore::snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                         const cache::CacheBlockKey &key) {
  co_return co_await loadRecord(txn, key, true);
}

CoTryTask<std::vector<std::optional<CacheBlockRecord>>> CacheBlockStore::snapshotLoadBatch(
    kv::IReadOnlyTransaction &txn,
    std::span<const cache::CacheBlockKey> keys) {
  std::vector<CoTryTask<std::optional<CacheBlockRecord>>> tasks;
  tasks.reserve(keys.size());
  for (const auto &key : keys) tasks.push_back(snapshotLoad(txn, key));
  auto results = co_await folly::coro::collectAllRange(std::move(tasks));
  std::vector<std::optional<CacheBlockRecord>> records;
  records.reserve(results.size());
  for (auto &result : results) {
    CO_RETURN_ON_ERROR(result);
    records.push_back(std::move(*result));
  }
  co_return records;
}

CoTryTask<CacheBlockPage> CacheBlockStore::snapshotList(kv::IReadOnlyTransaction &txn,
                                                        uint64_t inode,
                                                        cache::CacheBlockIndex begin,
                                                        uint32_t limit) {
  if (inode == 0 || limit == 0) co_return makeError(StatusCode::kInvalidArg, "invalid cache block list range");
  auto prefix = Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{0}, inode);
  auto beginKey = recordKey({inode, begin});
  auto endKey = kv::TransactionHelper::prefixListEndKey(prefix);
  auto result = co_await txn.snapshotGetRange({beginKey, true}, {endKey, false}, static_cast<int32_t>(limit + 1));
  CO_RETURN_ON_ERROR(result);
  CacheBlockPage page;
  page.more = result->kvs.size() > limit || result->hasMore;
  auto count = std::min<size_t>(result->kvs.size(), limit);
  page.records.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    auto record = decodeRecord(result->kvs[i]);
    CO_RETURN_ON_ERROR(record);
    page.records.push_back(std::move(*record));
  }
  co_return page;
}

CoTryTask<CacheBlockPage> CacheBlockStore::snapshotListRecoverable(kv::IReadOnlyTransaction &txn,
                                                                   std::optional<cache::CacheBlockKey> after,
                                                                   uint32_t limit) {
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidArg, "invalid recoverable cache block page limit");
  }
  if (after) CO_RETURN_ON_ERROR(after->valid());
  const auto prefix = Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{0});
  const auto endKey = kv::TransactionHelper::prefixListEndKey(prefix);
  auto beginKey = after ? recordKey(*after) : prefix;
  bool inclusive = !after.has_value();
  CacheBlockPage page;
  while (page.records.size() <= limit) {
    auto result = co_await txn.snapshotGetRange({beginKey, inclusive}, {endKey, false}, kRecoverableScanBatch);
    CO_RETURN_ON_ERROR(result);
    if (result->kvs.empty()) {
      if (result->hasMore) co_return makeError(CacheCode::kInvalidResponse, "empty cache recovery scan has more data");
      break;
    }
    for (const auto &value : result->kvs) {
      auto record = decodeRecord(value);
      CO_RETURN_ON_ERROR(record);
      if ((record->state == cache::CacheBlockState::QUEUED || record->state == cache::CacheBlockState::LOADING) &&
          record->permit) {
        page.records.push_back(std::move(*record));
        if (page.records.size() > limit) break;
      }
    }
    if (page.records.size() > limit || !result->hasMore) break;
    beginKey = result->kvs.back().key;
    inclusive = false;
  }
  page.more = page.records.size() > limit;
  if (page.more) page.records.resize(limit);
  co_return page;
}

CoTryTask<CacheBlockPage> CacheBlockStore::snapshotListReconcile(kv::IReadOnlyTransaction &txn,
                                                                 std::optional<cache::CacheBlockKey> after,
                                                                 uint32_t limit) {
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidArg, "invalid cache reconcile page limit");
  }
  if (after) CO_RETURN_ON_ERROR(after->valid());
  const auto prefix = Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{0});
  const auto endKey = kv::TransactionHelper::prefixListEndKey(prefix);
  auto beginKey = after ? recordKey(*after) : prefix;
  bool inclusive = !after.has_value();
  CacheBlockPage page;
  while (page.records.size() <= limit) {
    auto result = co_await txn.snapshotGetRange({beginKey, inclusive}, {endKey, false}, kRecoverableScanBatch);
    CO_RETURN_ON_ERROR(result);
    if (result->kvs.empty()) {
      if (result->hasMore) co_return makeError(CacheCode::kInvalidResponse, "empty cache reconcile scan has more data");
      break;
    }
    for (const auto &value : result->kvs) {
      auto record = decodeRecord(value);
      CO_RETURN_ON_ERROR(record);
      if (record->state == cache::CacheBlockState::READY || record->state == cache::CacheBlockState::EVICTING ||
          record->state == cache::CacheBlockState::CLEANING) {
        page.records.push_back(std::move(*record));
        if (page.records.size() > limit) break;
      }
    }
    if (page.records.size() > limit || !result->hasMore) break;
    beginKey = result->kvs.back().key;
    inclusive = false;
  }
  page.more = page.records.size() > limit;
  if (page.more) page.records.resize(limit);
  co_return page;
}

CoTryTask<std::vector<CacheBlockRecord>> CacheBlockStore::snapshotListAll(kv::IReadOnlyTransaction &txn,
                                                                          std::optional<uint64_t> inode) {
  auto prefix = inode ? Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{0}, *inode)
                      : Serializer::serRawArgs(kv::KeyPrefix::CacheBlock, uint8_t{0});
  auto options = kv::TransactionHelper::ListByPrefixOptions().withInclusive(true).withSnapshot(true).withLimit(0);
  auto values = co_await kv::TransactionHelper::listByPrefix(txn, prefix, options);
  CO_RETURN_ON_ERROR(values);
  std::vector<CacheBlockRecord> records;
  records.reserve(values->size());
  for (const auto &value : *values) {
    auto record = decodeRecord(value);
    CO_RETURN_ON_ERROR(record);
    records.push_back(std::move(*record));
  }
  co_return records;
}

CoTryTask<std::optional<CacheBlockRecord>> CacheBlockStore::load(kv::IReadWriteTransaction &txn,
                                                                 const cache::CacheBlockKey &key) {
  co_return co_await loadRecord(txn, key, false);
}

CoTryTask<Void> CacheBlockStore::store(kv::IReadWriteTransaction &txn, const CacheBlockRecord &record) {
  CO_RETURN_ON_ERROR(record.valid());
  auto result = co_await txn.set(recordKey(record.key), serde::serialize(record));
  CO_RETURN_ON_ERROR(result);
  cache::metrics::recordCount(cache::metrics::Event::META_STATE_TRANSITION,
                              1,
                              {.inode = record.key.inode,
                               .block = record.key.block.toUnderType(),
                               .reason = std::string(magic_enum::enum_name(record.state))});
  if (record.chargeKind != cache::ChargeKind::NONE && record.chargedBytes != 0) {
    cache::metrics::recordCount(cache::metrics::Event::META_CHARGE_BYTES,
                                record.chargedBytes,
                                {.inode = record.key.inode,
                                 .block = record.key.block.toUnderType(),
                                 .reason = std::string(magic_enum::enum_name(record.chargeKind))});
  }
  XLOGF(DBG,
        "Cache block state inode {} block {} state {} charge {} bytes {}",
        record.key.inode,
        record.key.block,
        magic_enum::enum_name(record.state),
        magic_enum::enum_name(record.chargeKind),
        record.chargedBytes);
  co_return Void{};
}

CoTryTask<Void> CacheBlockStore::remove(kv::IReadWriteTransaction &txn, const cache::CacheBlockKey &key) {
  CO_RETURN_ON_ERROR(key.valid());
  co_return co_await txn.clear(recordKey(key));
}

CoTryTask<CacheBlockRecord> CacheBlockStore::enqueue(kv::IReadWriteTransaction &txn,
                                                     const cache::CacheBlockKey &key,
                                                     flat::ChainId chainId,
                                                     uint64_t blockLength,
                                                     std::optional<storage::PermitIdentity> permit,
                                                     std::optional<storage::PermitIdentity> expectedPermit,
                                                     cache::CacheBlockState expectedState,
                                                     Uuid expectedLoaderId,
                                                     uint64_t expectedLoadEpoch) {
  if (blockLength == 0) co_return makeError(StatusCode::kInvalidArg, "block length is zero");
  if (permit.has_value()) CO_RETURN_ON_ERROR(permit->valid());
  if (expectedPermit.has_value()) CO_RETURN_ON_ERROR(expectedPermit->valid());
  auto existing = co_await load(txn, key);
  CO_RETURN_ON_ERROR(existing);
  if (existing->has_value() && (*existing)->state != cache::CacheBlockState::FAILED) {
    if ((*existing)->chainId != chainId || (*existing)->blockLength != blockLength) {
      co_return makeError(CacheCode::kStateConflict, "cache block layout changed");
    }
    if (!permit.has_value() || (*existing)->state == cache::CacheBlockState::READY) co_return **existing;
    if (!(*existing)->permit.has_value()) {
      co_return makeError(CacheCode::kStateConflict, "existing cache admission has no permit identity");
    }
    const auto &current = *(*existing)->permit;
    if (current.placement.admissionAttemptId == permit->placement.admissionAttemptId && current == *permit) {
      co_return **existing;
    }
    if (!expectedPermit.has_value()) co_return **existing;
    if (current != *expectedPermit || (*existing)->state != expectedState) {
      co_return makeError(CacheCode::kStateConflict, "cache permit replacement fence changed");
    }
    if (current.placement != permit->placement || permit->permitGeneration <= current.permitGeneration) {
      co_return makeError(CacheCode::kPermitConflict, "cache permit replacement is not a newer generation");
    }
    if ((*existing)->state == cache::CacheBlockState::LOADING) {
      if ((*existing)->loaderId != expectedLoaderId || (*existing)->loadEpoch != expectedLoadEpoch) {
        co_return makeError(CacheCode::kStateConflict, "cache loader fence changed");
      }
    } else if (expectedLoaderId != Uuid::zero() || expectedLoadEpoch != 0) {
      co_return makeError(CacheCode::kStateConflict, "queued permit replacement cannot carry loader fence");
    }
    (*existing)->permit = std::move(permit);
    CO_RETURN_ON_ERROR(co_await store(txn, **existing));
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
  record.permit = std::move(permit);
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

CoTryTask<cache::EvictionEpoch> CacheBlockStore::allocateEvictionEpoch(kv::IReadWriteTransaction &txn,
                                                                       const cache::CacheBlockKey &key) {
  CO_RETURN_ON_ERROR(key.valid());
  auto packedKey = evictionEpochKey(key);
  auto value = co_await txn.get(packedKey);
  CO_RETURN_ON_ERROR(value);
  cache::EvictionEpoch current;
  if (value->has_value()) {
    auto deserialized = serde::deserialize(current, **value);
    if (deserialized.hasError()) co_return makeError(StatusCode::kDataCorruption, "invalid cache eviction epoch");
  }
  auto next = cache::nextEvictionEpoch(current);
  CO_RETURN_ON_ERROR(next);
  CO_RETURN_ON_ERROR(co_await txn.set(packedKey, serde::serialize(*next)));
  co_return *next;
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

CoTryTask<bool> CacheBlockStore::updateAccess(kv::IReadWriteTransaction &txn,
                                              const cache::CacheBlockKey &key,
                                              cache::CacheGeneration generation,
                                              UtcTime managerReceiveTime) {
  CO_RETURN_ON_ERROR(key.valid());
  if (generation == cache::CacheGeneration{} || managerReceiveTime.isZero()) {
    co_return makeError(StatusCode::kInvalidArg, "invalid cache access update");
  }
  auto loaded = co_await load(txn, key);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value() || (*loaded)->state != cache::CacheBlockState::READY ||
      (*loaded)->cacheGeneration != generation || (*loaded)->lastAccessAt >= managerReceiveTime) {
    co_return false;
  }
  (*loaded)->lastAccessAt = managerReceiveTime;
  CO_RETURN_ON_ERROR(co_await store(txn, **loaded));
  co_return true;
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
  record.readyAt = UtcTime{};
  record.lastAccessAt = UtcTime{};
  record.permit.reset();
  record.placement.reset();
  record.committedPermit.reset();
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
    requeued->cleanupEpoch = record.cleanupEpoch;
    requeued->terminalState = cache::CleanupTerminalState::REENQUEUE;
    CO_RETURN_ON_ERROR(co_await store(txn, *requeued));
  }
  co_return Void{};
}

}  // namespace hf3fs::meta::server
