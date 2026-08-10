#include "meta/store/cache/PinStore.h"

#include <algorithm>
#include <limits>

#include "common/serde/Serde.h"
#include "meta/store/cache/OrchestrationKey.h"

namespace hf3fs::meta::server {
namespace {

constexpr size_t kMaxPinValueBytes = 16U << 10;

Result<std::string> encode(const cache::PinRecord &pin) {
  auto value = serde::serialize(pin);
  if (value.size() > kMaxPinValueBytes) return makeError(CacheCode::kRequestTooLarge, "cache pin record is too large");
  return value;
}

Result<cache::PinRecord> decodeByBlock(std::string_view key, std::string_view value) {
  auto identity = OrchestrationKey::unpackPinByBlock(key);
  RETURN_ON_ERROR(identity);
  cache::PinRecord pin;
  auto deserialized = serde::deserialize(pin, value);
  if (deserialized.hasError() || pin.valid().hasError() || pin.key != identity->block || pin.owner != identity->owner) {
    return makeError(StatusCode::kDataCorruption, "invalid pin-by-block record");
  }
  return pin;
}

Result<cache::PinRecord> decodeByOwner(std::string_view key, std::string_view value) {
  auto identity = OrchestrationKey::unpackPinByOwner(key);
  RETURN_ON_ERROR(identity);
  cache::PinRecord pin;
  auto deserialized = serde::deserialize(pin, value);
  if (deserialized.hasError() || pin.valid().hasError() || pin.key != identity->block || pin.owner != identity->owner) {
    return makeError(StatusCode::kDataCorruption, "invalid pin-by-owner record");
  }
  return pin;
}

CoTryTask<std::optional<cache::PinRecord>> loadPair(kv::IReadWriteTransaction &txn,
                                                    const cache::CacheBlockKey &key,
                                                    const cache::PinOwner &owner) {
  auto blockKey = OrchestrationKey::pinByBlock(key, owner);
  auto ownerKey = OrchestrationKey::pinByOwner(owner, key);
  auto byBlock = co_await txn.get(blockKey);
  CO_RETURN_ON_ERROR(byBlock);
  auto byOwner = co_await txn.get(ownerKey);
  CO_RETURN_ON_ERROR(byOwner);
  if (byBlock->has_value() != byOwner->has_value()) {
    co_return makeError(StatusCode::kDataCorruption, "cache pin indexes disagree");
  }
  if (!byBlock->has_value()) co_return std::nullopt;
  auto first = decodeByBlock(blockKey, **byBlock);
  CO_RETURN_ON_ERROR(first);
  auto second = decodeByOwner(ownerKey, **byOwner);
  CO_RETURN_ON_ERROR(second);
  if (*first != *second) co_return makeError(StatusCode::kDataCorruption, "cache pin index values disagree");
  co_return std::move(*first);
}

template <typename Transaction>
CoTryTask<std::optional<cache::PinOwnerLease>> loadOwnerLease(Transaction &txn,
                                                              const cache::PinOwner &owner,
                                                              bool snapshot) {
  auto key = OrchestrationKey::pinOwnerLease(owner);
  auto value = snapshot ? co_await txn.snapshotGet(key) : co_await txn.get(key);
  CO_RETURN_ON_ERROR(value);
  if (!value->has_value()) co_return std::nullopt;
  cache::PinOwnerLease lease;
  auto decoded = serde::deserialize(lease, **value);
  if (decoded.hasError() || lease.valid().hasError() || lease.owner != owner) {
    co_return makeError(StatusCode::kDataCorruption, "invalid pin owner lease");
  }
  co_return lease;
}

CoTryTask<Void> validateSnapshotPair(kv::IReadOnlyTransaction &txn, const cache::PinRecord &pin) {
  auto counterpart = co_await txn.snapshotGet(OrchestrationKey::pinByBlock(pin.key, pin.owner));
  CO_RETURN_ON_ERROR(counterpart);
  if (!counterpart->has_value()) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart is missing");
  auto decoded = decodeByBlock(OrchestrationKey::pinByBlock(pin.key, pin.owner), **counterpart);
  CO_RETURN_ON_ERROR(decoded);
  if (*decoded != pin) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart differs");
  co_return Void{};
}

CoTryTask<Void> checkActiveLimit(kv::IReadWriteTransaction &txn,
                                 std::string_view prefix,
                                 uint32_t limit,
                                 uint64_t nowMs,
                                 bool byBlock) {
  auto values = co_await kv::TransactionHelper::listByPrefix(
      txn,
      prefix,
      kv::TransactionHelper::ListByPrefixOptions().withSnapshot(false).withLimit(0));
  CO_RETURN_ON_ERROR(values);
  uint32_t active = 0;
  for (const auto &value : *values) {
    auto pin = byBlock ? decodeByBlock(value.key, value.value) : decodeByOwner(value.key, value.value);
    CO_RETURN_ON_ERROR(pin);
    auto counterpartKey = byBlock ? OrchestrationKey::pinByOwner(pin->owner, pin->key)
                                  : OrchestrationKey::pinByBlock(pin->key, pin->owner);
    auto counterpart = co_await txn.get(counterpartKey);
    CO_RETURN_ON_ERROR(counterpart);
    if (!counterpart->has_value()) {
      co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart is missing");
    }
    auto other = byBlock ? decodeByOwner(counterpartKey, **counterpart) : decodeByBlock(counterpartKey, **counterpart);
    CO_RETURN_ON_ERROR(other);
    if (*other != *pin) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart differs");
    auto lease = co_await loadOwnerLease(txn, pin->owner, false);
    CO_RETURN_ON_ERROR(lease);
    if ((pin->expiresAtMs > nowMs || (lease->has_value() && (*lease)->expiresAtMs > nowMs)) && ++active >= limit) {
      co_return makeError(CacheCode::kRequestTooLarge, "cache pin ownership limit reached");
    }
  }
  co_return Void{};
}

}  // namespace

Result<Void> PinStoreLimits::valid() const {
  if (maxOwnersPerBlock == 0 || maxBlocksPerOwner == 0 ||
      maxOwnersPerBlock >= static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) ||
      maxBlocksPerOwner >= static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return makeError(StatusCode::kInvalidArg, "invalid cache pin store limits");
  }
  return Void{};
}

CoTryTask<PinStore::RenewOwnerLeaseResult> PinStore::renewOwnerLease(kv::IReadWriteTransaction &txn,
                                                                     const cache::PinOwnerLease &lease) {
  CO_RETURN_ON_ERROR(lease.valid());
  auto existing = co_await loadOwnerLease(txn, lease.owner, false);
  CO_RETURN_ON_ERROR(existing);
  if (existing->has_value() &&
      ((*existing)->createdAtMs != lease.createdAtMs || (*existing)->expiresAtMs > lease.expiresAtMs)) {
    co_return makeError(CacheCode::kStateConflict, "pin owner lease moved backwards");
  }
  auto created = !existing->has_value();
  CO_RETURN_ON_ERROR(co_await txn.set(OrchestrationKey::pinOwnerLease(lease.owner), serde::serialize(lease)));
  co_return RenewOwnerLeaseResult{lease, created};
}

CoTryTask<cache::PinRecord> PinStore::upsert(kv::IReadWriteTransaction &txn,
                                             const cache::PinRecord &pin,
                                             const PinStoreLimits &limits) {
  CO_RETURN_ON_ERROR(pin.valid());
  CO_RETURN_ON_ERROR(limits.valid());
  auto existing = co_await loadPair(txn, pin.key, pin.owner);
  CO_RETURN_ON_ERROR(existing);
  if (existing->has_value() && (*existing)->expiresAtMs > pin.createdAtMs) {
    if (pin.createdAtMs != (*existing)->createdAtMs || pin.expiresAtMs < (*existing)->expiresAtMs ||
        ((*existing)->cacheGeneration != cache::CacheGeneration{} &&
         pin.cacheGeneration != (*existing)->cacheGeneration)) {
      co_return makeError(CacheCode::kStateConflict, "cache pin renewal moved its fence backwards");
    }
    if (**existing == pin) co_return **existing;
  } else {
    CO_RETURN_ON_ERROR(co_await checkActiveLimit(txn,
                                                 OrchestrationKey::pinByBlockPrefix(pin.key),
                                                 limits.maxOwnersPerBlock,
                                                 pin.createdAtMs,
                                                 true));
    CO_RETURN_ON_ERROR(co_await checkActiveLimit(txn,
                                                 OrchestrationKey::pinByOwnerPrefix(pin.owner),
                                                 limits.maxBlocksPerOwner,
                                                 pin.createdAtMs,
                                                 false));
  }
  auto value = encode(pin);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(OrchestrationKey::pinByBlock(pin.key, pin.owner), *value));
  CO_RETURN_ON_ERROR(co_await txn.set(OrchestrationKey::pinByOwner(pin.owner, pin.key), *value));
  co_return pin;
}

CoTryTask<bool> PinStore::remove(kv::IReadWriteTransaction &txn,
                                 const cache::CacheBlockKey &key,
                                 const cache::PinOwner &owner) {
  CO_RETURN_ON_ERROR(key.valid());
  CO_RETURN_ON_ERROR(owner.valid());
  auto existing = co_await loadPair(txn, key, owner);
  CO_RETURN_ON_ERROR(existing);
  if (!existing->has_value()) co_return false;
  CO_RETURN_ON_ERROR(co_await txn.clear(OrchestrationKey::pinByBlock(key, owner)));
  CO_RETURN_ON_ERROR(co_await txn.clear(OrchestrationKey::pinByOwner(owner, key)));
  co_return true;
}

CoTryTask<uint64_t> PinStore::removeByOwner(kv::IReadWriteTransaction &txn,
                                            const cache::PinOwner &owner,
                                            std::span<const cache::CacheBlockKey> keys) {
  CO_RETURN_ON_ERROR(owner.valid());
  std::vector<cache::CacheBlockKey> selected(keys.begin(), keys.end());
  if (selected.empty()) {
    auto prefix = OrchestrationKey::pinByOwnerPrefix(owner);
    auto values = co_await kv::TransactionHelper::listByPrefix(
        txn,
        prefix,
        kv::TransactionHelper::ListByPrefixOptions().withSnapshot(false).withLimit(0));
    CO_RETURN_ON_ERROR(values);
    selected.reserve(values->size());
    for (const auto &value : *values) {
      auto pin = decodeByOwner(value.key, value.value);
      CO_RETURN_ON_ERROR(pin);
      selected.push_back(pin->key);
    }
  }
  uint64_t removed = 0;
  for (const auto &key : selected) {
    auto result = co_await remove(txn, key, owner);
    CO_RETURN_ON_ERROR(result);
    removed += *result;
  }
  if (keys.empty()) CO_RETURN_ON_ERROR(co_await txn.clear(OrchestrationKey::pinOwnerLease(owner)));
  co_return removed;
}

CoTryTask<PinPage> PinStore::snapshotListByOwner(kv::IReadOnlyTransaction &txn,
                                                 const cache::PinOwner &owner,
                                                 std::optional<cache::CacheBlockKey> after,
                                                 uint32_t limit) {
  CO_RETURN_ON_ERROR(owner.valid());
  if (after) CO_RETURN_ON_ERROR(after->valid());
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidArg, "invalid cache pin page");
  }
  auto prefix = OrchestrationKey::pinByOwnerPrefix(owner);
  auto begin = after ? OrchestrationKey::pinByOwner(owner, *after) : prefix;
  auto end = kv::TransactionHelper::prefixListEndKey(prefix);
  auto values =
      co_await txn.snapshotGetRange({begin, !after.has_value()}, {end, false}, static_cast<int32_t>(limit + 1));
  CO_RETURN_ON_ERROR(values);
  PinPage page;
  page.more = values->kvs.size() > limit || values->hasMore;
  auto count = std::min<size_t>(values->kvs.size(), limit);
  page.pins.reserve(count);
  for (size_t index = 0; index < count; ++index) {
    auto pin = decodeByOwner(values->kvs[index].key, values->kvs[index].value);
    CO_RETURN_ON_ERROR(pin);
    CO_RETURN_ON_ERROR(co_await validateSnapshotPair(txn, *pin));
    page.pins.push_back(std::move(*pin));
  }
  co_return page;
}

CoTryTask<std::vector<cache::PinRecord>> PinStore::snapshotQueryActive(kv::IReadOnlyTransaction &txn,
                                                                       const cache::CacheBlockKey &key,
                                                                       uint64_t nowMs) {
  CO_RETURN_ON_ERROR(key.valid());
  if (nowMs == 0) co_return makeError(StatusCode::kInvalidArg, "cache pin query time is zero");
  auto prefix = OrchestrationKey::pinByBlockPrefix(key);
  auto values = co_await kv::TransactionHelper::listByPrefix(
      txn,
      prefix,
      kv::TransactionHelper::ListByPrefixOptions().withSnapshot(true).withLimit(0));
  CO_RETURN_ON_ERROR(values);
  std::vector<cache::PinRecord> active;
  for (const auto &value : *values) {
    auto pin = decodeByBlock(value.key, value.value);
    CO_RETURN_ON_ERROR(pin);
    auto counterpart = co_await txn.snapshotGet(OrchestrationKey::pinByOwner(pin->owner, pin->key));
    CO_RETURN_ON_ERROR(counterpart);
    if (!counterpart->has_value()) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart is missing");
    auto other = decodeByOwner(OrchestrationKey::pinByOwner(pin->owner, pin->key), **counterpart);
    CO_RETURN_ON_ERROR(other);
    if (*other != *pin) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart differs");
    auto lease = co_await loadOwnerLease(txn, pin->owner, true);
    CO_RETURN_ON_ERROR(lease);
    if (pin->expiresAtMs > nowMs || (lease->has_value() && (*lease)->expiresAtMs > nowMs)) {
      active.push_back(std::move(*pin));
    }
  }
  co_return active;
}

CoTryTask<std::vector<cache::PinRecord>> PinStore::queryActive(kv::IReadWriteTransaction &txn,
                                                               const cache::CacheBlockKey &key,
                                                               uint64_t nowMs) {
  CO_RETURN_ON_ERROR(key.valid());
  if (nowMs == 0) co_return makeError(StatusCode::kInvalidArg, "cache pin query time is zero");
  auto prefix = OrchestrationKey::pinByBlockPrefix(key);
  auto values = co_await kv::TransactionHelper::listByPrefix(
      txn,
      prefix,
      kv::TransactionHelper::ListByPrefixOptions().withSnapshot(false).withLimit(0));
  CO_RETURN_ON_ERROR(values);
  std::vector<cache::PinRecord> active;
  for (const auto &value : *values) {
    auto pin = decodeByBlock(value.key, value.value);
    CO_RETURN_ON_ERROR(pin);
    auto counterpart = co_await txn.get(OrchestrationKey::pinByOwner(pin->owner, pin->key));
    CO_RETURN_ON_ERROR(counterpart);
    if (!counterpart->has_value()) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart is missing");
    auto other = decodeByOwner(OrchestrationKey::pinByOwner(pin->owner, pin->key), **counterpart);
    CO_RETURN_ON_ERROR(other);
    if (*other != *pin) co_return makeError(StatusCode::kDataCorruption, "cache pin counterpart differs");
    auto lease = co_await loadOwnerLease(txn, pin->owner, false);
    CO_RETURN_ON_ERROR(lease);
    if (pin->expiresAtMs > nowMs || (lease->has_value() && (*lease)->expiresAtMs > nowMs)) {
      active.push_back(std::move(*pin));
    }
  }
  co_return active;
}

}  // namespace hf3fs::meta::server
