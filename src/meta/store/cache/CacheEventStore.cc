#include "meta/store/cache/CacheEventStore.h"

#include "common/kv/KeyPrefix.h"
#include "common/serde/BigEndian.h"
#include "common/serde/Serde.h"
#include "common/utils/SerDeser.h"

namespace hf3fs::meta::server {
namespace {

std::string deadLetterPrefix(std::optional<storage::PhysicalDiskId> sourceId) {
  if (sourceId.has_value()) return Serializer::serRawArgs(kv::KeyPrefix::CacheEventDead, sourceId->uuid);
  return Serializer::serRawArgs(kv::KeyPrefix::CacheEventDead);
}

}  // namespace

Result<Void> CacheEventCursorRecord::valid() const { return sourceId.valid(); }

std::string CacheEventStore::cursorKey(const storage::PhysicalDiskId &sourceId) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheEventCursor, sourceId.uuid);
}

std::string CacheEventStore::deadLetterKey(const storage::PhysicalDiskId &sourceId, uint64_t sequence) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheEventDead, sourceId.uuid, serde::BigEndian<uint64_t>{sequence});
}

CoTryTask<CacheEventCursorRecord> CacheEventStore::loadCursor(kv::IReadWriteTransaction &txn,
                                                              const storage::PhysicalDiskId &sourceId) {
  CO_RETURN_ON_ERROR(sourceId.valid());
  auto value = co_await txn.get(cursorKey(sourceId));
  CO_RETURN_ON_ERROR(value);
  if (!value->has_value()) co_return CacheEventCursorRecord{sourceId, 0};
  CacheEventCursorRecord cursor;
  auto decoded = serde::deserialize(cursor, **value);
  if (decoded.hasError() || cursor.valid().hasError() || cursor.sourceId != sourceId) {
    co_return makeError(StatusCode::kDataCorruption, "invalid cache event cursor");
  }
  co_return cursor;
}

CoTryTask<Void> CacheEventStore::storeCursor(kv::IReadWriteTransaction &txn, const CacheEventCursorRecord &cursor) {
  CO_RETURN_ON_ERROR(cursor.valid());
  co_return co_await txn.set(cursorKey(cursor.sourceId), serde::serialize(cursor));
}

CoTryTask<Void> CacheEventStore::storeDeadLetter(kv::IReadWriteTransaction &txn,
                                                 const CacheEventDeadLetter &deadLetter) {
  CO_RETURN_ON_ERROR(deadLetter.event.valid());
  if (deadLetter.errorCode == 0 || deadLetter.reason.empty()) {
    co_return makeError(StatusCode::kInvalidArg, "invalid cache event dead letter");
  }
  co_return co_await txn.set(deadLetterKey(deadLetter.event.sourceId, deadLetter.event.sequence),
                             serde::serialize(deadLetter));
}

CoTryTask<CacheEventDeadLetterPage> CacheEventStore::snapshotListDeadLetters(
    kv::IReadOnlyTransaction &txn,
    std::optional<storage::PhysicalDiskId> sourceId,
    uint64_t beginSequence,
    uint32_t limit) {
  if (sourceId.has_value()) CO_RETURN_ON_ERROR(sourceId->valid());
  if (!sourceId.has_value() && beginSequence != 0) {
    co_return makeError(StatusCode::kInvalidArg, "begin sequence requires an event source");
  }
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems) {
    co_return makeError(CacheCode::kRequestTooLarge, "invalid cache dead-letter page limit");
  }
  auto prefix = deadLetterPrefix(sourceId);
  auto begin = sourceId.has_value() ? deadLetterKey(*sourceId, beginSequence) : prefix;
  auto end = kv::TransactionHelper::prefixListEndKey(prefix);
  auto values = co_await txn.snapshotGetRange({begin, true}, {end, false}, static_cast<int32_t>(limit + 1));
  CO_RETURN_ON_ERROR(values);
  CacheEventDeadLetterPage page;
  page.more = values->hasMore || values->kvs.size() > limit;
  auto count = std::min<size_t>(values->kvs.size(), limit);
  page.items.reserve(count);
  for (size_t index = 0; index < count; ++index) {
    CacheEventDeadLetter deadLetter;
    auto decoded = serde::deserialize(deadLetter, values->kvs[index].value);
    if (decoded.hasError() || deadLetter.event.valid().hasError() ||
        deadLetterKey(deadLetter.event.sourceId, deadLetter.event.sequence) != values->kvs[index].key ||
        deadLetter.errorCode == 0 || deadLetter.reason.empty()) {
      co_return makeError(StatusCode::kDataCorruption, "invalid cache event dead letter");
    }
    page.items.push_back(std::move(deadLetter));
  }
  co_return page;
}

}  // namespace hf3fs::meta::server
