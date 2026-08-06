#include <limits>
#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "meta/store/cache/CacheEventStore.h"
#include "meta/store/cache/PinStore.h"

namespace hf3fs::meta::server {
namespace {

struct EventEffect {
  bool deadLettered{false};
  status_code_t errorCode{0};
  std::string reason;
};

EventEffect deadLetter(std::string reason) { return {true, CacheCode::kStateConflict, std::move(reason)}; }

bool placementMatches(const CacheBlockRecord &record, const CacheStorageEvent &event) {
  return record.placement.has_value() && *record.placement == event.placement;
}

CoTryTask<Void> enterCleaning(IReadWriteTransaction &txn,
                              CacheBlockRecord &record,
                              cache::CacheGeneration observedGeneration,
                              cache::CleanupTerminalState terminalState) {
  ++record.loadEpoch;
  ++record.cleanupEpoch;
  record.state = cache::CacheBlockState::CLEANING;
  record.terminalState = terminalState;
  record.deleteGeneration = std::max(record.cacheGeneration, observedGeneration);
  record.leaseExpiresAt = UtcTime{};
  if (terminalState != cache::CleanupTerminalState::FAILED) record.loaderId = Uuid::zero();
  if (record.permit.has_value()) {
    record.placement = record.permit->placement;
    record.permit.reset();
  }
  co_return co_await CacheBlockStore::store(txn, record);
}

CoTryTask<EventEffect> applyEvent(IReadWriteTransaction &txn, const CacheStorageEvent &event) {
  auto loaded = co_await CacheBlockStore::load(txn, event.logicalKey);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return EventEffect{};
  auto record = std::move(**loaded);

  if (record.state == cache::CacheBlockState::FAILED) co_return EventEffect{};
  if (record.state == cache::CacheBlockState::QUEUED) {
    co_return deadLetter("cache event refers to a QUEUED block with no physical generation");
  }
  if (event.generation < record.cacheGeneration) co_return EventEffect{};
  if (event.generation > record.cacheGeneration) {
    co_return deadLetter("cache event generation is newer than Metadata");
  }

  switch (record.state) {
    case cache::CacheBlockState::READY:
      if (!placementMatches(record, event)) {
        co_return deadLetter("cache event placement differs from READY");
      }
      if (event.type == cache::CacheStorageEventType::DELETED) {
        if (record.loadEpoch == std::numeric_limits<uint64_t>::max() ||
            record.cleanupEpoch.toUnderType() == std::numeric_limits<uint64_t>::max()) {
          co_return deadLetter("logical DELETED cannot allocate a cleanup identity");
        }
        CO_RETURN_ON_ERROR(co_await enterCleaning(txn, record, event.generation, cache::CleanupTerminalState::NONE));
        co_return deadLetter("logical DELETED arrived while Metadata was READY");
      }
      {
        auto nowMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000);
        auto pins = co_await PinStore::queryActive(txn, record.key, nowMs);
        CO_RETURN_ON_ERROR(pins);
        if (!pins->empty()) co_return deadLetter("local eviction encountered an active cache pin");
        auto epoch = co_await CacheBlockStore::allocateEvictionEpoch(txn, record.key);
        if (epoch.hasError()) {
          if (epoch.error().code() == CacheCode::kStateConflict) {
            co_return deadLetter("local eviction cannot allocate an eviction identity");
          }
          co_return makeError(epoch.error());
        }
        record.state = cache::CacheBlockState::EVICTING;
        record.evictionEpoch = *epoch;
        record.retireOperationId = Uuid::random();
        record.evictionReason = cache::EvictionReason::LOCAL_SAFETY;
        CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
        co_return EventEffect{};
      }
    case cache::CacheBlockState::EVICTING:
      if (!placementMatches(record, event)) {
        co_return deadLetter("cache event placement differs from EVICTING");
      }
      if (event.type == cache::CacheStorageEventType::EMERGENCY_EVICTED) co_return EventEffect{};
      if (!event.logicalRetireOperationId.has_value() || !event.evictionEpoch.has_value() ||
          *event.logicalRetireOperationId != record.retireOperationId || *event.evictionEpoch != record.evictionEpoch) {
        co_return deadLetter("logical DELETED retirement identity differs from EVICTING");
      }
      if (record.chargeKind != cache::ChargeKind::NONE) {
        CO_RETURN_ON_ERROR(co_await CacheCapacityStore::release(txn, record.chargeKind, record.chargedBytes));
      }
      CO_RETURN_ON_ERROR(co_await CacheBlockStore::remove(txn, record.key));
      co_return EventEffect{};
    case cache::CacheBlockState::LOADING:
      if (record.permit.has_value() && record.permit->placement != event.placement) {
        co_return deadLetter("cache event placement differs from LOADING permit");
      }
      if (record.loadEpoch == std::numeric_limits<uint64_t>::max() ||
          record.cleanupEpoch.toUnderType() == std::numeric_limits<uint64_t>::max()) {
        co_return deadLetter("cache event cannot allocate a fenced failure identity");
      }
      CO_RETURN_ON_ERROR(co_await enterCleaning(txn, record, event.generation, cache::CleanupTerminalState::FAILED));
      co_return EventEffect{};
    case cache::CacheBlockState::CLEANING:
      if (record.placement.has_value() && *record.placement != event.placement) {
        co_return deadLetter("cache event placement differs from CLEANING");
      }
      co_return EventEffect{};
    case cache::CacheBlockState::INVALID:
      co_return EventEffect{};
    case cache::CacheBlockState::FAILED:
    case cache::CacheBlockState::NONE:
      co_return EventEffect{};
    case cache::CacheBlockState::QUEUED:
      co_return deadLetter("cache event refers to a QUEUED block with no physical generation");
  }
  co_return deadLetter("cache event encountered an unknown Metadata state");
}

}  // namespace

class ReportCacheStorageEventsOp : public Operation<ReportCacheStorageEventsRsp> {
 public:
  ReportCacheStorageEventsOp(MetaStore &meta, const ReportCacheStorageEventsReq &req)
      : Operation<ReportCacheStorageEventsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ReportCacheStorageEventsRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    ReportCacheStorageEventsRsp response;
    response.results.reserve(req_.events.size());
    for (const auto &event : req_.events) {
      response.results.emplace_back(co_await consume(txn, event));
    }
    co_return response;
  }

 private:
  CoTryTask<CacheStorageEventAck> consume(IReadWriteTransaction &txn, const CacheStorageEvent &event) {
    auto cursor = co_await CacheEventStore::loadCursor(txn, event.sourceId);
    CO_RETURN_ON_ERROR(cursor);
    if (event.sequence <= cursor->acknowledgedSequence) {
      co_return CacheStorageEventAck{event.sourceId, cursor->acknowledgedSequence, false};
    }
    if (event.sequence != cursor->acknowledgedSequence + 1) {
      co_return makeError(CacheCode::kEventGap, "cache storage event sequence gap");
    }

    auto effect = co_await applyEvent(txn, event);
    CO_RETURN_ON_ERROR(effect);
    if (effect->deadLettered) {
      CacheEventDeadLetter dead{event, effect->errorCode, effect->reason};
      CO_RETURN_ON_ERROR(co_await CacheEventStore::storeDeadLetter(txn, dead));
    }
    cursor->acknowledgedSequence = event.sequence;
    CO_RETURN_ON_ERROR(co_await CacheEventStore::storeCursor(txn, *cursor));
    co_return CacheStorageEventAck{event.sourceId, event.sequence, effect->deadLettered};
  }

  const ReportCacheStorageEventsReq &req_;
};

class ListCacheEventDeadLettersOp : public Operation<ListCacheEventDeadLettersRsp> {
 public:
  ListCacheEventDeadLettersOp(MetaStore &meta, const ListCacheEventDeadLettersReq &req)
      : Operation<ListCacheEventDeadLettersRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);
  bool isReadOnly() override { return true; }

  CoTryTask<ListCacheEventDeadLettersRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await CacheEventStore::snapshotListDeadLetters(txn, req_.sourceId, req_.beginSequence, req_.limit);
    CO_RETURN_ON_ERROR(page);
    ListCacheEventDeadLettersRsp response;
    response.items = std::move(page->items);
    response.more = page->more;
    co_return response;
  }

 private:
  const ListCacheEventDeadLettersReq &req_;
};

MetaStore::OpPtr<ReportCacheStorageEventsRsp> MetaStore::reportCacheStorageEvents(
    const ReportCacheStorageEventsReq &req) {
  return std::make_unique<ReportCacheStorageEventsOp>(*this, req);
}

MetaStore::OpPtr<ListCacheEventDeadLettersRsp> MetaStore::listCacheEventDeadLetters(
    const ListCacheEventDeadLettersReq &req) {
  return std::make_unique<ListCacheEventDeadLettersOp>(*this, req);
}

}  // namespace hf3fs::meta::server
