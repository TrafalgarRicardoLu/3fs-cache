#include "storage/cache/event/CacheEventReporter.h"

namespace hf3fs::storage {

meta::CacheStorageEvent CacheEventReporter::toWire(const CacheEventEnvelope &envelope) {
  const auto &intent = envelope.intent;
  meta::CacheStorageEvent event;
  event.sourceId = envelope.sourceId;
  event.sequence = envelope.sequence;
  event.type = intent.type;
  event.storageOperationId = intent.storageOperationId;
  event.logicalRetireOperationId = intent.logicalRetireOperationId;
  event.logicalKey = intent.logicalKey;
  event.storageKey = intent.storageKey;
  event.storageTargetId = intent.storageTargetId;
  event.generation = intent.generation;
  event.placement = intent.placement;
  event.evictionEpoch = intent.evictionEpoch;
  event.diskId = intent.diskId;
  event.timestamp = intent.timestamp;
  return event;
}

CoTryTask<uint64_t> CacheEventReporter::reportOnce(uint32_t limit) {
  auto batch = outbox_.next(limit);
  CO_RETURN_ON_ERROR(batch);
  if (batch->empty()) co_return uint64_t{0};

  meta::ReportCacheStorageEventsReq request;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  request.events.reserve(batch->size());
  for (const auto &envelope : *batch) request.events.push_back(toWire(envelope));
  CO_RETURN_ON_ERROR(request.valid());

  auto response = co_await metaClient_.reportCacheStorageEvents(std::move(request));
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != batch->size()) {
    co_return makeError(CacheCode::kInvalidResponse, "Metadata returned the wrong cache event ACK count");
  }

  uint64_t acknowledged = 0;
  std::optional<Status> firstError;
  for (size_t index = 0; index < response->results.size(); ++index) {
    const auto &result = response->results[index];
    if (result.hasError()) {
      firstError = result.error();
      break;
    }
    if (result->sourceId != (*batch)[index].sourceId || result->sequence < (*batch)[index].sequence) {
      firstError = Status(CacheCode::kInvalidResponse, "Metadata returned an invalid cache event ACK");
      break;
    }
    acknowledged = std::max(acknowledged, result->sequence);
  }
  if (acknowledged != 0) {
    CO_RETURN_ON_ERROR(outbox_.acknowledge(batch->front().sourceId, acknowledged));
  }
  if (firstError.has_value()) co_return makeError(std::move(*firstError));
  co_return acknowledged;
}

}  // namespace hf3fs::storage
