#include "storage/cache/event/CacheEventOutbox.h"

namespace hf3fs::storage {

Result<Void> CacheEventOutbox::acknowledge(PhysicalDiskId sourceId, uint64_t sequence) {
  RETURN_ON_ERROR(sourceId.valid());
  if (sourceId != journal_.stats().sourceId) {
    return makeError(CacheCode::kStateConflict, "cache event ACK source does not match this outbox");
  }
  return journal_.acknowledge(sequence);
}

}  // namespace hf3fs::storage
