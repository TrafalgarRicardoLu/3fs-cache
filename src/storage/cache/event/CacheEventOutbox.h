#pragma once

#include "storage/cache/event/CacheEventJournal.h"

namespace hf3fs::storage {

class CacheEventOutbox {
 public:
  explicit CacheEventOutbox(CacheEventJournal &journal)
      : journal_(journal) {}

  Result<std::vector<CacheEventEnvelope>> next(uint32_t limit) const { return journal_.deliveryBatch(limit); }
  Result<Void> acknowledge(PhysicalDiskId sourceId, uint64_t sequence);

 private:
  CacheEventJournal &journal_;
};

}  // namespace hf3fs::storage
