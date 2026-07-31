#pragma once

#include "client/meta/MetaClient.h"
#include "storage/cache/event/CacheEventOutbox.h"

namespace hf3fs::storage {

class CacheEventReporter {
 public:
  CacheEventReporter(CacheEventOutbox &outbox, meta::client::MetaClient &metaClient)
      : outbox_(outbox),
        metaClient_(metaClient) {}

  CoTryTask<uint64_t> reportOnce(uint32_t limit);

  static meta::CacheStorageEvent toWire(const CacheEventEnvelope &envelope);

 private:
  CacheEventOutbox &outbox_;
  meta::client::MetaClient &metaClient_;
};

}  // namespace hf3fs::storage
