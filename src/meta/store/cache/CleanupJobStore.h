#pragma once

#include <optional>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/components/OriginNamespaceManager.h"

namespace hf3fs::meta::server {

class CleanupJobStore {
 public:
  static CoTryTask<std::optional<OriginCleanupJobRecord>> snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                       const Uuid &jobId);
  static CoTryTask<std::optional<OriginCleanupJobRecord>> load(kv::IReadWriteTransaction &txn, const Uuid &jobId);
  static CoTryTask<Void> store(kv::IReadWriteTransaction &txn, const OriginCleanupJobRecord &record);

  // Advance one bounded scan batch. A pass that still observes non-terminal blocks
  // wraps to beginBlock so a restarted manager can retry without losing its cursor.
  static CoTryTask<OriginCleanupJobRecord> advance(kv::IReadWriteTransaction &txn, const Uuid &jobId);
};

}  // namespace hf3fs::meta::server
