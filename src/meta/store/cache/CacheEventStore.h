#pragma once

#include <optional>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta::server {

struct CacheEventCursorRecord {
  SERDE_STRUCT_FIELD(sourceId, storage::PhysicalDiskId{});
  SERDE_STRUCT_FIELD(acknowledgedSequence, uint64_t{0});

 public:
  Result<Void> valid() const;
};

struct CacheEventDeadLetterPage {
  std::vector<CacheEventDeadLetter> items;
  bool more{false};
};

class CacheEventStore {
 public:
  static CoTryTask<CacheEventCursorRecord> loadCursor(kv::IReadWriteTransaction &txn,
                                                      const storage::PhysicalDiskId &sourceId);
  static CoTryTask<Void> storeCursor(kv::IReadWriteTransaction &txn, const CacheEventCursorRecord &cursor);
  static CoTryTask<Void> storeDeadLetter(kv::IReadWriteTransaction &txn, const CacheEventDeadLetter &deadLetter);
  static CoTryTask<CacheEventDeadLetterPage> snapshotListDeadLetters(kv::IReadOnlyTransaction &txn,
                                                                     std::optional<storage::PhysicalDiskId> sourceId,
                                                                     uint64_t beginSequence,
                                                                     uint32_t limit);

  static std::string cursorKey(const storage::PhysicalDiskId &sourceId);
  static std::string deadLetterKey(const storage::PhysicalDiskId &sourceId, uint64_t sequence);
};

}  // namespace hf3fs::meta::server
