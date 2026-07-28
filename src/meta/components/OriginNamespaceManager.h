#pragma once

#include <optional>

#include "common/kv/ITransaction.h"
#include "common/serde/Serde.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Uuid.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta::server {

enum class OriginCleanupJobState : uint8_t {
  PENDING,
  RUNNING,
  COMPLETE,
};

struct OriginCleanupJobRecord {
  SERDE_STRUCT_FIELD(jobId, Uuid::zero());
  SERDE_STRUCT_FIELD(inode, InodeId{});
  SERDE_STRUCT_FIELD(beginBlock, uint64_t{0});
  SERDE_STRUCT_FIELD(endBlock, uint64_t{0});
  SERDE_STRUCT_FIELD(cursor, uint64_t{0});
  SERDE_STRUCT_FIELD(state, OriginCleanupJobState::PENDING);
  SERDE_STRUCT_FIELD(lastBatchBegin, uint64_t{0});
  SERDE_STRUCT_FIELD(lastBatchEnd, uint64_t{0});
  SERDE_STRUCT_FIELD(lastBatchSucceeded, uint32_t{0});
  SERDE_STRUCT_FIELD(lastBatchFailed, uint32_t{0});
  SERDE_STRUCT_FIELD(remainingNonTerminalBlocks, uint64_t{0});
  SERDE_STRUCT_FIELD(remainingChargedBytes, uint64_t{0});

 public:
  Result<Void> valid() const;
  bool complete() const;
};

struct RefreshOriginFileRecord {
  SERDE_STRUCT_FIELD(requestId, Uuid::zero());
  SERDE_STRUCT_FIELD(path, PathAt{});
  SERDE_STRUCT_FIELD(expectedInode, InodeId{});
  SERDE_STRUCT_FIELD(oldObject, cache::ImmutableObjectIdentity{});
  SERDE_STRUCT_FIELD(newMetadata, OriginFileMetadata{});
  SERDE_STRUCT_FIELD(newInode, meta::Inode{});
  SERDE_STRUCT_FIELD(cleanupJobId, Uuid::zero());

 public:
  Result<Void> valid() const;
  bool matches(const RefreshOriginFileReq &req) const;
  RefreshOriginFileRsp response() const;
};

class OriginNamespaceManager {
 public:
  static CoTryTask<std::optional<RefreshOriginFileRecord>> loadRefresh(kv::IReadWriteTransaction &txn,
                                                                       const Uuid &requestId);
  static CoTryTask<Void> storeRefresh(kv::IReadWriteTransaction &txn, const RefreshOriginFileRecord &record);

  static CoTryTask<std::optional<OriginCleanupJobRecord>> snapshotLoadCleanup(kv::IReadOnlyTransaction &txn,
                                                                              const Uuid &jobId);
  static CoTryTask<std::optional<OriginCleanupJobRecord>> loadCleanup(kv::IReadWriteTransaction &txn,
                                                                      const Uuid &jobId);
  static CoTryTask<Void> storeCleanup(kv::IReadWriteTransaction &txn, const OriginCleanupJobRecord &record);

  static std::string refreshKey(const Uuid &requestId);
  static std::string cleanupKey(const Uuid &jobId);
};

}  // namespace hf3fs::meta::server
