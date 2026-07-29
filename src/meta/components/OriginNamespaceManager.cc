#include "meta/components/OriginNamespaceManager.h"

#include <limits>
#include <sys/stat.h>

#include "cache/metrics/CacheMetrics.h"
#include "common/kv/KeyPrefix.h"
#include "common/serde/Serde.h"
#include "common/utils/MagicEnum.hpp"
#include "common/utils/SerDeser.h"

namespace hf3fs::meta::server {
namespace {

template <typename Record, typename Transaction>
CoTryTask<std::optional<Record>> loadRecord(Transaction &txn, std::string key, bool snapshot) {
  auto value = snapshot ? co_await txn.snapshotGet(key) : co_await txn.get(key);
  CO_RETURN_ON_ERROR(value);
  if (!value->has_value()) co_return std::nullopt;
  Record record;
  if (auto result = serde::deserialize(record, **value); result.hasError()) {
    co_return makeError(StatusCode::kDataCorruption, "invalid origin namespace record");
  }
  if (auto result = record.valid(); result.hasError()) {
    co_return makeError(StatusCode::kDataCorruption, "corrupt origin namespace record");
  }
  co_return record;
}

}  // namespace

Result<Void> OriginCleanupJobRecord::valid() const {
  if (jobId == Uuid::zero() || inode == InodeId{}) {
    return makeError(StatusCode::kInvalidArg, "invalid cleanup job identity");
  }
  if (!magic_enum::enum_contains(state)) return makeError(StatusCode::kInvalidArg, "invalid cleanup job state");
  if (beginBlock > cursor || cursor > endBlock) return makeError(StatusCode::kInvalidArg, "invalid cleanup cursor");
  if (endBlock - beginBlock > std::numeric_limits<uint32_t>::max()) {
    return makeError(StatusCode::kInvalidArg, "cleanup block range is too large");
  }
  if (state == OriginCleanupJobState::PENDING && cursor != beginBlock) {
    return makeError(StatusCode::kInvalidArg, "pending cleanup job has advanced its cursor");
  }
  const bool wrappedRetry = state == OriginCleanupJobState::RUNNING && cursor == beginBlock &&
                            lastBatchEnd == endBlock && remainingNonTerminalBlocks != 0;
  if (lastBatchBegin > lastBatchEnd || lastBatchBegin < beginBlock || (!wrappedRetry && lastBatchEnd > cursor)) {
    return makeError(StatusCode::kInvalidArg, "invalid cleanup batch range");
  }
  auto lastBatchItems = lastBatchEnd - lastBatchBegin;
  if (lastBatchItems > kMaxCacheBatchItems ||
      uint64_t{lastBatchSucceeded} + uint64_t{lastBatchFailed} != lastBatchItems ||
      (cursor != beginBlock && lastBatchEnd != cursor)) {
    return makeError(StatusCode::kInvalidArg, "invalid cleanup batch result");
  }
  if (state == OriginCleanupJobState::COMPLETE && !complete()) {
    return makeError(StatusCode::kInvalidArg, "cleanup job marked complete before its predicate is satisfied");
  }
  return Void{};
}

bool OriginCleanupJobRecord::complete() const {
  return state == OriginCleanupJobState::COMPLETE && cursor == endBlock && remainingNonTerminalBlocks == 0 &&
         remainingChargedBytes == 0;
}

Result<Void> RefreshOriginFileRecord::valid() const {
  if (requestId == Uuid::zero() || cleanupJobId == Uuid::zero() || expectedInode == InodeId{}) {
    return makeError(StatusCode::kInvalidArg, "invalid refresh record identity");
  }
  RETURN_ON_ERROR(path.validForCreate());
  RETURN_ON_ERROR(oldObject.valid());
  RETURN_ON_ERROR(newMetadata.valid());
  if (!newInode.isOriginFile() || newInode.id == expectedInode ||
      newInode.asOriginFile().object != newMetadata.object || newInode.fileLength() != newMetadata.objectSize) {
    return makeError(StatusCode::kInvalidArg, "refresh result does not match the new origin metadata");
  }
  const auto &layout = newInode.fileLayout();
  if (layout.tableId != newMetadata.tableId || layout.chunkSize != newMetadata.blockSize ||
      layout.stripeSize != newMetadata.stripeSize ||
      newInode.acl.perm != Permission(newMetadata.permission.toUnderType() & ALLPERMS)) {
    return makeError(StatusCode::kInvalidArg, "refresh result layout or permission does not match");
  }
  return Void{};
}

bool RefreshOriginFileRecord::matches(const RefreshOriginFileReq &req) const {
  return requestId == req.requestId && path == req.path && expectedInode == req.expectedInode &&
         oldObject == req.oldObject && newMetadata.object == req.newMetadata.object &&
         newMetadata.objectSize == req.newMetadata.objectSize && newMetadata.tableId == req.newMetadata.tableId &&
         newMetadata.blockSize == req.newMetadata.blockSize && newMetadata.stripeSize == req.newMetadata.stripeSize &&
         newMetadata.permission == req.newMetadata.permission;
}

RefreshOriginFileRsp RefreshOriginFileRecord::response() const {
  RefreshOriginFileRsp rsp;
  rsp.newInode = newInode;
  rsp.cleanupJobId = cleanupJobId;
  return rsp;
}

std::string OriginNamespaceManager::refreshKey(const Uuid &requestId) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheRefresh, requestId);
}

std::string OriginNamespaceManager::cleanupKey(const Uuid &jobId) {
  return Serializer::serRawArgs(kv::KeyPrefix::CacheCleanup, jobId);
}

CoTryTask<std::optional<RefreshOriginFileRecord>> OriginNamespaceManager::loadRefresh(kv::IReadWriteTransaction &txn,
                                                                                      const Uuid &requestId) {
  if (requestId == Uuid::zero()) co_return makeError(StatusCode::kInvalidArg, "empty refresh request id");
  auto record = co_await loadRecord<RefreshOriginFileRecord>(txn, refreshKey(requestId), false);
  CO_RETURN_ON_ERROR(record);
  if (record->has_value() && (**record).requestId != requestId) {
    co_return makeError(StatusCode::kDataCorruption, "refresh record key mismatch");
  }
  co_return record;
}

CoTryTask<Void> OriginNamespaceManager::storeRefresh(kv::IReadWriteTransaction &txn,
                                                     const RefreshOriginFileRecord &record) {
  CO_RETURN_ON_ERROR(record.valid());
  co_return co_await txn.set(refreshKey(record.requestId), serde::serialize(record));
}

CoTryTask<std::optional<OriginCleanupJobRecord>> OriginNamespaceManager::snapshotLoadCleanup(
    kv::IReadOnlyTransaction &txn,
    const Uuid &jobId) {
  if (jobId == Uuid::zero()) co_return makeError(StatusCode::kInvalidArg, "empty cleanup job id");
  auto record = co_await loadRecord<OriginCleanupJobRecord>(txn, cleanupKey(jobId), true);
  CO_RETURN_ON_ERROR(record);
  if (record->has_value() && (**record).jobId != jobId) {
    co_return makeError(StatusCode::kDataCorruption, "cleanup record key mismatch");
  }
  co_return record;
}

CoTryTask<std::optional<OriginCleanupJobRecord>> OriginNamespaceManager::loadCleanup(kv::IReadWriteTransaction &txn,
                                                                                     const Uuid &jobId) {
  if (jobId == Uuid::zero()) co_return makeError(StatusCode::kInvalidArg, "empty cleanup job id");
  auto record = co_await loadRecord<OriginCleanupJobRecord>(txn, cleanupKey(jobId), false);
  CO_RETURN_ON_ERROR(record);
  if (record->has_value() && (**record).jobId != jobId) {
    co_return makeError(StatusCode::kDataCorruption, "cleanup record key mismatch");
  }
  co_return record;
}

CoTryTask<Void> OriginNamespaceManager::storeCleanup(kv::IReadWriteTransaction &txn,
                                                     const OriginCleanupJobRecord &record) {
  CO_RETURN_ON_ERROR(record.valid());
  auto result = co_await txn.set(cleanupKey(record.jobId), serde::serialize(record));
  CO_RETURN_ON_ERROR(result);
  cache::metrics::recordCount(
      cache::metrics::Event::META_CLEANUP_JOB_STATE,
      1,
      {.inode = record.inode.u64(), .reason = std::string(magic_enum::enum_name(record.state))});
  XLOGF(DBG,
        "Cache cleanup job {} inode {} state {} cursor {}/{}",
        record.jobId,
        record.inode,
        magic_enum::enum_name(record.state),
        record.cursor,
        record.endBlock);
  co_return Void{};
}

}  // namespace hf3fs::meta::server
