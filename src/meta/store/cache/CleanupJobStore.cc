#include "meta/store/cache/CleanupJobStore.h"

#include <algorithm>

#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {

CoTryTask<std::optional<OriginCleanupJobRecord>> CleanupJobStore::snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                               const Uuid &jobId) {
  co_return co_await OriginNamespaceManager::snapshotLoadCleanup(txn, jobId);
}

CoTryTask<std::optional<OriginCleanupJobRecord>> CleanupJobStore::load(kv::IReadWriteTransaction &txn,
                                                                       const Uuid &jobId) {
  co_return co_await OriginNamespaceManager::loadCleanup(txn, jobId);
}

CoTryTask<Void> CleanupJobStore::store(kv::IReadWriteTransaction &txn, const OriginCleanupJobRecord &record) {
  co_return co_await OriginNamespaceManager::storeCleanup(txn, record);
}

CoTryTask<OriginCleanupJobRecord> CleanupJobStore::advance(kv::IReadWriteTransaction &txn, const Uuid &jobId) {
  auto loaded = co_await load(txn, jobId);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "cleanup job not found");
  auto job = std::move(**loaded);
  if (job.complete()) co_return job;

  if (job.beginBlock == job.endBlock) {
    job.state = OriginCleanupJobState::COMPLETE;
    CO_RETURN_ON_ERROR(co_await store(txn, job));
    co_return job;
  }

  // Start a fresh pass after the preceding pass reached the end with failures.
  if (job.cursor == job.beginBlock && job.state == OriginCleanupJobState::RUNNING && job.lastBatchEnd == job.endBlock) {
    job.lastBatchBegin = job.beginBlock;
    job.lastBatchEnd = job.beginBlock;
    job.lastBatchSucceeded = 0;
    job.lastBatchFailed = 0;
    job.remainingNonTerminalBlocks = 0;
    job.remainingChargedBytes = 0;
  }

  const auto begin = job.cursor;
  const auto end = std::min(job.endBlock, begin + uint64_t{kMaxCacheBatchItems});
  uint32_t succeeded = 0;
  uint32_t failed = 0;
  uint64_t chargedBytes = 0;
  for (auto block = begin; block < end; ++block) {
    auto record = co_await CacheBlockStore::load(
        txn,
        cache::CacheBlockKey{job.inode.u64(), cache::CacheBlockIndex{static_cast<uint32_t>(block)}});
    CO_RETURN_ON_ERROR(record);
    if (!record->has_value() ||
        ((**record).state == cache::CacheBlockState::FAILED && (**record).chargeKind == cache::ChargeKind::NONE)) {
      ++succeeded;
    } else {
      ++failed;
      chargedBytes += (**record).chargedBytes;
    }
  }

  job.state = OriginCleanupJobState::RUNNING;
  job.cursor = end;
  job.lastBatchBegin = begin;
  job.lastBatchEnd = end;
  job.lastBatchSucceeded = succeeded;
  job.lastBatchFailed = failed;
  job.remainingNonTerminalBlocks += failed;
  job.remainingChargedBytes += chargedBytes;
  if (job.cursor == job.endBlock) {
    if (job.remainingNonTerminalBlocks == 0 && job.remainingChargedBytes == 0) {
      job.state = OriginCleanupJobState::COMPLETE;
    } else {
      job.cursor = job.beginBlock;
    }
  }
  CO_RETURN_ON_ERROR(co_await store(txn, job));
  co_return job;
}

}  // namespace hf3fs::meta::server
