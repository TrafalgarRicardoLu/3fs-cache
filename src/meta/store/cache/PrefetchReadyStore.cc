#include "meta/store/cache/PrefetchReadyStore.h"

#include <limits>

#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/PrefetchJobStore.h"

namespace hf3fs::meta::server {
namespace {

Result<Void> add(uint64_t &value, uint64_t increment, std::string_view name) {
  if (increment > std::numeric_limits<uint64_t>::max() - value) {
    return makeError(CacheCode::kStateConflict, std::string(name) + " overflow");
  }
  value += increment;
  return Void{};
}

bool terminal(cache::PrefetchJobState state) {
  return state == cache::PrefetchJobState::READY || state == cache::PrefetchJobState::FAILED ||
         state == cache::PrefetchJobState::CANCELLED;
}

}  // namespace

CoTryTask<TrackPrefetchReadyResult> PrefetchReadyStore::track(kv::IReadWriteTransaction &txn,
                                                              cache::PrefetchJobId jobId,
                                                              std::optional<cache::CacheBlockKey> after,
                                                              uint32_t limit) {
  auto loaded = co_await PrefetchJobStore::load(txn, jobId);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch job not found");
  auto job = std::move(**loaded);
  if (terminal(job.state)) co_return makeError(CacheCode::kStateConflict, "terminal prefetch job cannot be tracked");
  auto page = co_await PrefetchPlanStore::list(txn, jobId, after, limit);
  CO_RETURN_ON_ERROR(page);

  TrackPrefetchReadyResult result;
  result.more = page->more;
  if (!page->entries.empty()) result.nextAfter = page->entries.back().key;
  uint64_t achievedBytes = 0;
  uint64_t achievedBlocks = 0;
  for (const auto &entry : page->entries) {
    auto cacheBlock = co_await CacheBlockStore::load(txn, entry.key);
    CO_RETURN_ON_ERROR(cacheBlock);
    const bool cacheReady = cacheBlock->has_value() && (*cacheBlock)->state == cache::CacheBlockState::READY &&
                            (*cacheBlock)->ready.has_value() && (*cacheBlock)->blockLength == entry.blockLength &&
                            (*cacheBlock)->ready->blockLength == entry.blockLength;
    if (entry.state == cache::PrefetchPlanEntryState::READY) {
      if (cacheReady && entry.ready == (*cacheBlock)->ready) {
        CO_RETURN_ON_ERROR(add(result.currentReadyBytes, entry.blockLength, "current ready bytes"));
        CO_RETURN_ON_ERROR(add(result.currentReadyBlocks, 1, "current ready blocks"));
      }
      continue;
    }
    if (!cacheReady || (entry.state != cache::PrefetchPlanEntryState::ADMITTED &&
                        entry.state != cache::PrefetchPlanEntryState::ATTACHED)) {
      continue;
    }
    auto desired = entry;
    desired.state = cache::PrefetchPlanEntryState::READY;
    desired.ready = (*cacheBlock)->ready;
    CO_RETURN_ON_ERROR(co_await PrefetchPlanStore::update(txn, entry, desired));
    CO_RETURN_ON_ERROR(add(achievedBytes, entry.blockLength, "achieved ready bytes"));
    CO_RETURN_ON_ERROR(add(achievedBlocks, 1, "achieved ready blocks"));
    CO_RETURN_ON_ERROR(add(result.currentReadyBytes, entry.blockLength, "current ready bytes"));
    CO_RETURN_ON_ERROR(add(result.currentReadyBlocks, 1, "current ready blocks"));
  }

  if (achievedBlocks != 0) {
    CO_RETURN_ON_ERROR(add(job.readyBytes, achievedBytes, "Job ready bytes"));
    CO_RETURN_ON_ERROR(add(job.readyBlocks, achievedBlocks, "Job ready blocks"));
    if (job.stateVersion == std::numeric_limits<uint64_t>::max() ||
        job.updatedAtMs == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "prefetch Job tracker version exhausted");
    }
    ++job.stateVersion;
    ++job.updatedAtMs;
    auto updated = co_await PrefetchJobStore::update(txn, job.stateVersion - 1, job);
    CO_RETURN_ON_ERROR(updated);
    result.job = std::move(*updated);
  } else {
    result.job = std::move(job);
  }
  co_return result;
}

}  // namespace hf3fs::meta::server
