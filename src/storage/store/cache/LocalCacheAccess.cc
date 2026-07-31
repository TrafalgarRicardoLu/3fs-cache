#include "storage/store/cache/LocalCacheAccess.h"

#include <algorithm>

namespace hf3fs::storage {

Result<bool> LocalCacheAccess::record(const ChunkId &chunkId,
                                      cache::CacheGeneration generation,
                                      uint64_t observedAtNs,
                                      uint64_t persistIntervalNs,
                                      PersistFn persist) {
  if (chunkId.data().empty() || generation == cache::CacheGeneration{} || observedAtNs == 0 || persistIntervalNs == 0 ||
      !persist) {
    return makeError(StatusCode::kInvalidArg, "invalid local cache access");
  }
  auto lock = std::unique_lock(mutex_);
  auto &entry = entries_[chunkId];
  if (entry.generation != generation) entry = Entry{generation, observedAtNs, 0};
  entry.observedAtNs = std::max(entry.observedAtNs, observedAtNs);
  if (entry.persistedAtNs != 0 &&
      (entry.observedAtNs <= entry.persistedAtNs || entry.observedAtNs - entry.persistedAtNs < persistIntervalNs)) {
    return false;
  }
  auto result = persist(generation, entry.observedAtNs);
  RETURN_ON_ERROR(result);
  if (*result) {
    entry.persistedAtNs = entry.observedAtNs;
  } else {
    entries_.erase(chunkId);
  }
  return *result;
}

}  // namespace hf3fs::storage
