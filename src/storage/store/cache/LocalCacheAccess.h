#pragma once

#include <functional>
#include <mutex>
#include <unordered_map>

#include "fbs/storage/Common.h"

namespace hf3fs::storage {

class LocalCacheAccess {
 public:
  using PersistFn = std::function<Result<bool>(cache::CacheGeneration, uint64_t)>;

  Result<bool> record(const ChunkId &chunkId,
                      cache::CacheGeneration generation,
                      uint64_t observedAtNs,
                      uint64_t persistIntervalNs,
                      PersistFn persist);

 private:
  struct Entry {
    cache::CacheGeneration generation{};
    uint64_t observedAtNs{0};
    uint64_t persistedAtNs{0};
  };

  std::mutex mutex_;
  std::unordered_map<ChunkId, Entry> entries_;
};

}  // namespace hf3fs::storage
