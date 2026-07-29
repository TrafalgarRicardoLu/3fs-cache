#pragma once

#include <optional>
#include <span>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/cache/CacheBlockRecord.h"

namespace hf3fs::meta::server {

class CacheBlockStore {
 public:
  static CoTryTask<std::optional<CacheBlockRecord>> snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                 const cache::CacheBlockKey &key);
  static CoTryTask<std::vector<std::optional<CacheBlockRecord>>> snapshotLoadBatch(
      kv::IReadOnlyTransaction &txn,
      std::span<const cache::CacheBlockKey> keys);
  static CoTryTask<std::optional<CacheBlockRecord>> load(kv::IReadWriteTransaction &txn,
                                                         const cache::CacheBlockKey &key);
  static CoTryTask<Void> store(kv::IReadWriteTransaction &txn, const CacheBlockRecord &record);
  static CoTryTask<Void> remove(kv::IReadWriteTransaction &txn, const cache::CacheBlockKey &key);

  static CoTryTask<CacheBlockRecord> enqueue(kv::IReadWriteTransaction &txn,
                                             const cache::CacheBlockKey &key,
                                             flat::ChainId chainId,
                                             uint64_t blockLength);
  static CoTryTask<cache::CacheGeneration> allocateGeneration(kv::IReadWriteTransaction &txn,
                                                              const cache::CacheBlockKey &key);
  static CoTryTask<CacheBlockRecord> commitCharge(kv::IReadWriteTransaction &txn,
                                                  const CacheBlockRecord &desiredRecord);
  static CoTryTask<Void> finishClean(kv::IReadWriteTransaction &txn,
                                     const cache::CacheBlockKey &key,
                                     cache::CleanupTerminalState terminalState);

  static std::string recordKey(const cache::CacheBlockKey &key);
  static std::string generationKey(const cache::CacheBlockKey &key);
};

}  // namespace hf3fs::meta::server
