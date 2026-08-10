#pragma once

#include <optional>
#include <span>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/cache/CacheBlockRecord.h"

namespace hf3fs::meta::server {

struct CacheBlockPage {
  std::vector<CacheBlockRecord> records;
  bool more{false};
};

class CacheBlockStore {
 public:
  static CoTryTask<std::optional<CacheBlockRecord>> snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                 const cache::CacheBlockKey &key);
  static CoTryTask<std::vector<std::optional<CacheBlockRecord>>> snapshotLoadBatch(
      kv::IReadOnlyTransaction &txn,
      std::span<const cache::CacheBlockKey> keys);
  static CoTryTask<CacheBlockPage> snapshotList(kv::IReadOnlyTransaction &txn,
                                                uint64_t inode,
                                                cache::CacheBlockIndex begin,
                                                uint32_t limit);
  static CoTryTask<CacheBlockPage> snapshotListRecoverable(kv::IReadOnlyTransaction &txn,
                                                           std::optional<cache::CacheBlockKey> after,
                                                           uint32_t limit);
  static CoTryTask<CacheBlockPage> snapshotListReconcile(kv::IReadWriteTransaction &txn,
                                                         std::optional<cache::CacheBlockKey> after,
                                                         uint32_t limit);
  static CoTryTask<CacheBlockPage> snapshotListState(kv::IReadOnlyTransaction &txn,
                                                     cache::CacheBlockState state,
                                                     std::optional<cache::CacheBlockKey> after,
                                                     uint32_t limit,
                                                     bool inclusive = false);
  static CoTryTask<bool> snapshotStateIndexReady(kv::IReadOnlyTransaction &txn);
  static CoTryTask<std::vector<CacheBlockRecord>> snapshotListAll(kv::IReadOnlyTransaction &txn,
                                                                  std::optional<uint64_t> inode = std::nullopt);
  static CoTryTask<std::optional<CacheBlockRecord>> load(kv::IReadWriteTransaction &txn,
                                                         const cache::CacheBlockKey &key);
  static CoTryTask<Void> store(kv::IReadWriteTransaction &txn, const CacheBlockRecord &record);
  static CoTryTask<Void> remove(kv::IReadWriteTransaction &txn, const cache::CacheBlockKey &key);

  static CoTryTask<CacheBlockRecord> enqueue(kv::IReadWriteTransaction &txn,
                                             const cache::CacheBlockKey &key,
                                             flat::ChainId chainId,
                                             uint64_t blockLength,
                                             std::optional<storage::PermitIdentity> permit = std::nullopt,
                                             std::optional<storage::PermitIdentity> expectedPermit = std::nullopt,
                                             cache::CacheBlockState expectedState = cache::CacheBlockState::NONE,
                                             Uuid expectedLoaderId = Uuid::zero(),
                                             uint64_t expectedLoadEpoch = 0);
  static CoTryTask<cache::CacheGeneration> allocateGeneration(kv::IReadWriteTransaction &txn,
                                                              const cache::CacheBlockKey &key);
  static CoTryTask<cache::EvictionEpoch> allocateEvictionEpoch(kv::IReadWriteTransaction &txn,
                                                               const cache::CacheBlockKey &key);
  static CoTryTask<CacheBlockRecord> commitCharge(kv::IReadWriteTransaction &txn,
                                                  const CacheBlockRecord &desiredRecord);
  static CoTryTask<bool> updateAccess(kv::IReadWriteTransaction &txn,
                                      const cache::CacheBlockKey &key,
                                      cache::CacheGeneration generation,
                                      UtcTime managerReceiveTime);
  static CoTryTask<Void> finishClean(kv::IReadWriteTransaction &txn,
                                     const cache::CacheBlockKey &key,
                                     cache::CleanupTerminalState terminalState);

  static std::string recordKey(const cache::CacheBlockKey &key);
  static std::string generationKey(const cache::CacheBlockKey &key);
  static std::string evictionEpochKey(const cache::CacheBlockKey &key);
};

}  // namespace hf3fs::meta::server
