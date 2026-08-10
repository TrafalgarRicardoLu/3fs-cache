#pragma once

#include <optional>
#include <span>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

struct PinStoreLimits {
  uint32_t maxOwnersPerBlock{1024};
  uint32_t maxBlocksPerOwner{1U << 20};

  Result<Void> valid() const;
};

struct PinPage {
  std::vector<cache::PinRecord> pins;
  bool more{false};
};

class PinStore {
 public:
  struct RenewOwnerLeaseResult {
    cache::PinOwnerLease lease;
    bool created{false};
  };
  static CoTryTask<RenewOwnerLeaseResult> renewOwnerLease(kv::IReadWriteTransaction &txn,
                                                          const cache::PinOwnerLease &lease);
  static CoTryTask<cache::PinRecord> upsert(kv::IReadWriteTransaction &txn,
                                            const cache::PinRecord &pin,
                                            const PinStoreLimits &limits = {});
  static CoTryTask<bool> remove(kv::IReadWriteTransaction &txn,
                                const cache::CacheBlockKey &key,
                                const cache::PinOwner &owner);
  // An empty key list removes every pin belonging to owner.
  static CoTryTask<uint64_t> removeByOwner(kv::IReadWriteTransaction &txn,
                                           const cache::PinOwner &owner,
                                           std::span<const cache::CacheBlockKey> keys = {});
  static CoTryTask<PinPage> snapshotListByOwner(kv::IReadOnlyTransaction &txn,
                                                const cache::PinOwner &owner,
                                                std::optional<cache::CacheBlockKey> after,
                                                uint32_t limit);
  static CoTryTask<std::vector<cache::PinRecord>> snapshotQueryActive(kv::IReadOnlyTransaction &txn,
                                                                      const cache::CacheBlockKey &key,
                                                                      uint64_t nowMs);
  // Uses non-snapshot reads so a subsequent mutation in the same transaction
  // conflicts with concurrent pin creation or renewal.
  static CoTryTask<std::vector<cache::PinRecord>> queryActive(kv::IReadWriteTransaction &txn,
                                                              const cache::CacheBlockKey &key,
                                                              uint64_t nowMs);
};

}  // namespace hf3fs::meta::server
