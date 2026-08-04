#pragma once

#include <optional>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

struct CreatePrefetchJobResult {
  cache::PrefetchJobRecord job;
  bool created{false};
};

struct PrefetchJobPage {
  std::vector<cache::PrefetchJobRecord> jobs;
  bool more{false};
};

class PrefetchJobStore {
 public:
  static CoTryTask<CreatePrefetchJobResult> create(kv::IReadWriteTransaction &txn, const cache::PrefetchJobRecord &job);
  static CoTryTask<std::optional<cache::PrefetchJobRecord>> snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                         cache::PrefetchJobId jobId);
  static CoTryTask<std::optional<cache::PrefetchJobRecord>> load(kv::IReadWriteTransaction &txn,
                                                                 cache::PrefetchJobId jobId);
  static CoTryTask<PrefetchJobPage> snapshotList(kv::IReadOnlyTransaction &txn,
                                                 std::optional<flat::Uid> ownerUid,
                                                 std::optional<cache::PrefetchJobId> after,
                                                 uint32_t limit);
  static CoTryTask<cache::PrefetchJobRecord> update(kv::IReadWriteTransaction &txn,
                                                    uint64_t expectedStateVersion,
                                                    const cache::PrefetchJobRecord &job);
};

}  // namespace hf3fs::meta::server
