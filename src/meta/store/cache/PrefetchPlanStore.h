#pragma once

#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

struct AppendPrefetchPlanResult {
  uint64_t insertedBlocks{0};
  uint64_t insertedBytes{0};
  cache::PrefetchJobRecord job;
};

struct PrefetchPlanPage {
  std::vector<cache::PrefetchPlanEntry> entries;
  bool more{false};
};

class PrefetchPlanStore {
 public:
  static CoTryTask<AppendPrefetchPlanResult> append(kv::IReadWriteTransaction &txn,
                                                    cache::PrefetchJobId jobId,
                                                    std::span<const cache::PrefetchPlanEntry> entries,
                                                    uint32_t plannerSourceIndex,
                                                    std::string_view plannerCursor,
                                                    bool planningComplete);
  static CoTryTask<PrefetchPlanPage> snapshotList(kv::IReadOnlyTransaction &txn,
                                                  cache::PrefetchJobId jobId,
                                                  std::optional<cache::CacheBlockKey> after,
                                                  uint32_t limit);
  static CoTryTask<PrefetchPlanPage> list(kv::IReadWriteTransaction &txn,
                                          cache::PrefetchJobId jobId,
                                          std::optional<cache::CacheBlockKey> after,
                                          uint32_t limit);
  static CoTryTask<cache::PrefetchPlanEntry> update(kv::IReadWriteTransaction &txn,
                                                    const cache::PrefetchPlanEntry &expected,
                                                    const cache::PrefetchPlanEntry &desired);
};

}  // namespace hf3fs::meta::server
