#pragma once

#include <map>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "common/utils/Duration.h"
#include "common/utils/Result.h"
#include "common/utils/UtcTime.h"
#include "fbs/cache/Common.h"
#include "fbs/storage/Common.h"

namespace hf3fs::cache_manager {

struct EvictionCandidate {
  cache::CacheBlockKey key;
  cache::ReadyIdentity ready;
  storage::ChainId chainId;
  uint64_t logicalBytes{0};
  UtcTime readyAt;
  UtcTime lastAccessAt;
  std::map<storage::PhysicalDiskId, uint64_t> physicalFootprintByDisk;
};

struct EvictionContext {
  std::map<storage::PhysicalDiskId, uint64_t> bytesToReleaseByDisk;
  UtcTime now;
  Duration protectionPeriod{0_ns};
};

class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;
  virtual Result<std::vector<size_t>> select(std::span<const EvictionCandidate> candidates,
                                             const EvictionContext &context) = 0;
};

Result<std::unique_ptr<EvictionPolicy>> createEvictionPolicy(std::string_view name);

Result<std::map<storage::PhysicalDiskId, uint64_t>> validateEvictionSelection(
    std::span<const EvictionCandidate> candidates,
    const EvictionContext &context,
    std::span<const size_t> selected,
    size_t batchLimit);

bool protectedFromEviction(const EvictionCandidate &candidate, const EvictionContext &context);

}  // namespace hf3fs::cache_manager
