#pragma once

#include <optional>

#include "common/serde/Serde.h"
#include "common/utils/Result.h"
#include "common/utils/UtcTime.h"
#include "common/utils/Uuid.h"
#include "fbs/cache/Common.h"
#include "fbs/mgmtd/MgmtdTypes.h"
#include "fbs/storage/Common.h"

namespace hf3fs::meta::server {

struct CacheBlockRecord {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(state, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(chainId, flat::ChainId{});
  SERDE_STRUCT_FIELD(blockLength, uint64_t{0});
  SERDE_STRUCT_FIELD(loaderId, Uuid::zero());
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{0});
  SERDE_STRUCT_FIELD(cacheGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(leaseExpiresAt, UtcTime{});
  SERDE_STRUCT_FIELD(ready, std::optional<cache::ReadyIdentity>{});
  SERDE_STRUCT_FIELD(chargeKind, cache::ChargeKind::NONE);
  SERDE_STRUCT_FIELD(chargedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(cleanupEpoch, cache::CleanupEpoch{});
  SERDE_STRUCT_FIELD(terminalState, cache::CleanupTerminalState::NONE);
  SERDE_STRUCT_FIELD(deleteGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(permit, std::optional<storage::PermitIdentity>{});
  SERDE_STRUCT_FIELD(placement, std::optional<storage::PlacementIdentity>{});
  SERDE_STRUCT_FIELD(committedPermit, std::optional<storage::PermitIdentity>{});
  SERDE_STRUCT_FIELD(readyAt, UtcTime{});
  SERDE_STRUCT_FIELD(lastAccessAt, UtcTime{});

 public:
  Result<Void> valid() const;
};

}  // namespace hf3fs::meta::server
