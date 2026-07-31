#pragma once

#include <optional>
#include <vector>

#include "client/mgmtd/RoutingInfo.h"
#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"
#include "fbs/mgmtd/MgmtdTypes.h"

namespace hf3fs::meta::server {

struct CacheBlockLayout {
  flat::ChainId chainId;
  flat::ChainVersion chainVersion;
  std::vector<flat::TargetId> replicaTargets;
  uint64_t blockLength;
  flat::ChainTableChecksumType checksumType;
};

CoTryTask<CacheBlockLayout> resolveCacheBlockLayout(kv::IReadWriteTransaction &txn,
                                                    const cache::CacheBlockKey &key,
                                                    std::optional<uint64_t> expectedLength,
                                                    const client::RoutingInfo &routing);

}  // namespace hf3fs::meta::server
