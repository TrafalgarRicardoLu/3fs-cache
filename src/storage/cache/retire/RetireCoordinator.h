#pragma once

#include <functional>

#include "common/utils/Coroutine.h"
#include "storage/cache/retire/RetireOperationStore.h"

namespace hf3fs::storage {

class RetireCoordinator {
 public:
  using RetireReplica = std::function<CoTryTask<RetireCacheReplicaResult>(TargetId, const RetireCacheReplicaItem &)>;

  RetireCoordinator(RetireOperationStore &store, RetireReplica retireReplica)
      : store_(store),
        retireReplica_(std::move(retireReplica)) {}

  CoTryTask<CoordinateCacheRetireResult> coordinate(const CoordinateCacheRetireItem &item,
                                                    const PhysicalDiskId &diskId);

 private:
  RetireOperationStore &store_;
  RetireReplica retireReplica_;
};

}  // namespace hf3fs::storage
