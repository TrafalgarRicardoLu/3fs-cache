#include "storage/cache/retire/RetireCoordinator.h"

#include <algorithm>
#include <optional>

namespace hf3fs::storage {

CoTryTask<CoordinateCacheRetireResult> RetireCoordinator::coordinate(const CoordinateCacheRetireItem &item,
                                                                     const PhysicalDiskId &diskId) {
  CO_RETURN_ON_ERROR(item.valid());
  auto operation = store_.prepare(item, diskId, UtcClock::now());
  CO_RETURN_ON_ERROR(operation);
  if (operation->state == RetireOperationState::COMPLETED) {
    co_return CoordinateCacheRetireResult{item.operationId, true};
  }

  std::optional<Status> firstFailure;
  const auto expectedReplicaTargets = operation->item.placement.expectedReplicaTargets;
  for (auto targetId : expectedReplicaTargets) {
    if (std::binary_search(operation->durableRetiredTargets.begin(),
                           operation->durableRetiredTargets.end(),
                           targetId)) {
      continue;
    }
    RetireCacheReplicaItem replica;
    replica.key = operation->item.key;
    replica.targetId = targetId;
    replica.expectedGeneration = operation->item.expectedGeneration;
    replica.placement = operation->item.placement;
    replica.evictionEpoch = operation->item.evictionEpoch;
    replica.operationId = operation->item.operationId;
    auto retired = co_await retireReplica_(targetId, replica);
    if (retired.hasError()) {
      if (!firstFailure) firstFailure = retired.error();
      continue;
    }
    if (retired->operationId != operation->item.operationId || !retired->durableRetired) {
      if (!firstFailure) firstFailure = Status(CacheCode::kInvalidResponse, "replica did not acknowledge retirement");
      continue;
    }
    auto acknowledged = store_.acknowledge(item.operationId, targetId);
    CO_RETURN_ON_ERROR(acknowledged);
    operation = std::move(acknowledged);
  }

  if (!operation->allReplicasDurableRetired()) {
    if (firstFailure) co_return makeError(std::move(*firstFailure));
    co_return makeError(CacheCode::kUnavailable, "recorded cache replicas are not all durably retired");
  }
  auto completed = store_.complete(item.operationId);
  CO_RETURN_ON_ERROR(completed);
  co_return CoordinateCacheRetireResult{item.operationId, true};
}

}  // namespace hf3fs::storage
