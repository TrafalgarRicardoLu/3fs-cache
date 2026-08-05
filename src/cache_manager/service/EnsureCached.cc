#include "cache_manager/service/EnsureCached.h"

#include <algorithm>
#include <chrono>
#include <limits>

#include "cache/metrics/CacheMetrics.h"
#include "common/utils/MagicEnum.hpp"

namespace hf3fs::cache_manager {
namespace {

uint64_t defaultWallClockNs() { return static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000; }

uint64_t steadyTimeNs(SteadyTime now) {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
}

bool isActive(const storage::CachePermitResult &permit, uint64_t nowNs) {
  return permit.state == cache::CachePermitState::PINNED ||
         (permit.state == cache::CachePermitState::RESERVED && permit.expiresAtNs > nowNs);
}

}  // namespace

EnsureCached::EnsureCached(std::shared_ptr<CacheManagerBackend> backend,
                           HintCoalescer &hints,
                           CacheCleanupWorker *cleanupWorker,
                           AdmissionPolicy &admissionPolicy,
                           PhysicalPreflight &physicalPreflight,
                           Uuid managerEpoch,
                           Duration permitTtl,
                           SteadyClockFn steadyClock,
                           WallClockNsFn wallClockNs,
                           AttachFn attach)
    : backend_(std::move(backend)),
      hints_(hints),
      cleanupWorker_(cleanupWorker),
      admissionPolicy_(&admissionPolicy),
      physicalPreflight_(&physicalPreflight),
      managerEpoch_(managerEpoch),
      permitTtl_(permitTtl),
      steadyClock_(std::move(steadyClock)),
      wallClockNs_(wallClockNs ? std::move(wallClockNs) : defaultWallClockNs),
      attach_(std::move(attach)) {}

EnsureCachedRsp EnsureCached::respond(const meta::Inode &inode,
                                      const EnsureCachedReq &req,
                                      EnsureCachedStatus status,
                                      BypassReason bypassReason) {
  const auto &origin = inode.asOriginFile();
  lastBypassReason_.store(bypassReason, std::memory_order_relaxed);
  cache::metrics::recordCount(
      cache::metrics::Event::MANAGER_ADMISSION_RESULT,
      1,
      {.inode = inode.id.u64(),
       .block = req.beginBlock.toUnderType(),
       .originId = origin.object.originId.toUnderType(),
       .reason = bypassReason == BypassReason::NONE ? std::string(magic_enum::enum_name(status))
                                                    : std::string(magic_enum::enum_name(bypassReason))});
  return {status, bypassReason};
}

CoTryTask<EnsureCachedRsp> EnsureCached::run(const EnsureCachedReq &req) {
  auto inode = co_await backend_->stat(req.inode);
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile, "cache hint inode is not an OriginFile");
  if (auto routing = backend_->routingInfo(); routing && routing->raw()) {
    auto state = routing->raw()->cachePhase2State;
    if (state == flat::CachePhase2RolloutState::DRAINING ||
        (admissionPolicy_ && state != flat::CachePhase2RolloutState::ENABLED)) {
      co_return respond(*inode, req, EnsureCachedStatus::BYPASSED, BypassReason::ADMISSION_DISABLED);
    }
  }
  const auto &origin = inode->asOriginFile();
  if (origin.superseded || origin.cacheAdmissionDisabled) {
    co_return respond(*inode, req, EnsureCachedStatus::BYPASSED, BypassReason::ADMISSION_DISABLED);
  }
  if (req.beginBlock.toUnderType() > std::numeric_limits<uint32_t>::max() - req.blockCount) {
    co_return makeError(StatusCode::kInvalidArg, "cache hint block range overflow");
  }

  auto blockSize = uint64_t{inode->fileLayout().chunkSize};
  std::vector<meta::CacheBlockRequestBase> items;
  items.reserve(req.blockCount);
  for (uint32_t i = 0; i < req.blockCount; ++i) {
    auto block = cache::CacheBlockIndex{req.beginBlock.toUnderType() + i};
    auto offset = uint64_t{block.toUnderType()} * blockSize;
    if (offset >= inode->fileLength()) break;
    items.push_back({{inode->id.u64(), block}, std::min(blockSize, inode->fileLength() - offset)});
  }
  if (items.empty()) co_return respond(*inode, req, EnsureCachedStatus::BYPASSED, BypassReason::EMPTY_RANGE);

  if (admissionPolicy_ && physicalPreflight_) co_return co_await runPhase2(req, *inode, items);
  co_return co_await runLegacy(req, *inode, items);
}

CoTryTask<EnsureCachedRsp> EnsureCached::runLegacy(const EnsureCachedReq &req,
                                                   const meta::Inode &inode,
                                                   const std::vector<meta::CacheBlockRequestBase> &items) {
  auto enqueued = co_await backend_->enqueue(items);
  CO_RETURN_ON_ERROR(enqueued);
  if (enqueued->results.size() != items.size()) {
    co_return makeError(CacheCode::kInvalidResponse, "invalid enqueue result count");
  }
  bool accepted = false;
  bool attached = false;
  bool capacityBypass = false;
  for (size_t i = 0; i < items.size(); ++i) {
    const auto &result = enqueued->results[i];
    if (result.hasError()) {
      if (result.error().code() == CacheCode::kCapacityExceeded) {
        capacityBypass = true;
        continue;
      }
      co_return makeError(result.error());
    }
    switch (result->state) {
      case cache::CacheBlockState::QUEUED: {
        auto fresh = hints_.enqueue({inode.id, items[i].key.block, items[i].blockLength, req.reason, req.priority});
        accepted = accepted || fresh;
        attached = attached || !fresh;
        break;
      }
      case cache::CacheBlockState::LOADING:
      case cache::CacheBlockState::READY:
        attached = true;
        break;
      case cache::CacheBlockState::CLEANING:
        attached = true;
        if (cleanupWorker_) {
          (void)co_await cleanupWorker_->clean(
              {items[i].key, std::nullopt, std::nullopt, cache::CleanupTerminalState::FAILED});
        }
        break;
      default:
        break;
    }
  }
  cache::metrics::setGauge(cache::metrics::Event::MANAGER_QUEUE, hints_.size());
  if (accepted) co_return respond(inode, req, EnsureCachedStatus::ACCEPTED);
  if (attached) co_return respond(inode, req, EnsureCachedStatus::ATTACHED);
  co_return respond(inode,
                    req,
                    EnsureCachedStatus::BYPASSED,
                    capacityBypass ? BypassReason::CAPACITY : BypassReason::EMPTY_RANGE);
}

CoTryTask<meta::EnqueueCacheBlocksRsp> EnsureCached::enqueueWithRetry(const meta::CacheBlockRequestBase &item) {
  auto result = co_await backend_->enqueue({item});
  if (result.hasError() && result.error().code() == RPCCode::kTimeout) {
    result = co_await backend_->enqueue({item});
  }
  co_return result;
}

CoTryTask<void> EnsureCached::release(const storage::PermitIdentity &permit) {
  co_return co_await backend_->releasePermit(permit);
}

CoTryTask<void> EnsureCached::cancelQueued(const cache::CacheBlockKey &key, const storage::PermitIdentity &permit) {
  CO_RETURN_ON_ERROR(co_await backend_->cancelQueuedAdmission(key, permit));
  co_return co_await release(permit);
}

Result<bool> EnsureCached::attach(LoadHint hint) {
  if (attach_) return attach_(std::move(hint));
  return hints_.attach(std::move(hint));
}

CoTryTask<EnsureCachedRsp> EnsureCached::runPhase2(const EnsureCachedReq &req,
                                                   const meta::Inode &inode,
                                                   const std::vector<meta::CacheBlockRequestBase> &items) {
  bool accepted = false;
  bool attached = false;
  bool capacityBypass = false;
  bool unavailableBypass = false;
  bool policyBypass = false;
  const auto ttlNs = static_cast<uint64_t>(permitTtl_.count());

  for (const auto &base : items) {
    auto receivedAt = steadyClock_();
    auto decision = admissionPolicy_->evaluate({base.key,
                                                steadyTimeNs(receivedAt),
                                                req.reason,
                                                static_cast<uint32_t>(std::max(req.priority, int32_t{0})),
                                                {},
                                                req.reason != EnsureReason::FOREGROUND_MISS,
                                                false});
    if (decision.action != AdmissionAction::ADMIT) {
      policyBypass = true;
      continue;
    }

    auto attemptId = Uuid::random();
    auto permit =
        co_await backend_->makePermit(inode, base.key.block, base.blockLength, managerEpoch_, attemptId, uint64_t{1});
    if (permit.hasError()) {
      unavailableBypass = true;
      continue;
    }
    auto reservation = physicalPreflight_->tryReserve(permit->placement.versionedChain.chainId,
                                                      permit->footprintByTarget,
                                                      steadyClock_());
    if (reservation.hasError()) {
      if (reservation.error().code() == CacheCode::kCapacityExceeded) {
        capacityBypass = true;
      } else {
        unavailableBypass = true;
      }
      continue;
    }

    auto nowNs = wallClockNs_();
    if (ttlNs == 0 || nowNs > std::numeric_limits<uint64_t>::max() - ttlNs) {
      unavailableBypass = true;
      continue;
    }
    auto expiresAtNs = nowNs + ttlNs;
    auto prepared = co_await backend_->preparePermit(*permit, expiresAtNs);
    if (prepared.hasError() || !isActive(*prepared, nowNs)) {
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      if (prepared.hasError() && prepared.error().code() == CacheCode::kCapacityExceeded) {
        capacityBypass = true;
      } else {
        unavailableBypass = true;
      }
      continue;
    }

    auto item = base;
    item.permit = *permit;
    auto enqueued = co_await enqueueWithRetry(item);
    if (enqueued.hasError()) {
      if (enqueued.error().code() != RPCCode::kTimeout) {
        auto released = co_await release(*permit);
        if (released.hasError()) co_return makeError(released.error());
      }
      unavailableBypass = true;
      continue;
    }
    if (enqueued->results.size() != 1 || enqueued->permits.size() != 1 || enqueued->results.front().hasError()) {
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      if (enqueued->results.size() == 1 && enqueued->results.front().hasError() &&
          enqueued->results.front().error().code() == CacheCode::kCapacityExceeded) {
        capacityBypass = true;
      } else {
        unavailableBypass = true;
      }
      continue;
    }
    const auto &result = *enqueued->results.front();
    const auto &persistedPermit = enqueued->permits.front();
    if (result.enqueueOutcome == cache::CacheEnqueueOutcome::CREATED ||
        (result.enqueueOutcome == cache::CacheEnqueueOutcome::QUEUED &&
         persistedPermit == std::optional<storage::PermitIdentity>{*permit})) {
      auto fresh = attach({inode.id, base.key.block, base.blockLength, req.reason, req.priority});
      if (fresh.hasError()) {
        CO_RETURN_ON_ERROR(co_await cancelQueued(base.key, *permit));
        unavailableBypass = true;
        continue;
      }
      accepted = accepted || *fresh;
      attached = attached || !*fresh;
      continue;
    }

    if (result.enqueueOutcome == cache::CacheEnqueueOutcome::READY || result.state == cache::CacheBlockState::READY) {
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      attached = true;
      continue;
    }
    if (result.enqueueOutcome == cache::CacheEnqueueOutcome::LOADING ||
        result.state == cache::CacheBlockState::LOADING) {
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      attached = true;
      continue;
    }
    if (result.enqueueOutcome != cache::CacheEnqueueOutcome::QUEUED || result.state != cache::CacheBlockState::QUEUED ||
        !persistedPermit.has_value()) {
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      unavailableBypass = true;
      continue;
    }

    const auto existing = *persistedPermit;
    auto queried = co_await backend_->queryPermit(existing);
    if (queried.hasError()) {
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      unavailableBypass = true;
      continue;
    }
    if (isActive(*queried, nowNs)) {
      auto renewed = co_await backend_->renewPermit(existing, expiresAtNs);
      auto released = co_await release(*permit);
      if (released.hasError()) co_return makeError(released.error());
      if (renewed.hasError() || !isActive(*renewed, nowNs)) {
        unavailableBypass = true;
        continue;
      }
      auto fresh = attach({inode.id, base.key.block, base.blockLength, req.reason, req.priority});
      if (fresh.hasError()) {
        CO_RETURN_ON_ERROR(co_await cancelQueued(base.key, existing));
        unavailableBypass = true;
        continue;
      }
      accepted = accepted || *fresh;
      attached = attached || !*fresh;
      continue;
    }

    auto replacement = storage::PermitIdentity{managerEpoch_,
                                               existing.placement,
                                               existing.permitGeneration + 1,
                                               permit->footprintByTarget};
    auto released = co_await release(*permit);
    if (released.hasError()) co_return makeError(released.error());
    if (existing.permitGeneration == std::numeric_limits<uint64_t>::max() || replacement.valid().hasError()) {
      unavailableBypass = true;
      continue;
    }
    auto replacementPrepared = co_await backend_->preparePermit(replacement, expiresAtNs);
    if (replacementPrepared.hasError() || !isActive(*replacementPrepared, nowNs)) {
      auto replacementReleased = co_await release(replacement);
      if (replacementReleased.hasError()) co_return makeError(replacementReleased.error());
      unavailableBypass = true;
      continue;
    }
    auto replacementItem = base;
    replacementItem.permit = replacement;
    replacementItem.expectedPermit = existing;
    replacementItem.expectedState = cache::CacheBlockState::QUEUED;
    auto replaced = co_await enqueueWithRetry(replacementItem);
    if (replaced.hasError()) {
      if (replaced.error().code() != RPCCode::kTimeout) {
        auto replacementReleased = co_await release(replacement);
        if (replacementReleased.hasError()) co_return makeError(replacementReleased.error());
      }
      unavailableBypass = true;
      continue;
    }
    if (replaced->results.size() != 1 || replaced->permits.size() != 1 || replaced->results.front().hasError() ||
        replaced->permits.front() != std::optional<storage::PermitIdentity>{replacement}) {
      auto replacementReleased = co_await release(replacement);
      if (replacementReleased.hasError()) co_return makeError(replacementReleased.error());
      unavailableBypass = true;
      continue;
    }
    auto fresh = attach({inode.id, base.key.block, base.blockLength, req.reason, req.priority});
    if (fresh.hasError()) {
      CO_RETURN_ON_ERROR(co_await cancelQueued(base.key, replacement));
      unavailableBypass = true;
      continue;
    }
    accepted = accepted || *fresh;
    attached = attached || !*fresh;
  }

  cache::metrics::setGauge(cache::metrics::Event::MANAGER_QUEUE, hints_.size());
  if (accepted) co_return respond(inode, req, EnsureCachedStatus::ACCEPTED);
  if (attached) co_return respond(inode, req, EnsureCachedStatus::ATTACHED);
  if (capacityBypass) co_return respond(inode, req, EnsureCachedStatus::BYPASSED, BypassReason::CAPACITY);
  if (unavailableBypass) co_return respond(inode, req, EnsureCachedStatus::BYPASSED, BypassReason::UNAVAILABLE);
  co_return respond(inode,
                    req,
                    EnsureCachedStatus::BYPASSED,
                    policyBypass ? BypassReason::POLICY : BypassReason::EMPTY_RANGE);
}

}  // namespace hf3fs::cache_manager
