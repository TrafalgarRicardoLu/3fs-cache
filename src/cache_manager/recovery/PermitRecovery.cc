#include "cache_manager/recovery/PermitRecovery.h"

#include <limits>

#include "cache/metrics/CacheMetrics.h"
#include "cache_manager/cleanup/CacheCleanupWorker.h"

namespace hf3fs::cache_manager {
namespace {

uint64_t recoveryWallClockNs() { return static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000; }

bool missingPermit(const Status &error) {
  return error.code() == CacheCode::kNotFound || error.code() == CacheCode::kPermitExpired;
}

}  // namespace

PermitRecovery::PermitRecovery(std::shared_ptr<CacheManagerBackend> backend,
                               HintCoalescer &hints,
                               Uuid managerEpoch,
                               Duration permitTtl,
                               uint32_t pageSize,
                               WallClockNsFn wallClockNs,
                               CacheCleanupWorker *cleanupWorker,
                               bool recoverLoading)
    : backend_(std::move(backend)),
      hints_(hints),
      managerEpoch_(managerEpoch),
      permitTtl_(permitTtl),
      pageSize_(pageSize),
      wallClockNs_(std::move(wallClockNs)),
      cleanupWorker_(cleanupWorker),
      recoverLoading_(recoverLoading) {}

CoTryTask<void> PermitRecovery::attach(const meta::RecoverableCachePermit &item) {
  (void)hints_.enqueue(
      {meta::InodeId{item.key.inode}, item.key.block, item.blockLength, EnsureReason::FOREGROUND_MISS, int32_t{0}});
  co_return Void{};
}

CoTryTask<void> PermitRecovery::cancel(const meta::RecoverableCachePermit &item) {
  CO_RETURN_ON_ERROR(co_await backend_->cancelQueuedAdmission(item.key, item.permit));
  CO_RETURN_ON_ERROR(co_await backend_->releasePermit(item.permit));
  co_return Void{};
}

CoTryTask<void> PermitRecovery::recoverLoading(const meta::RecoverableCachePermit &item, uint64_t nowNs) {
  if (!cleanupWorker_ || item.leaseExpiresAt.isZero() || item.cacheGeneration == cache::CacheGeneration{} ||
      !item.placement || *item.placement != item.permit.placement) {
    co_return makeError(CacheCode::kInvalidResponse, "loading recovery item is missing its phase four fence");
  }
  auto leaseUs = item.leaseExpiresAt.toMicroseconds();
  if (leaseUs <= 0 || static_cast<uint64_t>(leaseUs) > std::numeric_limits<uint64_t>::max() / 1000) {
    co_return makeError(CacheCode::kInvalidResponse, "invalid loading recovery lease deadline");
  }
  if (static_cast<uint64_t>(leaseUs) * 1000 > nowNs) {
    cache::metrics::recordCount(cache::metrics::Event::MANAGER_LEASE_RECOVERY, 1, {.reason = "deferred"});
    co_return Void{};
  }

  meta::RecoverExpiredCacheLoadItem recovery;
  recovery.key = item.key;
  recovery.loaderId = item.loaderId;
  recovery.loadEpoch = item.loadEpoch;
  recovery.expectedLeaseExpiresAt = item.leaseExpiresAt;
  recovery.expectedGeneration = item.cacheGeneration;
  recovery.expectedPermit = item.permit;
  recovery.expectedPlacement = *item.placement;
  recovery.terminalState = cache::CleanupTerminalState::REENQUEUE;
  auto recovered = co_await backend_->recoverExpiredLoad(recovery);
  CO_RETURN_ON_ERROR(recovered);
  if (recovered->key != item.key || recovered->state != cache::CacheBlockState::CLEANING) {
    co_return makeError(CacheCode::kInvalidResponse, "expired loading recovery did not enter cleanup");
  }
  meta::BeginCleanCacheBlockItem cleanup;
  cleanup.key = item.key;
  cleanup.terminalState = cache::CleanupTerminalState::REENQUEUE;
  CO_RETURN_ON_ERROR(co_await cleanupWorker_->clean(cleanup));
  auto released = co_await backend_->releasePermit(item.permit);
  if (released.hasError() && !missingPermit(released.error())) CO_RETURN_ERROR(released);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_LEASE_RECOVERY, 1, {.reason = "recovered"});
  co_return Void{};
}

CoTryTask<void> PermitRecovery::recover(const meta::RecoverableCachePermit &item,
                                        uint64_t nowNs,
                                        uint64_t expiresAtNs) {
  CO_RETURN_ON_ERROR(item.valid());
  auto queried = co_await backend_->queryPermit(item.permit);
  if (queried && queried->permit != item.permit) {
    co_return makeError(CacheCode::kInvalidResponse, "queried cache permit identity changed");
  }
  if (queried && queried->state == cache::CachePermitState::PINNED) {
    if (item.state == cache::CacheBlockState::QUEUED) {
      co_return makeError(CacheCode::kStateConflict, "queued cache permit is unexpectedly executing");
    }
    if (recoverLoading_) co_return co_await recoverLoading(item, nowNs);
    co_return Void{};
  }
  if (queried && queried->state == cache::CachePermitState::RESERVED && queried->expiresAtNs > nowNs) {
    auto renewed = co_await backend_->renewPermit(item.permit, expiresAtNs);
    CO_RETURN_ON_ERROR(renewed);
    if (renewed->state != cache::CachePermitState::RESERVED || renewed->expiresAtNs <= nowNs) {
      co_return makeError(CacheCode::kPermitExpired, "recovered permit did not remain reserved");
    }
    if (item.state == cache::CacheBlockState::QUEUED) co_return co_await attach(item);
    if (recoverLoading_) co_return co_await recoverLoading(item, nowNs);
    co_return Void{};
  }
  if (queried.hasError() && !missingPermit(queried.error())) co_return makeError(queried.error());
  if (item.state == cache::CacheBlockState::LOADING) {
    if (recoverLoading_) co_return co_await recoverLoading(item, nowNs);
    co_return Void{};
  }

  if (item.permit.permitGeneration == std::numeric_limits<uint64_t>::max()) {
    co_return co_await cancel(item);
  }
  auto replacement = item.permit;
  replacement.managerEpoch = managerEpoch_;
  ++replacement.permitGeneration;
  CO_RETURN_ON_ERROR(replacement.valid());
  auto prepared = co_await backend_->preparePermit(replacement, expiresAtNs);
  if (prepared.hasError()) {
    if (prepared.error().code() == CacheCode::kCapacityExceeded ||
        prepared.error().code() == CacheCode::kPermitExpired) {
      co_return co_await cancel(item);
    }
    co_return makeError(prepared.error());
  }
  if (prepared->state != cache::CachePermitState::RESERVED || prepared->expiresAtNs <= nowNs) {
    (void)co_await backend_->releasePermit(replacement);
    co_return makeError(CacheCode::kPermitExpired, "replacement permit is not reserved");
  }

  meta::CacheBlockRequestBase replacementItem;
  replacementItem.key = item.key;
  replacementItem.blockLength = item.blockLength;
  replacementItem.permit = replacement;
  replacementItem.expectedPermit = item.permit;
  replacementItem.expectedState = cache::CacheBlockState::QUEUED;
  auto replaced = co_await backend_->enqueue({replacementItem});
  if (replaced.hasError() && replaced.error().code() == RPCCode::kTimeout) {
    replaced = co_await backend_->enqueue({replacementItem});
  }
  if (replaced.hasError()) {
    if (replaced.error().code() != RPCCode::kTimeout) (void)co_await backend_->releasePermit(replacement);
    if (replaced.error().code() == CacheCode::kStateConflict) co_return Void{};
    co_return makeError(replaced.error());
  }
  if (replaced->results.size() != 1 || replaced->permits.size() != 1 || replaced->results.front().hasError() ||
      replaced->permits.front() != std::optional<storage::PermitIdentity>{replacement}) {
    (void)co_await backend_->releasePermit(replacement);
    if (replaced->results.size() == 1 && replaced->results.front().hasError() &&
        replaced->results.front().error().code() == CacheCode::kStateConflict) {
      co_return Void{};
    }
    co_return makeError(CacheCode::kInvalidResponse, "invalid queued permit replacement result");
  }
  CO_RETURN_ON_ERROR(co_await backend_->releasePermit(item.permit));
  co_return co_await attach(item);
}

CoTryTask<void> PermitRecovery::run(bool refreshRouting) {
  if (!backend_ || managerEpoch_ == Uuid::zero() || permitTtl_ <= 0_ns || pageSize_ == 0) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid cache permit recovery configuration");
  }
  if (refreshRouting) CO_RETURN_ON_ERROR(co_await backend_->refreshRouting());
  std::optional<cache::CacheBlockKey> cursor;
  while (true) {
    auto page = co_await backend_->listRecoverablePermits(cursor, pageSize_);
    CO_RETURN_ON_ERROR(page);
    if (page->more && page->items.empty()) {
      co_return makeError(CacheCode::kInvalidResponse, "empty cache permit recovery page has more data");
    }
    for (const auto &item : page->items) {
      auto nowNs = wallClockNs_ ? wallClockNs_() : recoveryWallClockNs();
      auto ttlNs = permitTtl_.count();
      if (ttlNs <= 0 || nowNs > std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(ttlNs)) {
        co_return makeError(CacheCode::kPermitExpired, "invalid cache permit recovery deadline");
      }
      auto recovered = co_await recover(item, nowNs, nowNs + static_cast<uint64_t>(ttlNs));
      if (recovered.hasError() && recoverLoading_ && item.state == cache::CacheBlockState::LOADING) {
        cache::metrics::recordCount(cache::metrics::Event::MANAGER_LEASE_RECOVERY, 1, {.reason = "failed"});
      }
      CO_RETURN_ON_ERROR(recovered);
      cursor = item.key;
    }
    if (!page->more) break;
  }
  co_return Void{};
}

}  // namespace hf3fs::cache_manager
