#include "cache_manager/recovery/PermitRecovery.h"

#include <limits>

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
                               WallClockNsFn wallClockNs)
    : backend_(std::move(backend)),
      hints_(hints),
      managerEpoch_(managerEpoch),
      permitTtl_(permitTtl),
      pageSize_(pageSize),
      wallClockNs_(std::move(wallClockNs)) {}

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

CoTryTask<void> PermitRecovery::recover(const meta::RecoverableCachePermit &item,
                                        uint64_t nowNs,
                                        uint64_t expiresAtNs) {
  CO_RETURN_ON_ERROR(item.valid());
  auto queried = co_await backend_->queryPermit(item.permit);
  if (queried && queried->state == cache::CachePermitState::PINNED) {
    if (item.state == cache::CacheBlockState::QUEUED) {
      co_return makeError(CacheCode::kStateConflict, "queued cache permit is unexpectedly executing");
    }
    co_return Void{};
  }
  if (queried && queried->state == cache::CachePermitState::RESERVED && queried->expiresAtNs > nowNs) {
    auto renewed = co_await backend_->renewPermit(item.permit, expiresAtNs);
    CO_RETURN_ON_ERROR(renewed);
    if (renewed->state != cache::CachePermitState::RESERVED || renewed->expiresAtNs <= nowNs) {
      co_return makeError(CacheCode::kPermitExpired, "recovered permit did not remain reserved");
    }
    if (item.state == cache::CacheBlockState::QUEUED) co_return co_await attach(item);
    co_return Void{};
  }
  if (queried.hasError() && !missingPermit(queried.error())) co_return makeError(queried.error());
  if (item.state == cache::CacheBlockState::LOADING) co_return Void{};

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

CoTryTask<void> PermitRecovery::run() {
  if (!backend_ || managerEpoch_ == Uuid::zero() || permitTtl_ <= 0_ns || pageSize_ == 0) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid cache permit recovery configuration");
  }
  CO_RETURN_ON_ERROR(co_await backend_->refreshRouting());
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
      CO_RETURN_ON_ERROR(co_await recover(item, nowNs, nowNs + static_cast<uint64_t>(ttlNs)));
      cursor = item.key;
    }
    if (!page->more) break;
  }
  co_return Void{};
}

}  // namespace hf3fs::cache_manager
