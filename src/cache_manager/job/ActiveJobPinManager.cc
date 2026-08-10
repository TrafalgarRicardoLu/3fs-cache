#include "cache_manager/job/ActiveJobPinManager.h"

#include <limits>

namespace hf3fs::cache_manager {
namespace {

uint64_t defaultWallClockMs() { return static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000); }

}  // namespace

CoTryTask<meta::ListPrefetchJobsRsp> MetaActiveJobPinBackend::listJobs(std::optional<cache::PrefetchJobId> after,
                                                                       uint32_t limit) {
  meta::ListPrefetchJobsReq request;
  request.service = service_;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  co_return co_await metaClient_->listPrefetchJobs(std::move(request));
}

CoTryTask<meta::ListPrefetchPlanRsp> MetaActiveJobPinBackend::listPlan(cache::PrefetchJobId jobId,
                                                                       std::optional<cache::CacheBlockKey> after,
                                                                       uint32_t limit) {
  meta::ListPrefetchPlanReq request;
  request.service = service_;
  request.jobId = jobId;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  co_return co_await metaClient_->listPrefetchPlan(std::move(request));
}

CoTryTask<void> MetaActiveJobPinBackend::upsert(std::vector<cache::PinRecord> pins) {
  meta::UpsertCachePinsReq request;
  request.service = service_;
  request.pins = std::move(pins);
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto response = co_await metaClient_->upsertCachePins(std::move(request));
  CO_RETURN_ON_ERROR(response);
  for (const auto &result : response->results) CO_RETURN_ON_ERROR(result);
  co_return Void{};
}

CoTryTask<bool> MetaActiveJobPinBackend::renew(cache::PinOwnerLease lease) {
  meta::RenewCachePinOwnerLeaseReq request;
  request.service = service_;
  request.lease = std::move(lease);
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto response = co_await metaClient_->renewCachePinOwnerLease(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return response->created;
}

CoTryTask<void> MetaActiveJobPinBackend::remove(cache::PinOwner owner) {
  meta::RemoveCachePinsReq request;
  request.service = service_;
  request.owner = owner;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  CO_RETURN_ON_ERROR(co_await metaClient_->removeCachePins(std::move(request)));
  co_return Void{};
}

CoTryTask<void> MetaActiveJobPinBackend::convert(cache::PrefetchJobId jobId, std::vector<cache::CacheBlockKey> keys) {
  meta::ConvertActiveJobPinsReq request;
  request.service = service_;
  request.jobId = jobId;
  request.keys = std::move(keys);
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  CO_RETURN_ON_ERROR(co_await metaClient_->convertActiveJobPins(std::move(request)));
  co_return Void{};
}

ActiveJobPinManager::ActiveJobPinManager(std::shared_ptr<ActiveJobPinBackend> backend,
                                         uint32_t jobPageLimit,
                                         uint32_t planPageLimit,
                                         uint64_t ttlMs,
                                         WallClockMs wallClockMs)
    : backend_(std::move(backend)),
      jobPageLimit_(jobPageLimit),
      planPageLimit_(planPageLimit),
      ttlMs_(ttlMs),
      wallClockMs_(wallClockMs ? std::move(wallClockMs) : defaultWallClockMs) {}

cache::PinOwner ActiveJobPinManager::owner(cache::PrefetchJobId jobId) {
  return {cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerId{jobId.toUnderType()}};
}

bool ActiveJobPinManager::retain(const cache::PrefetchJobRecord &job) {
  switch (job.state) {
    case cache::PrefetchJobState::INVALID:
      return false;
    case cache::PrefetchJobState::PENDING:
    case cache::PrefetchJobState::PLANNING:
    case cache::PrefetchJobState::LOADING:
    case cache::PrefetchJobState::PARTIAL_READY:
      return true;
    case cache::PrefetchJobState::READY:
      return job.spec.pinAfterReady;
    case cache::PrefetchJobState::FAILED:
    case cache::PrefetchJobState::CANCELLED:
      return false;
  }
  return false;
}

CoTryTask<void> ActiveJobPinManager::reconcile(const cache::PrefetchJobRecord &job, uint64_t expiresAtMs) {
  auto pinOwner = owner(job.spec.jobId);
  if (!retain(job)) {
    CO_RETURN_ON_ERROR(co_await backend_->remove(pinOwner));
    co_return Void{};
  }
  if (job.state != cache::PrefetchJobState::READY) {
    auto renewed = co_await backend_->renew({pinOwner, job.createdAtMs, expiresAtMs});
    CO_RETURN_ON_ERROR(renewed);
    if (!*renewed) co_return Void{};
  }
  std::optional<cache::CacheBlockKey> after;
  do {
    auto page = co_await backend_->listPlan(job.spec.jobId, after, planPageLimit_);
    CO_RETURN_ON_ERROR(page);
    if (page->entries.size() > planPageLimit_ || (page->more && page->entries.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid active Job pin plan page");
    }
    std::vector<cache::PinRecord> pins;
    std::vector<cache::CacheBlockKey> keys;
    pins.reserve(page->entries.size());
    keys.reserve(page->entries.size());
    for (const auto &entry : page->entries) {
      CO_RETURN_ON_ERROR(entry.valid());
      if (job.state == cache::PrefetchJobState::READY)
        keys.push_back(entry.key);
      else
        pins.push_back({entry.key, pinOwner, job.createdAtMs, expiresAtMs, {}});
    }
    Result<Void> updated = Void{};
    if (!keys.empty())
      updated = co_await backend_->convert(job.spec.jobId, std::move(keys));
    else if (!pins.empty())
      updated = co_await backend_->upsert(std::move(pins));
    if (updated.hasError()) {
      if (job.state != cache::PrefetchJobState::READY) (void)co_await backend_->remove(pinOwner);
      co_return makeError(updated.error());
    }
    if (!page->more) break;
    after = page->entries.back().key;
  } while (true);
  if (job.state == cache::PrefetchJobState::READY) CO_RETURN_ON_ERROR(co_await backend_->remove(pinOwner));
  co_return Void{};
}

CoTryTask<void> ActiveJobPinManager::runOnce() {
  if (!backend_ || jobPageLimit_ == 0 || jobPageLimit_ > meta::kMaxCacheBatchItems || planPageLimit_ == 0 ||
      planPageLimit_ > meta::kMaxCacheBatchItems || ttlMs_ == 0) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid active Job pin manager configuration");
  }
  auto nowMs = wallClockMs_();
  if (nowMs == 0 || ttlMs_ > std::numeric_limits<uint64_t>::max() - nowMs) {
    co_return makeError(CacheCode::kStateConflict, "active Job pin expiry overflow");
  }
  auto expiresAtMs = nowMs + ttlMs_;
  std::optional<cache::PrefetchJobId> after;
  do {
    auto page = co_await backend_->listJobs(after, jobPageLimit_);
    CO_RETURN_ON_ERROR(page);
    if (page->jobs.size() > jobPageLimit_ || (page->more && page->jobs.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid active Job pin Job page");
    }
    for (const auto &job : page->jobs) {
      CO_RETURN_ON_ERROR(job.valid());
      CO_RETURN_ON_ERROR(co_await reconcile(job, expiresAtMs));
    }
    if (!page->more) break;
    after = page->jobs.back().spec.jobId;
  } while (true);
  co_return Void{};
}

}  // namespace hf3fs::cache_manager
