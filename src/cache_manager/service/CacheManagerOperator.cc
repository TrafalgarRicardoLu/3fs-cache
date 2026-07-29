#include "cache_manager/service/CacheManagerOperator.h"

#include <folly/experimental/coro/BlockingWait.h>

namespace hf3fs::cache_manager {

CacheManagerOperator::CacheManagerOperator(const Config &config,
                                           std::shared_ptr<meta::client::MetaClient> metaClient,
                                           std::shared_ptr<storage::client::StorageClient> storageClient)
    : config_(config),
      metaClient_(std::move(metaClient)),
      storageClient_(std::move(storageClient)) {}

CacheManagerOperator::~CacheManagerOperator() { stop(); }

Result<Void> CacheManagerOperator::start(CPUExecutorGroup &executor) {
  RETURN_ON_ERROR(config_.validateRuntime());
  auto scheduler = std::make_unique<BackgroundRunner>(executor);
  if (!scheduler->start(
          "CacheManagerScheduler",
          []() -> CoTask<void> { co_return; },
          [this] { return config_.scheduler_interval(); })) {
    return makeError(StatusCode::kQueueConflict, "failed to start cache manager scheduler");
  }
  auto lock = std::unique_lock(mutex_);
  if (running_) {
    lock.unlock();
    folly::coro::blockingWait(scheduler->stopAll());
    return Void{};
  }
  scheduler_ = std::move(scheduler);
  running_ = true;
  return Void{};
}

Result<Void> CacheManagerOperator::startForTest(SchedulerStartHook startHook, SchedulerStopHook stopHook) {
  RETURN_ON_ERROR(config_.validateRuntime());
  {
    auto lock = std::unique_lock(mutex_);
    if (running_) return Void{};
  }
  auto result = startHook ? startHook() : Result<Void>{Void{}};
  if (!result) {
    if (stopHook) stopHook();
    return result;
  }
  auto lock = std::unique_lock(mutex_);
  schedulerStopHook_ = std::move(stopHook);
  running_ = true;
  return Void{};
}

void CacheManagerOperator::stop() {
  std::unique_ptr<BackgroundRunner> scheduler;
  SchedulerStopHook stopHook;
  {
    auto lock = std::unique_lock(mutex_);
    if (!running_ && !scheduler_ && !schedulerStopHook_) return;
    running_ = false;
    scheduler = std::move(scheduler_);
    stopHook = std::move(schedulerStopHook_);
  }
  if (scheduler) folly::coro::blockingWait(scheduler->stopAll());
  if (stopHook) stopHook();
}

bool CacheManagerOperator::running() const {
  auto lock = std::unique_lock(mutex_);
  return running_;
}

Result<Void> CacheManagerOperator::checkProtocol(uint32_t version) const {
  if (version != cache::kCacheProtocolVersion) {
    return makeError(CacheCode::kUpgradeRequired, "incompatible cache protocol version");
  }
  return Void{};
}

Result<Void> CacheManagerOperator::checkService(const ServiceIdentity &service) const {
  RETURN_ON_ERROR(service.valid());
  if (service.name != config_.service_name() || service.token != config_.service_token()) {
    return makeError(StatusCode::kAuthenticationFail, "invalid cache manager service identity");
  }
  return Void{};
}

CoTryTask<EnsureCachedRsp> CacheManagerOperator::ensureCached(const EnsureCachedReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  CO_RETURN_ON_ERROR(checkService(req.service));
  co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED};
}

CoTryTask<ReportCacheBlockInvalidRsp> CacheManagerOperator::reportCacheBlockInvalid(
    const ReportCacheBlockInvalidReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  co_return makeError(CacheCode::kFeatureDisabled, "cache cleanup worker is not enabled");
}

CoTryTask<AdminCleanupCacheBlocksRsp> CacheManagerOperator::adminCleanupCacheBlocks(
    const AdminCleanupCacheBlocksReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  co_return makeError(CacheCode::kFeatureDisabled, "cache cleanup worker is not enabled");
}

CoTryTask<GetCacheStatusRsp> CacheManagerOperator::getCacheStatus(const GetCacheStatusReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  co_return GetCacheStatusRsp{};
}

}  // namespace hf3fs::cache_manager
