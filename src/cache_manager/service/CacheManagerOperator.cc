#include "cache_manager/service/CacheManagerOperator.h"

#include <folly/experimental/coro/BlockingWait.h>

namespace hf3fs::cache_manager {

CacheManagerOperator::CacheManagerOperator(const Config &config,
                                           std::shared_ptr<meta::client::MetaClient> metaClient,
                                           std::shared_ptr<storage::client::StorageClient> storageClient,
                                           std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                                           RealCacheManagerBackend::Stores stores)
    : config_(config),
      metaClient_(std::move(metaClient)),
      storageClient_(std::move(storageClient)) {
  if (metaClient_ && storageClient_ && mgmtdClient) {
    backend_ = std::make_shared<RealCacheManagerBackend>(config_,
                                                         metaClient_,
                                                         storageClient_,
                                                         std::move(mgmtdClient),
                                                         std::move(stores));
  }
}

CacheManagerOperator::~CacheManagerOperator() { stop(); }

Result<Void> CacheManagerOperator::start(CPUExecutorGroup &executor) {
  RETURN_ON_ERROR(config_.validateRuntime());
  if (!backend_) return makeError(StatusCode::kInvalidConfig, "cache manager backend is not configured");
  std::map<cache::OriginId, CapacityGate::Limit> originLimits;
  for (size_t i = 0; i < config_.origins_length(); ++i) {
    const auto &origin = config_.origins(i);
    originLimits.emplace(cache::OriginId{origin.origin_id()},
                         CapacityGate::Limit{origin.max_concurrent_requests(), origin.max_inflight_bytes()});
  }
  capacityGate_ =
      std::make_unique<CapacityGate>(CapacityGate::Limit{config_.global_concurrency(), config_.max_inflight_bytes()},
                                     std::move(originLimits));
  loader_ = std::make_unique<CacheLoader>(backend_, *capacityGate_);
  loaderScheduler_ = std::make_unique<LoaderScheduler>(hints_, *loader_, config_.range_size());
  ensureCached_ = std::make_unique<EnsureCached>(backend_, hints_);
  auto scheduler = std::make_unique<BackgroundRunner>(executor);
  if (!scheduler->start(
          "CacheManagerScheduler",
          [this]() -> CoTask<void> { co_await loaderScheduler_->runOne(); },
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
  if (!ensureCached_) co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED};
  co_return co_await ensureCached_->run(req);
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
