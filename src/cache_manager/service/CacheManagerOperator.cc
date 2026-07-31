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
  loader_ = std::make_unique<CacheLoader>(backend_,
                                          *capacityGate_,
                                          config_.enable_phase2() ? config_.storage_permit_ttl() : 0_ns);
  loaderScheduler_ = std::make_unique<LoaderScheduler>(hints_, *loader_, config_.range_size());
  cleanupWorker_ = std::make_unique<CacheCleanupWorker>(backend_);
  reportInvalid_ = std::make_unique<ReportCacheBlockInvalid>(backend_, *cleanupWorker_);
  adminCleanup_ = std::make_unique<AdminCleanupCacheBlocks>(backend_, *cleanupWorker_);
  if (config_.enable_phase2()) {
    auto evictionPolicy = createEvictionPolicy(config_.eviction_policy());
    RETURN_ON_ERROR(evictionPolicy);
    evictionPolicy_ = std::move(*evictionPolicy);
    auto admissionPolicy = createAdmissionPolicy(config_.admission_policy(),
                                                 config_.second_miss_window().count(),
                                                 config_.second_miss_max_entries());
    RETURN_ON_ERROR(admissionPolicy);
    admissionPolicy_ = std::move(*admissionPolicy);
    managerEpoch_ = Uuid::random();
    permitRecovery_ = std::make_unique<PermitRecovery>(backend_, hints_, managerEpoch_, config_.storage_permit_ttl());
    auto recovered = folly::coro::blockingWait(permitRecovery_->run());
    RETURN_ON_ERROR(recovered);
    AccessFlushWorker::Config accessConfig;
    accessConfig.flushThreshold = config_.access_flush_threshold();
    accessConfig.batchSize = config_.access_flush_batch_size();
    accessConfig.maxEntries = config_.access_max_entries();
    accessConfig.flushInterval = config_.access_flush_interval();
    accessFlushWorker_ = std::make_unique<AccessFlushWorker>(backend_, accessConfig);
    RETURN_ON_ERROR(accessFlushWorker_->start());
    physicalTopology_ = std::make_unique<PhysicalTopology>();
    spacePoller_ = std::make_unique<SpacePoller>(
        *physicalTopology_,
        [backend = backend_](const storage::QueryCacheSpaceReq &req) { return backend->queryCacheSpace(req); });
    physicalPreflight_ = std::make_unique<PhysicalPreflight>(*physicalTopology_,
                                                             config_.space_snapshot_max_age(),
                                                             config_.capacity_high_watermark());
    ensureCached_ = std::make_unique<EnsureCached>(backend_,
                                                   hints_,
                                                   cleanupWorker_.get(),
                                                   *admissionPolicy_,
                                                   *physicalPreflight_,
                                                   managerEpoch_,
                                                   config_.storage_permit_ttl());
  } else {
    ensureCached_ = std::make_unique<EnsureCached>(backend_, hints_, cleanupWorker_.get());
  }
  auto scheduler = std::make_unique<BackgroundRunner>(executor);
  if (!scheduler->start(
          "CacheManagerScheduler",
          [this]() -> CoTask<void> { co_await loaderScheduler_->runOne(); },
          [this] { return config_.scheduler_interval(); })) {
    if (accessFlushWorker_) accessFlushWorker_->stop();
    return makeError(StatusCode::kQueueConflict, "failed to start cache manager scheduler");
  }
  if (spacePoller_ && !scheduler->start(
                          "CacheManagerSpacePoller",
                          [this]() -> CoTask<void> {
                            auto routing = backend_->routingInfo();
                            if (!routing || !routing->raw()) co_return;
                            physicalTopology_->updateRouting(routing);
                            std::map<flat::NodeId, std::vector<flat::TargetId>> targetsByNode;
                            for (const auto &[targetId, target] : routing->raw()->targets) {
                              if (target.storageRole == storage::StorageRole::CACHE_ONLY && target.nodeId) {
                                targetsByNode[*target.nodeId].push_back(targetId);
                              }
                            }
                            for (auto &[nodeId, targetIds] : targetsByNode) {
                              auto result = co_await spacePoller_->poll(nodeId, std::move(targetIds));
                              if (result.hasError()) {
                                XLOGF(WARN, "Cache space poll for node {} failed: {}", nodeId, result.error());
                              }
                            }
                          },
                          [this] { return config_.space_poll_interval(); })) {
    folly::coro::blockingWait(scheduler->stopAll());
    if (accessFlushWorker_) accessFlushWorker_->stop();
    return makeError(StatusCode::kQueueConflict, "failed to start cache manager space poller");
  }
  auto lock = std::unique_lock(mutex_);
  if (running_) {
    lock.unlock();
    folly::coro::blockingWait(scheduler->stopAll());
    if (accessFlushWorker_) accessFlushWorker_->stop();
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
  if (accessFlushWorker_) accessFlushWorker_->stop();
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

Result<Void> CacheManagerOperator::checkPhase2Protocol(uint32_t version) const {
  return cache::checkPhase2Capability(version, config_.enable_phase2());
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
  if (!ensureCached_) co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED, BypassReason::FEATURE_DISABLED};
  co_return co_await ensureCached_->run(req);
}

CoTryTask<ReportCacheBlockInvalidRsp> CacheManagerOperator::reportCacheBlockInvalid(
    const ReportCacheBlockInvalidReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  if (!reportInvalid_) co_return makeError(CacheCode::kFeatureDisabled, "cache cleanup worker is not enabled");
  co_return co_await reportInvalid_->run(req);
}

CoTryTask<AdminCleanupCacheBlocksRsp> CacheManagerOperator::adminCleanupCacheBlocks(
    const AdminCleanupCacheBlocksReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  if (!adminCleanup_) co_return makeError(CacheCode::kFeatureDisabled, "cache cleanup worker is not enabled");
  co_return co_await adminCleanup_->run(req);
}

CoTryTask<GetCacheStatusRsp> CacheManagerOperator::getCacheStatus(const GetCacheStatusReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  if (backend_) CO_RETURN_ON_ERROR(co_await backend_->authorizeAdmin(req.user, req.inode));
  GetCacheStatusRsp response;
  response.queued = hints_.size();
  if (capacityGate_) {
    response.loading = capacityGate_->inflightRequests();
    response.inflightBytes = capacityGate_->inflightBytes();
  }
  if (ensureCached_) response.lastBypassReason = ensureCached_->lastBypassReason();
  if (metaClient_) {
    meta::GetCacheStatusReq status;
    status.user = req.user;
    status.inode = req.inode;
    status.cacheProtocolVersion = req.cacheProtocolVersion;
    auto metaStatus = co_await metaClient_->getCacheStatus(std::move(status));
    CO_RETURN_ON_ERROR(metaStatus);
    response.logicalCapacity = metaStatus->logicalCapacity;
    response.usedCapacity = metaStatus->usedCapacity;
    for (const auto &count : metaStatus->stateCounts) {
      switch (count.state) {
        case cache::CacheBlockState::READY:
          response.ready = count.count;
          break;
        case cache::CacheBlockState::CLEANING:
          response.cleaning = count.count;
          break;
        default:
          break;
      }
    }
  }
  co_return response;
}

CoTryTask<ReportCacheAccessRsp> CacheManagerOperator::reportCacheAccess(const ReportCacheAccessReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase2Protocol(req.cacheProtocolVersion));
  if (!accessFlushWorker_) co_return makeError(CacheCode::kUnavailable, "cache access aggregator is not running");
  auto statuses = accessFlushWorker_->submit(req.items);
  ReportCacheAccessRsp response;
  response.results.reserve(req.items.size());
  for (size_t i = 0; i < req.items.size(); ++i) {
    response.results.push_back(CacheAccessReportResult{req.items[i].key, statuses[i]});
  }
  co_return response;
}

CoTryTask<GetPhase2CacheStatusRsp> CacheManagerOperator::getPhase2CacheStatus(const GetPhase2CacheStatusReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase2Protocol(req.cacheProtocolVersion));
  co_return makeError(StatusCode::kNotImplemented, "phase two status is not implemented");
}

}  // namespace hf3fs::cache_manager
