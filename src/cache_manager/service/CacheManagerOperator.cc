#include "cache_manager/service/CacheManagerOperator.h"

#include <algorithm>
#include <cmath>
#include <folly/experimental/coro/BlockingWait.h>
#include <limits>

#include "cache/metrics/CacheMetrics.h"
#include "cache/origin/RoutedObjectStore.h"
#include "cache_manager/planner/ManifestPlanner.h"
#include "cache_manager/planner/NamespaceDatasetPlanner.h"
#include "cache_manager/planner/S3PrefixPlanner.h"
#include "cache_manager/recovery/StartupRecoveryCoordinator.h"

namespace hf3fs::cache_manager {

CacheManagerOperator::CacheManagerOperator(const Config &config,
                                           std::shared_ptr<meta::client::MetaClient> metaClient,
                                           std::shared_ptr<storage::client::StorageClient> storageClient,
                                           std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                                           RealCacheManagerBackend::Stores stores)
    : config_(config),
      metaClient_(std::move(metaClient)),
      storageClient_(std::move(storageClient)),
      stores_(stores) {
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
  {
    auto lock = std::unique_lock(mutex_);
    if (running_) return Void{};
  }
  admissionReady_.store(false, std::memory_order_release);
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
  // Keep inventory polling available while admission is disabled so an
  // administrator can satisfy the Phase 2 enable preconditions.
  physicalTopology_ = std::make_unique<PhysicalTopology>();
  spacePoller_ = std::make_unique<SpacePoller>(
      *physicalTopology_,
      [backend = backend_](const storage::QueryCacheSpaceReq &req) { return backend->queryCacheSpace(req); });
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
    permitRecovery_ = std::make_unique<PermitRecovery>(backend_,
                                                       hints_,
                                                       managerEpoch_,
                                                       config_.storage_permit_ttl(),
                                                       config_.lease_recovery_page_size(),
                                                       PermitRecovery::WallClockNsFn{},
                                                       cleanupWorker_.get(),
                                                       config_.enable_phase4());
    if (config_.enable_phase4()) {
      CacheReconcilerConfig reconcileConfig;
      reconcileConfig.pageSize = config_.reconcile_page_size();
      reconcileConfig.maxTargetConcurrency = config_.reconcile_max_concurrency();
      reconcileConfig.maxMutations = config_.reconcile_max_mutations();
      reconcileConfig.maxRuntime = config_.reconcile_max_run_time();
      reconcileConfig.dryRun = config_.reconcile_dry_run();
      reconciler_ = std::make_unique<CacheReconciler>(backend_, *cleanupWorker_, reconcileConfig);
    }
    AccessFlushWorker::Config accessConfig;
    accessConfig.flushThreshold = config_.access_flush_threshold();
    accessConfig.batchSize = config_.access_flush_batch_size();
    accessConfig.maxEntries = config_.access_max_entries();
    accessConfig.flushInterval = config_.access_flush_interval();
    accessFlushWorker_ = std::make_unique<AccessFlushWorker>(backend_, accessConfig);
    evictionPressure_ = std::make_unique<EvictionPressureState>();
    physicalPreflight_ = std::make_unique<PhysicalPreflight>(*physicalTopology_,
                                                             config_.space_snapshot_max_age(),
                                                             config_.capacity_high_watermark(),
                                                             evictionPressure_.get());
    EvictionControllerConfig evictionConfig;
    evictionConfig.highWatermark = config_.capacity_high_watermark();
    evictionConfig.lowWatermark = config_.capacity_low_watermark();
    evictionConfig.snapshotMaxAge = config_.space_snapshot_max_age();
    evictionConfig.protectionPeriod = config_.eviction_protection_period();
    evictionConfig.candidatePageSize = config_.eviction_page_size();
    evictionConfig.batchSize = config_.eviction_batch_size();
    evictionController_ = std::make_unique<EvictionController>(backend_,
                                                               *physicalTopology_,
                                                               *evictionPolicy_,
                                                               *evictionPressure_,
                                                               evictionConfig);
    evictingWorker_ = std::make_unique<EvictingWorker>(backend_, config_.evicting_page_size());
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
  if (config_.enable_phase3()) {
    cache::origin::RoutedObjectStore::Stores routedStores;
    for (const auto &[originId, store] : stores_) routedStores.emplace(originId.toUnderType(), store);
    auto objectStore = std::make_shared<cache::origin::RoutedObjectStore>(std::move(routedStores));
    auto resolver = std::make_shared<MetaNamespaceFileResolver>(metaClient_, flat::UserInfo{});
    auto importer = std::make_shared<MetaOriginFileImporter>(metaClient_, flat::UserInfo{});
    SourcePlannerFactory::Builders builders;
    builders.emplace(cache::DatasetSourceType::NAMESPACE_PATH,
                     makeNamespaceDatasetPlannerBuilder(resolver, cache::DatasetSourceType::NAMESPACE_PATH));
    builders.emplace(cache::DatasetSourceType::PATH_LIST,
                     makeNamespaceDatasetPlannerBuilder(resolver, cache::DatasetSourceType::PATH_LIST));
    builders.emplace(cache::DatasetSourceType::MANIFEST_PATH,
                     makeManifestPlannerBuilder(resolver, objectStore, ManifestPlannerConfig{}));
    S3PrefixPlannerConfig prefixConfig;
    prefixConfig.layout.tableId = flat::ChainTableId{config_.phase3_prefix_table_id()};
    prefixConfig.layout.blockSize = config_.phase3_prefix_block_size();
    prefixConfig.layout.stripeSize = config_.phase3_prefix_stripe_size();
    builders.emplace(cache::DatasetSourceType::S3_PREFIX,
                     makeS3PrefixPlannerBuilder(objectStore, importer, prefixConfig));
    auto factory = SourcePlannerFactory::create({config_.phase3_plan_page_size(), cache::kMaxDatasetPathLength},
                                                std::move(builders));
    RETURN_ON_ERROR(factory);
    auto sharedFactory = std::shared_ptr<SourcePlannerFactory>(std::move(*factory));
    meta::CacheServiceIdentity service{config_.service_name(), config_.service_token()};
    auto plannerBackend = std::make_shared<MetaJobPlannerBackend>(metaClient_, service);
    auto activePinTtlMs = static_cast<uint64_t>(config_.phase3_active_pin_ttl().asMs().count());
    jobPlanner_ =
        std::make_shared<JobPlanner>(plannerBackend,
                                     std::move(sharedFactory),
                                     config_.phase3_plan_page_size(),
                                     activePinTtlMs,
                                     [] { return static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000); });
    jobQuota_ = std::make_unique<JobQuota>();
    auto runnerBackend = std::make_shared<MetaJobRunnerBackend>(metaClient_, service, *ensureCached_);
    jobRunner_ = std::make_shared<JobRunner>(runnerBackend, *jobQuota_, config_.phase3_plan_page_size(), Uuid::random);
    auto trackerBackend = std::make_shared<MetaJobTrackerBackend>(metaClient_, service);
    jobTracker_ = std::make_shared<JobTracker>(trackerBackend, config_.phase3_plan_page_size());
    auto cancellerBackend = std::make_shared<MetaJobCancellerBackend>(metaClient_, service, backend_);
    jobCanceller_ = std::make_shared<JobCanceller>(cancellerBackend, hints_, config_.phase3_plan_page_size());
    auto pinBackend = std::make_shared<MetaActiveJobPinBackend>(metaClient_, service);
    activeJobPins_ = std::make_shared<ActiveJobPinManager>(pinBackend,
                                                           config_.phase3_job_page_size(),
                                                           config_.phase3_plan_page_size(),
                                                           activePinTtlMs);
    auto coordinatorBackend = std::make_shared<MetaOrchestrationCoordinatorBackend>(metaClient_, std::move(service));
    orchestration_ = std::make_unique<OrchestrationCoordinator>(
        std::move(coordinatorBackend),
        config_.phase3_job_page_size(),
        [planner = jobPlanner_](const cache::PrefetchJobRecord &job, const CancellationToken &cancellation)
            -> CoTryTask<cache::PrefetchJobRecord> { co_return co_await planner->runNextPage(job, cancellation); },
        [runner = jobRunner_](const cache::PrefetchJobRecord &job,
                              std::optional<cache::CacheBlockKey> after,
                              const CancellationToken &cancellation) -> CoTryTask<JobRunnerPageResult> {
          co_return co_await runner->runNextPage(job, after, cancellation);
        },
        [tracker = jobTracker_](const cache::PrefetchJobRecord &job,
                                const CancellationToken &cancellation) -> CoTryTask<void> {
          CO_RETURN_ON_ERROR(co_await tracker->run(job, cancellation));
          co_return Void{};
        },
        [runner = jobRunner_](const cache::PrefetchJobRecord &job,
                              std::optional<cache::CacheBlockKey> after,
                              const CancellationToken &cancellation) -> CoTryTask<JobRunnerPageResult> {
          co_return co_await runner->runNextPage(job, after, cancellation, true);
        });
  }
  auto noCleanup = [] {};
  StartupRecoveryCoordinator recovery({{
      {StartupRecoveryStage::ROUTING,
       [this]() -> CoTryTask<void> { co_return co_await backend_->refreshRouting(); },
       noCleanup},
      {StartupRecoveryStage::PERMIT_AND_LOADING,
       [this]() -> CoTryTask<void> {
         if (permitRecovery_) co_return co_await permitRecovery_->run(false);
         co_return Void{};
       },
       noCleanup},
      {StartupRecoveryStage::EVICTION_AND_CLEANUP,
       [this]() -> CoTryTask<void> {
         if (!evictingWorker_) co_return Void{};
         auto recovered = co_await evictingWorker_->runOnce();
         CO_RETURN_ON_ERROR(recovered);
         if (recovered->failed != 0) {
           co_return makeError(CacheCode::kUnavailable, "startup EVICTING recovery has failed items");
         }
         co_return Void{};
       },
       noCleanup},
      {StartupRecoveryStage::JOBS,
       [this]() -> CoTryTask<void> {
         if (orchestration_) CO_RETURN_ON_ERROR(co_await orchestration_->recover());
         co_return Void{};
       },
       [this] {
         if (orchestration_) orchestration_->stop();
       }},
      {StartupRecoveryStage::RECONCILE,
       [this]() -> CoTryTask<void> {
         if (!reconciler_) co_return Void{};
         auto routing = backend_->routingInfo();
         if (!routing || !routing->raw()) {
           co_return makeError(CacheCode::kUnavailable, "routing is missing after startup refresh");
         }
         std::vector<storage::TargetId> targets;
         for (const auto &[targetId, target] : routing->raw()->targets) {
           if (target.storageRole == storage::StorageRole::CACHE_ONLY) targets.push_back(targetId);
         }
         CO_RETURN_ON_ERROR(co_await reconciler_->run(targets));
         auto status = reconciler_->status();
         if (status.state != cache::ReconcileRunState::HEALTHY) {
           co_return makeError(CacheCode::kUnavailable, "startup cache reconciliation is degraded: " + status.error);
         }
         co_return Void{};
       },
       [this] {
         if (reconciler_) reconciler_->stop();
       }},
  }});
  auto recovered = folly::coro::blockingWait(recovery.run());
  RETURN_ON_ERROR(recovered);
  auto rollbackRecovered = [this] {
    admissionReady_.store(false, std::memory_order_release);
    if (orchestration_) orchestration_->stop();
    if (reconciler_) reconciler_->stop();
    if (accessFlushWorker_) accessFlushWorker_->stop();
  };
  if (accessFlushWorker_) {
    auto started = accessFlushWorker_->start();
    if (started.hasError()) {
      rollbackRecovered();
      return makeError(started.error());
    }
  }
  auto scheduler = std::make_unique<BackgroundRunner>(executor);
  if (!scheduler->start(
          "CacheManagerScheduler",
          [this]() -> CoTask<void> { co_await loaderScheduler_->runOne(); },
          [this] { return config_.scheduler_interval(); })) {
    rollbackRecovered();
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
    rollbackRecovered();
    return makeError(StatusCode::kQueueConflict, "failed to start cache manager space poller");
  }
  if (evictionController_ && !scheduler->start(
                                 "CacheManagerEvictionController",
                                 [this]() -> CoTask<void> {
                                   auto result = co_await evictionController_->runOnce();
                                   if (result.hasError()) {
                                     XLOGF(WARN, "Cache eviction controller iteration failed: {}", result.error());
                                   }
                                 },
                                 [this] { return config_.eviction_interval(); })) {
    folly::coro::blockingWait(scheduler->stopAll());
    rollbackRecovered();
    return makeError(StatusCode::kQueueConflict, "failed to start cache eviction controller");
  }
  if (config_.enable_phase2()) {
    if (!scheduler->start(
            "CacheManagerEvictingWorker",
            [this]() -> CoTask<void> {
              auto result = co_await evictingWorker_->runOnce();
              if (result.hasError()) XLOGF(WARN, "EVICTING recovery iteration failed: {}", result.error());
            },
            [this] { return config_.evicting_scan_interval(); })) {
      folly::coro::blockingWait(scheduler->stopAll());
      rollbackRecovered();
      return makeError(StatusCode::kQueueConflict, "failed to start EVICTING recovery worker");
    }
  }
  if (config_.enable_phase4() && !scheduler->start(
                                     "CacheManagerLeaseRecovery",
                                     [this]() -> CoTask<void> {
                                       auto result = co_await permitRecovery_->run();
                                       if (result.hasError())
                                         XLOGF(WARN, "LOADING lease recovery iteration failed: {}", result.error());
                                     },
                                     [this] { return config_.lease_recovery_interval(); })) {
    folly::coro::blockingWait(scheduler->stopAll());
    rollbackRecovered();
    return makeError(StatusCode::kQueueConflict, "failed to start LOADING lease recovery worker");
  }
  if (reconciler_ && !scheduler->start(
                         "CacheManagerReconciler",
                         [this]() -> CoTask<void> {
                           auto routing = backend_->routingInfo();
                           if (!routing || !routing->raw()) co_return;
                           std::vector<storage::TargetId> targets;
                           for (const auto &[targetId, target] : routing->raw()->targets) {
                             if (target.storageRole == storage::StorageRole::CACHE_ONLY) targets.push_back(targetId);
                           }
                           auto result = co_await reconciler_->run(targets);
                           if (result.hasError()) {
                             XLOGF(WARN, "Cache reconcile iteration failed: {}", result.error());
                           }
                         },
                         [this] { return config_.reconcile_interval(); })) {
    reconciler_->stop();
    folly::coro::blockingWait(scheduler->stopAll());
    rollbackRecovered();
    return makeError(StatusCode::kQueueConflict, "failed to start cache reconciler");
  }
  if (orchestration_) {
    auto startWorker = [&](String name, auto task, Duration interval) {
      return scheduler->start(
          std::move(name),
          [task = std::move(task)]() -> CoTask<void> {
            auto result = co_await task();
            if (result.hasError()) XLOGF(WARN, "Cache orchestration iteration failed: {}", result.error());
          },
          [interval] { return interval; });
    };
    bool started = startWorker(
                       "CacheManagerJobPlanner",
                       [this]() -> CoTryTask<void> {
                         auto routing = backend_->routingInfo();
                         if (routing && routing->raw() &&
                             routing->raw()->cachePhase3State == flat::CachePhase3RolloutState::ENABLED)
                           co_return co_await orchestration_->runPlannerOnce();
                         co_return Void{};
                       },
                       config_.phase3_planner_interval()) &&
                   startWorker(
                       "CacheManagerJobRunner",
                       [this]() -> CoTryTask<void> {
                         auto routing = backend_->routingInfo();
                         if (routing && routing->raw() &&
                             routing->raw()->cachePhase3State == flat::CachePhase3RolloutState::ENABLED)
                           co_return co_await orchestration_->runRunnerOnce();
                         co_return Void{};
                       },
                       config_.phase3_runner_interval()) &&
                   startWorker(
                       "CacheManagerJobTracker",
                       [this] { return orchestration_->runTrackerOnce(); },
                       config_.phase3_tracker_interval()) &&
                   startWorker(
                       "CacheManagerActiveJobPins",
                       [this] { return activeJobPins_->runOnce(); },
                       config_.phase3_pin_renew_interval());
    if (!started) {
      orchestration_->stop();
      folly::coro::blockingWait(scheduler->stopAll());
      rollbackRecovered();
      return makeError(StatusCode::kQueueConflict, "failed to start cache orchestration workers");
    }
  }
  auto lock = std::unique_lock(mutex_);
  if (running_) {
    lock.unlock();
    folly::coro::blockingWait(scheduler->stopAll());
    rollbackRecovered();
    return Void{};
  }
  scheduler_ = std::move(scheduler);
  running_ = true;
  admissionReady_.store(true, std::memory_order_release);
  return Void{};
}

Result<Void> CacheManagerOperator::startForTest(SchedulerStartHook startHook, SchedulerStopHook stopHook) {
  RETURN_ON_ERROR(config_.validateRuntime());
  {
    auto lock = std::unique_lock(mutex_);
    if (running_) return Void{};
  }
  admissionReady_.store(false, std::memory_order_release);
  auto result = startHook ? startHook() : Result<Void>{Void{}};
  if (!result) {
    if (stopHook) stopHook();
    admissionReady_.store(false, std::memory_order_release);
    return result;
  }
  auto lock = std::unique_lock(mutex_);
  schedulerStopHook_ = std::move(stopHook);
  running_ = true;
  admissionReady_.store(true, std::memory_order_release);
  return Void{};
}

void CacheManagerOperator::stop() {
  std::unique_ptr<BackgroundRunner> scheduler;
  SchedulerStopHook stopHook;
  {
    auto lock = std::unique_lock(mutex_);
    if (!running_ && !scheduler_ && !schedulerStopHook_) return;
    running_ = false;
    admissionReady_.store(false, std::memory_order_release);
    scheduler = std::move(scheduler_);
    stopHook = std::move(schedulerStopHook_);
  }
  if (orchestration_) orchestration_->stop();
  if (reconciler_) reconciler_->stop();
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

Result<Void> CacheManagerOperator::checkPhase3Protocol(uint32_t version) const {
  RETURN_ON_ERROR(cache::checkPhase3Capability(version, config_.enable_phase3()));
  if (backend_) {
    auto routing = backend_->routingInfo();
    if (!routing || !routing->raw() || routing->raw()->cachePhase2State != flat::CachePhase2RolloutState::ENABLED ||
        routing->raw()->cachePhase3State == flat::CachePhase3RolloutState::DISABLED) {
      return makeError(CacheCode::kFeatureDisabled, "cache phase three rollout is not enabled");
    }
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
  if (config_.enable_phase4() && !admissionReady_.load(std::memory_order_acquire)) {
    co_return EnsureCachedRsp{EnsureCachedStatus::BYPASSED, BypassReason::UNAVAILABLE};
  }
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
  response.phase3Enabled = config_.enable_phase3();
  response.phase4Enabled = config_.enable_phase4();
  if (reconciler_) {
    response.reconcile = reconciler_->status();
  } else {
    response.reconcile.state = cache::ReconcileRunState::NEVER_RUN;
  }
  response.queued = hints_.size();
  response.exclusiveQueuedClaims = hints_.exclusiveJobClaims();
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
    if (response.phase3Enabled) {
      std::optional<cache::PrefetchJobId> after;
      do {
        meta::ListPrefetchJobsReq list;
        list.service = {config_.service_name(), config_.service_token()};
        if (!req.user.isRoot()) list.ownerUid = req.user.uid;
        list.after = after;
        list.limit = config_.phase3_job_page_size();
        list.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
        auto jobs = co_await metaClient_->listPrefetchJobs(std::move(list));
        CO_RETURN_ON_ERROR(jobs);
        for (const auto &job : jobs->jobs) {
          auto terminal = job.state == cache::PrefetchJobState::READY || job.state == cache::PrefetchJobState::FAILED ||
                          job.state == cache::PrefetchJobState::CANCELLED;
          if (!terminal) ++response.activeJobs;
          if (job.state == cache::PrefetchJobState::FAILED) {
            ++response.failedJobs;
            if (!job.error.empty()) response.lastJobError = job.error.substr(0, 256);
          }
          response.phase3PlannedBytes +=
              std::min(job.plannedBytes, std::numeric_limits<uint64_t>::max() - response.phase3PlannedBytes);
          response.phase3ReadyBytes +=
              std::min(job.readyBytes, std::numeric_limits<uint64_t>::max() - response.phase3ReadyBytes);
          if (job.spec.pinAfterReady) {
            GetPinStatusReq pinStatus{req.user,
                                      cache::PinOwnerId{job.spec.jobId.toUnderType()},
                                      cache::kCachePhase3ProtocolVersion};
            auto pin = co_await getPinStatus(pinStatus);
            CO_RETURN_ON_ERROR(pin);
            response.pinnedBytes +=
                std::min(pin->pinnedBytes, std::numeric_limits<uint64_t>::max() - response.pinnedBytes);
          }
        }
        if (!jobs->more) break;
        if (jobs->jobs.empty()) co_return makeError(CacheCode::kInvalidResponse, "Job status page did not advance");
        after = jobs->jobs.back().spec.jobId;
      } while (true);
      cache::metrics::setGauge(cache::metrics::Event::MANAGER_ACTIVE_JOBS, response.activeJobs);
      cache::metrics::setGauge(cache::metrics::Event::MANAGER_PINNED_BYTES, response.pinnedBytes);
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
  CO_RETURN_ON_ERROR(checkProtocol(req.cacheProtocolVersion));
  if (backend_) CO_RETURN_ON_ERROR(co_await backend_->authorizeAdmin(req.user, std::nullopt));
  GetPhase2CacheStatusRsp response;
  response.enabled = config_.enable_phase2();
  response.managerEpoch = managerEpoch_;
  response.admissionPolicy = config_.admission_policy();
  response.evictionPolicy = config_.eviction_policy();
  response.capacityHighWatermark = config_.capacity_high_watermark();
  response.capacityLowWatermark = config_.capacity_low_watermark();
  response.snapshotMaxAgeNs = config_.space_snapshot_max_age().asUs().count() * 1000;
  response.permitTtlNs = config_.storage_permit_ttl().asUs().count() * 1000;
  auto now = SteadyClock::now();
  auto pressured = evictionPressure_ ? evictionPressure_->snapshot() : std::set<storage::PhysicalDiskId>{};
  if (physicalTopology_) {
    for (const auto &[diskId, snapshot] : physicalTopology_->snapshots()) {
      Phase2DiskStatus disk;
      disk.physicalDiskId = diskId;
      disk.role = snapshot.space.role;
      disk.capacityBytes = snapshot.space.capacityBytes;
      disk.physicalUsedBytes = snapshot.space.physicalUsedBytes;
      disk.allocatableBytes = snapshot.space.allocatableBytes;
      disk.reservedBytes = snapshot.space.reservedBytes;
      disk.activeGenerations = snapshot.space.activeGenerations;
      disk.enforcedHighWatermark = snapshot.space.enforcedHighWatermark;
      disk.permitStoreHealthy = snapshot.space.permitStoreHealthy;
      disk.eventJournalWritable = snapshot.space.eventJournalWritable;
      disk.eventPrepared = snapshot.space.eventPrepared;
      disk.eventDeliverable = snapshot.space.eventDeliverable;
      disk.eventAcknowledgedSequence = snapshot.space.eventAcknowledgedSequence;
      response.eventBacklog += disk.eventPrepared + disk.eventDeliverable;
      disk.snapshotAgeNs = now >= snapshot.receivedAt
                               ? std::chrono::duration_cast<std::chrono::nanoseconds>(now - snapshot.receivedAt).count()
                               : std::numeric_limits<uint64_t>::max();
      disk.admissionPaused =
          disk.snapshotAgeNs > static_cast<uint64_t>(config_.space_snapshot_max_age().asUs().count()) * 1000;
      if (disk.admissionPaused) disk.pauseReason = "stale_space_snapshot";
      if (pressured.contains(diskId)) {
        disk.admissionPaused = true;
        disk.pauseReason = "capacity_watermark";
      }
      if (std::abs(disk.enforcedHighWatermark - config_.capacity_high_watermark()) >
          std::numeric_limits<double>::epsilon()) {
        disk.admissionPaused = true;
        disk.pauseReason = "watermark_mismatch";
      }
      if (!disk.permitStoreHealthy) {
        disk.admissionPaused = true;
        disk.pauseReason = "permit_store_unavailable";
      }
      if (!disk.eventJournalWritable) {
        disk.admissionPaused = true;
        disk.pauseReason = "event_journal_unwritable";
      }
      response.disks.push_back(std::move(disk));
    }
  }
  if (metaClient_) {
    meta::GetCacheStatusReq status;
    status.cacheProtocolVersion = req.cacheProtocolVersion;
    auto metaStatus = co_await metaClient_->getCacheStatus(std::move(status));
    CO_RETURN_ON_ERROR(metaStatus);
    for (const auto &count : metaStatus->stateCounts) {
      if (count.state == cache::CacheBlockState::EVICTING) response.evicting = count.count;
    }
    if (config_.enable_phase2()) {
      meta::ListCacheEventDeadLettersReq deadLetters;
      deadLetters.limit = cache::kMaxPhase2BatchItems;
      deadLetters.cacheProtocolVersion = req.cacheProtocolVersion;
      auto page = co_await metaClient_->listCacheEventDeadLetters(std::move(deadLetters));
      CO_RETURN_ON_ERROR(page);
      response.deadLetters = page->items.size();
    }
  }
  co_return response;
}

CoTryTask<CreatePrefetchJobRsp> CacheManagerOperator::createPrefetchJob(const CreatePrefetchJobReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  if (backend_) {
    auto routing = backend_->routingInfo();
    if (!routing || !routing->raw() || routing->raw()->cachePhase3State != flat::CachePhase3RolloutState::ENABLED) {
      co_return makeError(CacheCode::kFeatureDisabled, "cache phase three is draining");
    }
  }
  if (!metaClient_) co_return makeError(CacheCode::kUnavailable, "metadata client is unavailable");
  auto nowMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000);
  if (nowMs == 0) co_return makeError(StatusCode::kInvalidArg, "cache manager clock is invalid");
  cache::PrefetchJobRecord job;
  job.spec = req.spec;
  job.state = cache::PrefetchJobState::PENDING;
  job.stateVersion = 1;
  job.createdAtMs = job.updatedAtMs = nowMs;
  meta::CreatePrefetchJobReq create;
  create.service = {config_.service_name(), config_.service_token()};
  create.job = std::move(job);
  create.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto created = co_await metaClient_->createPrefetchJob(std::move(create));
  CO_RETURN_ON_ERROR(created);
  co_return CreatePrefetchJobRsp{std::move(created->job)};
}

CoTryTask<GetPrefetchJobRsp> CacheManagerOperator::getPrefetchJob(const GetPrefetchJobReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  if (!metaClient_) co_return makeError(CacheCode::kUnavailable, "metadata client is unavailable");
  meta::GetPrefetchJobReq get;
  get.service = {config_.service_name(), config_.service_token()};
  get.jobId = req.jobId;
  get.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto fetched = co_await metaClient_->getPrefetchJob(std::move(get));
  CO_RETURN_ON_ERROR(fetched);
  if (!req.user.isRoot() && fetched->job.spec.ownerUid != req.user.uid) {
    co_return makeError(MetaCode::kNoPermission, "prefetch Job belongs to another user");
  }
  co_return GetPrefetchJobRsp{std::move(fetched->job)};
}

CoTryTask<ListPrefetchJobsRsp> CacheManagerOperator::listPrefetchJobs(const ListPrefetchJobsReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  if (!metaClient_) co_return makeError(CacheCode::kUnavailable, "metadata client is unavailable");
  meta::ListPrefetchJobsReq list;
  list.service = {config_.service_name(), config_.service_token()};
  if (!req.user.isRoot()) list.ownerUid = req.user.uid;
  list.after = req.after;
  list.limit = req.limit;
  list.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  auto page = co_await metaClient_->listPrefetchJobs(std::move(list));
  CO_RETURN_ON_ERROR(page);
  ListPrefetchJobsRsp response;
  response.jobs = std::move(page->jobs);
  response.more = page->more;
  co_return response;
}

CoTryTask<CancelPrefetchJobRsp> CacheManagerOperator::cancelPrefetchJob(const CancelPrefetchJobReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  GetPrefetchJobReq get{req.user, req.jobId, req.cacheProtocolVersion};
  CO_RETURN_ON_ERROR(co_await getPrefetchJob(get));
  if (!jobCanceller_) co_return makeError(CacheCode::kUnavailable, "prefetch Job canceller is unavailable");
  auto cancelled = co_await jobCanceller_->cancel(req.jobId);
  CO_RETURN_ON_ERROR(cancelled);
  co_return CancelPrefetchJobRsp{std::move(cancelled->job)};
}

CoTryTask<PinDatasetRsp> CacheManagerOperator::pinDataset(const PinDatasetReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  cache::PrefetchJobSpec spec;
  spec.jobId = cache::PrefetchJobId{req.pinId.toUnderType()};
  spec.ownerUid = req.user.uid;
  spec.sources = req.sources;
  spec.priority = req.priority;
  spec.pinAfterReady = true;
  spec.pinTtlMs = req.ttlMs;
  spec.loadMissing = req.prefetchMissing;
  CreatePrefetchJobReq create{req.user, std::move(spec), req.cacheProtocolVersion};
  auto created = co_await createPrefetchJob(create);
  CO_RETURN_ON_ERROR(created);
  PinDatasetRsp response;
  response.pinId = req.pinId;
  response.plannedBytes = created->job.plannedBytes;
  co_return response;
}

CoTryTask<UnpinDatasetRsp> CacheManagerOperator::unpinDataset(const UnpinDatasetReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  if (!metaClient_) co_return makeError(CacheCode::kUnavailable, "metadata client is unavailable");
  auto jobId = cache::PrefetchJobId{req.pinId.toUnderType()};
  GetPrefetchJobReq get{req.user, jobId, req.cacheProtocolVersion};
  auto fetched = co_await getPrefetchJob(get);
  CO_RETURN_ON_ERROR(fetched);
  if (fetched->job.state != cache::PrefetchJobState::READY && fetched->job.state != cache::PrefetchJobState::FAILED &&
      fetched->job.state != cache::PrefetchJobState::CANCELLED && jobCanceller_) {
    CO_RETURN_ON_ERROR(co_await jobCanceller_->cancel(jobId));
  }
  uint64_t removed = 0;
  for (auto kind :
       {cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerKind::EXPLICIT_PIN, cache::PinOwnerKind::POST_READY}) {
    meta::RemoveCachePinsReq remove;
    remove.service = {config_.service_name(), config_.service_token()};
    remove.owner = {kind, req.pinId};
    remove.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto result = co_await metaClient_->removeCachePins(std::move(remove));
    CO_RETURN_ON_ERROR(result);
    if (result->removed > std::numeric_limits<uint64_t>::max() - removed) {
      co_return makeError(CacheCode::kStateConflict, "removed pin counter overflow");
    }
    removed += result->removed;
  }
  co_return UnpinDatasetRsp{removed};
}

CoTryTask<GetPinStatusRsp> CacheManagerOperator::getPinStatus(const GetPinStatusReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));
  if (!metaClient_) co_return makeError(CacheCode::kUnavailable, "metadata client is unavailable");
  auto jobId = cache::PrefetchJobId{req.pinId.toUnderType()};
  GetPrefetchJobReq get{req.user, jobId, req.cacheProtocolVersion};
  auto fetched = co_await getPrefetchJob(get);
  CO_RETURN_ON_ERROR(fetched);
  GetPinStatusRsp response;
  response.pinId = req.pinId;
  response.plannedBytes = fetched->job.plannedBytes;
  response.readyBytes = fetched->job.readyBytes;
  std::vector<cache::PinRecord> pins;
  for (auto kind :
       {cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerKind::EXPLICIT_PIN, cache::PinOwnerKind::POST_READY}) {
    std::optional<cache::CacheBlockKey> after;
    do {
      meta::ListCachePinsByOwnerReq list;
      list.service = {config_.service_name(), config_.service_token()};
      list.owner = {kind, req.pinId};
      list.after = after;
      list.limit = config_.phase3_plan_page_size();
      list.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
      auto page = co_await metaClient_->listCachePinsByOwner(std::move(list));
      CO_RETURN_ON_ERROR(page);
      pins.insert(pins.end(), page->pins.begin(), page->pins.end());
      if (!page->more) break;
      if (page->pins.empty()) co_return makeError(CacheCode::kInvalidResponse, "pin status page did not advance");
      after = page->pins.back().key;
    } while (true);
  }
  std::optional<cache::CacheBlockKey> after;
  do {
    meta::ListPrefetchPlanReq list;
    list.service = {config_.service_name(), config_.service_token()};
    list.jobId = jobId;
    list.after = after;
    list.limit = config_.phase3_plan_page_size();
    list.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto page = co_await metaClient_->listPrefetchPlan(std::move(list));
    CO_RETURN_ON_ERROR(page);
    for (const auto &entry : page->entries) {
      auto found = std::find_if(pins.begin(), pins.end(), [&](const auto &pin) { return pin.key == entry.key; });
      if (found == pins.end()) continue;
      if (entry.blockLength > std::numeric_limits<uint64_t>::max() - response.pinnedBytes) {
        co_return makeError(CacheCode::kStateConflict, "pinned byte counter overflow");
      }
      response.pinnedBytes += entry.blockLength;
      response.expiresAtMs = std::max(response.expiresAtMs, found->expiresAtMs);
    }
    if (!page->more) break;
    if (page->entries.empty()) co_return makeError(CacheCode::kInvalidResponse, "pin plan page did not advance");
    after = page->entries.back().key;
  } while (true);
  co_return response;
}

}  // namespace hf3fs::cache_manager
