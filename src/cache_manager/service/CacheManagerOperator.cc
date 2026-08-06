#include "cache_manager/service/CacheManagerOperator.h"

#include <cmath>
#include <folly/experimental/coro/BlockingWait.h>
#include <limits>

#include "cache/origin/RoutedObjectStore.h"
#include "cache_manager/planner/ManifestPlanner.h"
#include "cache_manager/planner/NamespaceDatasetPlanner.h"
#include "cache_manager/planner/S3PrefixPlanner.h"

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
    jobPlanner_ =
        std::make_shared<JobPlanner>(plannerBackend, std::move(sharedFactory), config_.phase3_plan_page_size());
    jobQuota_ = std::make_unique<JobQuota>();
    auto runnerBackend = std::make_shared<MetaJobRunnerBackend>(metaClient_, service, *ensureCached_);
    jobRunner_ = std::make_shared<JobRunner>(runnerBackend, *jobQuota_, config_.phase3_plan_page_size(), Uuid::random);
    auto trackerBackend = std::make_shared<MetaJobTrackerBackend>(metaClient_, service);
    jobTracker_ = std::make_shared<JobTracker>(trackerBackend, config_.phase3_plan_page_size());
    auto cancellerBackend = std::make_shared<MetaJobCancellerBackend>(metaClient_, service, backend_);
    jobCanceller_ = std::make_shared<JobCanceller>(cancellerBackend, hints_, config_.phase3_plan_page_size());
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
        });
    auto recovered = folly::coro::blockingWait(orchestration_->recover());
    if (recovered.hasError()) {
      orchestration_.reset();
      if (accessFlushWorker_) accessFlushWorker_->stop();
      return makeError(recovered.error());
    }
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
    if (accessFlushWorker_) accessFlushWorker_->stop();
    return makeError(StatusCode::kQueueConflict, "failed to start cache eviction controller");
  }
  if (config_.enable_phase2()) {
    evictingWorker_ = std::make_unique<EvictingWorker>(backend_, config_.evicting_page_size());
    if (!scheduler->start(
            "CacheManagerEvictingWorker",
            [this]() -> CoTask<void> {
              auto result = co_await evictingWorker_->runOnce();
              if (result.hasError()) XLOGF(WARN, "EVICTING recovery iteration failed: {}", result.error());
            },
            [this] { return config_.evicting_scan_interval(); })) {
      folly::coro::blockingWait(scheduler->stopAll());
      if (accessFlushWorker_) accessFlushWorker_->stop();
      return makeError(StatusCode::kQueueConflict, "failed to start EVICTING recovery worker");
    }
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
                       [this] { return orchestration_->runPlannerOnce(); },
                       config_.phase3_planner_interval()) &&
                   startWorker(
                       "CacheManagerJobRunner",
                       [this] { return orchestration_->runRunnerOnce(); },
                       config_.phase3_runner_interval()) &&
                   startWorker(
                       "CacheManagerJobTracker",
                       [this] { return orchestration_->runTrackerOnce(); },
                       config_.phase3_tracker_interval());
    if (!started) {
      orchestration_->stop();
      folly::coro::blockingWait(scheduler->stopAll());
      if (accessFlushWorker_) accessFlushWorker_->stop();
      return makeError(StatusCode::kQueueConflict, "failed to start cache orchestration workers");
    }
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
  if (orchestration_) orchestration_->stop();
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
  return cache::checkPhase3Capability(version, config_.enable_phase3());
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

#define PHASE3_DISABLED_METHOD(NAME, REQ, RSP)                                         \
  CoTryTask<RSP> CacheManagerOperator::NAME(const REQ &req) {                          \
    CO_RETURN_ON_ERROR(req.valid());                                                   \
    CO_RETURN_ON_ERROR(checkPhase3Protocol(req.cacheProtocolVersion));                 \
    co_return makeError(CacheCode::kFeatureDisabled, "cache phase three is disabled"); \
  }

PHASE3_DISABLED_METHOD(createPrefetchJob, CreatePrefetchJobReq, CreatePrefetchJobRsp);
PHASE3_DISABLED_METHOD(getPrefetchJob, GetPrefetchJobReq, GetPrefetchJobRsp);
PHASE3_DISABLED_METHOD(listPrefetchJobs, ListPrefetchJobsReq, ListPrefetchJobsRsp);
PHASE3_DISABLED_METHOD(cancelPrefetchJob, CancelPrefetchJobReq, CancelPrefetchJobRsp);
PHASE3_DISABLED_METHOD(pinDataset, PinDatasetReq, PinDatasetRsp);
PHASE3_DISABLED_METHOD(unpinDataset, UnpinDatasetReq, UnpinDatasetRsp);
PHASE3_DISABLED_METHOD(getPinStatus, GetPinStatusReq, GetPinStatusRsp);
#undef PHASE3_DISABLED_METHOD

}  // namespace hf3fs::cache_manager
