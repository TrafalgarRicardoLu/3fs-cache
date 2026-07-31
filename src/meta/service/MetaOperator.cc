#include "meta/service/MetaOperator.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <fcntl.h>
#include <fmt/core.h>
#include <folly/Conv.h>
#include <folly/Expected.h>
#include <folly/Overload.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Invoke.h>
#include <folly/experimental/coro/Sleep.h>
#include <folly/functional/Invoke.h>
#include <folly/functional/Partial.h>
#include <folly/logging/xlog.h>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/app/NodeId.h"
#include "common/kv/ITransaction.h"
#include "common/kv/WithTransaction.h"
#include "common/monitor/Recorder.h"
#include "common/serde/ClientContext.h"
#include "common/utils/BackgroundRunner.h"
#include "common/utils/CPUExecutorGroup.h"
#include "common/utils/Coroutine.h"
#include "common/utils/CoroutinesPool.h"
#include "common/utils/Duration.h"
#include "common/utils/FaultInjection.h"
#include "common/utils/Result.h"
#include "common/utils/RobinHood.h"
#include "common/utils/UtcTime.h"
#include "core/user/UserToken.h"
#include "fbs/meta/Service.h"
#include "fbs/meta/Utils.h"
#include "fdb/FDBRetryStrategy.h"
#include "meta/components/ChainAllocator.h"
#include "meta/components/Distributor.h"
#include "meta/components/FileHelper.h"
#include "meta/components/Forward.h"
#include "meta/components/InodeIdAllocator.h"
#include "meta/components/OriginNamespaceManager.h"
#include "meta/components/SessionManager.h"
#include "meta/store/Idempotent.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/PathResolve.h"
#include "meta/store/Utils.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "meta/store/ops/BatchOperation.h"

#define AUTHENTICATE(user)                             \
  do {                                                 \
    if (config_.authenticate()) {                      \
      CO_RETURN_ON_ERROR(co_await authenticate(user)); \
    }                                                  \
  } while (0)

namespace hf3fs::meta::server {
using namespace std::chrono_literals;

template <typename Func, typename Arg>
auto MetaOperator::runOp(Func &&func, Arg &&arg)
    -> CoTryTask<typename std::invoke_result_t<Func, MetaStore, Arg &&>::element_type::RspT> {
#ifndef NDEBUG
  auto fi = FaultInjection::clone();
#endif
  auto deadline = std::optional<SteadyTime>();
  if constexpr (std::is_base_of_v<ReqBase, std::remove_reference_t<Arg>>) {
    CO_RETURN_ON_ERROR(arg.valid());
    if (config_.operation_timeout() != 0_s) {
      deadline = SteadyClock::now() + config_.operation_timeout();
    }
  }
  auto txn = kvEngine_->createReadWriteTransaction();
  auto op = ((*metaStore_).*func)(std::forward<Arg>(arg));
  auto driver = OperationDriver(*op, arg, deadline);
  co_return co_await driver.run(std::move(txn), createRetryConfig(), config_.readonly(), config_.grv_cache());
}

CoTryTask<Inode> MetaOperator::runBatch(InodeId inodeId,
                                        std::unique_ptr<BatchedOp> op,
                                        std::optional<SteadyTime> deadline) {
#ifndef NDEBUG
  auto fi = FaultInjection::clone();
#endif
  assert(op);
  auto txn = kvEngine_->createReadWriteTransaction();
  auto driver = OperationDriver(*op, Void{}, deadline);
  auto result = co_await driver.run(std::move(txn), createRetryConfig(), config_.readonly(), config_.grv_cache());
  if (!result.hasError()) {
    XLOGF_IF(FATAL, inodeId != result->id, "expected {}, get {}", inodeId, result->id);
  }

  batches_.withLock(
      [&](auto &map) {
        auto iter = map.find(op->inodeId_);
        XLOGF_IF(FATAL, iter == map.end(), "shouldn't happen");
        if (!iter->second.wakeupNext()) {
          map.erase(iter);
        }
      },
      op->inodeId_);
  co_return result;
}

template <typename Req, typename Rsp>
CoTryTask<Rsp> MetaOperator::runInBatch(InodeId inodeId, Req req) {
  CO_RETURN_ON_ERROR(req.valid());
  auto deadline = std::optional<SteadyTime>();
  if (config_.operation_timeout() != 0_s) {
    deadline = SteadyClock::now() + config_.operation_timeout();
  }
  OperationRecorder::Guard guard(OperationRecorder::server(), MetaSerde<>::getRpcName(req), req.user.uid);
  BatchedOp::Waiter<Req, Rsp> waiter(std::move(req));
  auto op = addBatchReq(inodeId, waiter);
  co_await waiter.baton;
  if (op) {
    co_await runBatch(inodeId, std::move(op), deadline);
  }
  auto result = waiter.getResult();
  guard.finish(result);
  co_return result;
}

MetaOperator::MetaOperator(const Config &cfg,
                           flat::NodeId nodeId,
                           std::shared_ptr<kv::IKVEngine> kvEngine,
                           std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                           std::shared_ptr<storage::client::StorageClient> storageClient,
                           std::unique_ptr<Forward> forward)
    : config_(cfg),
      nodeId_(nodeId),
      metaEventTraceLog_(config_.event_trace_log()),
      kvEngine_(kvEngine),
      mgmtd_(mgmtdClient),
      distributor_(std::make_shared<Distributor>(cfg.distributor(), nodeId, kvEngine)),
      userStore_(std::make_shared<core::UserStoreEx>(*kvEngine_, config_.retry_transaction(), config_.user_cache())),
      inodeIdAlloc_(InodeIdAllocator::create(kvEngine)),
      chainAlloc_(std::make_shared<ChainAllocator>(mgmtdClient)),
      fileHelper_(std::make_shared<FileHelper>(cfg, mgmtdClient, storageClient)),
      sessionManager_(
          std::make_shared<SessionManager>(cfg.session_manager(), nodeId, kvEngine_, mgmtdClient, fileHelper_)),
      gcManager_(std::make_shared<GcManager>(cfg,
                                             nodeId,
                                             metaEventTraceLog_,
                                             kvEngine_,
                                             mgmtdClient,
                                             inodeIdAlloc_,
                                             fileHelper_,
                                             sessionManager_,
                                             userStore_)),
      forward_(std::move(forward)),
      metaStore_(std::make_unique<MetaStore>(cfg,
                                             metaEventTraceLog_,
                                             distributor_,
                                             inodeIdAlloc_,
                                             chainAlloc_,
                                             fileHelper_,
                                             sessionManager_,
                                             gcManager_)) {
  sessionManager_->setCloseFunc(
      [&](const auto &req) -> CoTryTask<void> { co_return (co_await close(req)).then([](auto &) { return Void{}; }); });
}

CoTryTask<void> MetaOperator::init(std::optional<Layout> layout) {
  XLOGF(INFO, "MetaOperator::init");
  if (layout.has_value()) {
    CO_RETURN_ON_ERROR(co_await runOp(&MetaStore::initFs, *layout));
  }

  if (!metaEventTraceLog_.open()) {
    XLOGF(CRITICAL, "Failed to open trace log in directory: {}", config_.event_trace_log().trace_file_dir());
    co_return makeError(StatusCode::kIOError);
  }

  CO_RETURN_ON_ERROR(co_await gcManager_->init());
  XLOGF(INFO, "MetaOperator::init success.");
  co_return Void{};
}

void MetaOperator::start(CPUExecutorGroup &exec) {
  XLOGF(INFO, "MetaOperator::start");

  distributor_->start(exec);
  fileHelper_->start(exec);
  gcManager_->start(exec);
  sessionManager_->start(exec);

  bgRunner_ = std::make_unique<BackgroundRunner>(&exec.randomPick());

  bgRunner_->start(
      "idempotent_clean",
      [&]() -> CoTask<void> {
        if (!isFirstMeta(*mgmtd_, nodeId_)) co_return;

        auto prev = std::optional<std::string>();
        size_t total = 0, cleaned = 0;
        auto more = true;
        while (more && !stop_) {
          size_t t = 0, c = 0;
          auto strategy = kv::FDBRetryStrategy(createRetryConfig());
          auto txn = kvEngine_->createReadWriteTransaction();
          auto result = co_await kv::WithTransaction(strategy).run(
              std::move(txn),
              [&](kv::IReadWriteTransaction &txn) -> CoTryTask<std::pair<std::string, bool>> {
                co_return co_await Idempotent::clean(txn, prev, config_.idempotent_record_expire(), 2048, t, c);
              });
          if (!result) {
            XLOGF(ERR, "Clean idempotent record failed, {}", result.error());
            break;
          }
          total += t;
          cleaned += c;
          prev = result->first;
          more = result->second;
        }
        XLOGF(INFO, "Clean idempotent record, total {}, cleaned {}", total, cleaned);
        co_return;
      },
      config_.idempotent_record_clean_getter());
}

void MetaOperator::beforeStop() {
  XLOGF(INFO, "MetaOperator::beforeStop");
  stop_ = true;
  if (distributor_) {
    distributor_->stopAndJoin(true);
  }
  XLOGF(INFO, "MetaOperator::beforeStop finished");
}

void MetaOperator::afterStop() {
  XLOGF(INFO, "MetaOperator::afterStop");
  if (bgRunner_) {
    folly::coro::blockingWait(bgRunner_->stopAll());
    bgRunner_.reset();
  }
  if (gcManager_) {
    gcManager_->stopAndJoin();
  }
  if (sessionManager_) {
    sessionManager_->stopAndJoin();
  }
  if (fileHelper_) {
    fileHelper_->stopAndJoin();
  }
  metaEventTraceLog_.close();
  XLOGF(INFO, "MetaOperator::afterStop finished");
}

kv::FDBRetryStrategy::Config MetaOperator::createRetryConfig() const {
  return kv::FDBRetryStrategy::Config{config_.retry_transaction().max_backoff(),
                                      config_.retry_transaction().max_retry_count(),
                                      true};
}

CoTryTask<void> MetaOperator::authenticate(UserInfo &userInfo) {
  static monitor::CountRecorder failed("meta_server.auth_failed");
  auto guard = folly::makeGuard([&]() {
    failed.addSample(1, {{"uid", folly::to<std::string>(userInfo.uid.toUnderType())}});
  });

  auto ret = co_await userStore_->authenticate(userInfo);
  CO_RETURN_ON_ERROR(ret);

  guard.dismiss();
  co_return Void{};
}

CoTryTask<Void> MetaOperator::requireCacheAdmin(const UserInfo &userInfo) {
  if (userInfo.isRoot()) co_return Void{};
  auto user = co_await userStore_->getUser(userInfo.uid);
  if (user.hasError()) {
    if (user.error().code() != StatusCode::kAuthenticationFail) CO_RETURN_ERROR(user);
    co_return makeError(MetaCode::kNoPermission, "cache admin permission required");
  }
  if (!user->admin) {
    co_return makeError(MetaCode::kNoPermission, "cache admin permission required");
  }
  co_return Void{};
}

Result<Void> MetaOperator::checkCacheFeature(uint32_t protocolVersion) const {
  if (protocolVersion != cache::kCacheProtocolVersion) {
    return makeError(CacheCode::kUpgradeRequired, "incompatible cache protocol version");
  }
  auto routing = mgmtd_->getRoutingInfo();
  if (!routing || !routing->raw() ||
      !routing->raw()->cacheFeatureEnabled(cache::kCacheSchemaVersion, cache::kCacheProtocolVersion)) {
    return makeError(CacheCode::kFeatureDisabled, "cache feature gate is not active");
  }
  return Void{};
}

Result<Void> MetaOperator::checkCachePhase2(uint32_t protocolVersion) const {
  RETURN_ON_ERROR(cache::checkPhase2Capability(protocolVersion, config_.enable_cache_phase2()));
  return checkCacheFeature(protocolVersion);
}

Result<Void> MetaOperator::checkCacheService(const CacheServiceIdentity &service) const {
  RETURN_ON_ERROR(service.valid());
  if (config_.cache_service_token().empty() || service.name != config_.cache_service_name() ||
      service.token != config_.cache_service_token()) {
    return makeError(MetaCode::kNoPermission, "invalid cache service identity");
  }
  return Void{};
}

Result<Void> MetaOperator::checkCacheTable(flat::ChainTableId tableId) const {
  auto routing = mgmtd_->getRoutingInfo();
  auto table = routing && routing->raw() ? routing->raw()->getChainTable(tableId) : nullptr;
  if (table == nullptr || !table->isCacheData()) {
    return makeError(MetaCode::kInvalidFileLayout, "OriginFile requires a CACHE_DATA chain table");
  }
  RETURN_ON_ERROR(table->valid());
  return Void{};
}

CoTryTask<AuthRsp> MetaOperator::authenticate(AuthReq req) {
  AUTHENTICATE(req.user);
  co_return AuthRsp(std::move(req.user));
}

CoTryTask<StatFsRsp> MetaOperator::statFs(StatFsReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::statFs, req);
}

CoTryTask<StatRsp> MetaOperator::stat(StatReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::stat, req);
}

CoTryTask<BatchStatRsp> MetaOperator::batchStat(BatchStatReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::batchStat, req);
}

CoTryTask<BatchStatByPathRsp> MetaOperator::batchStatByPath(BatchStatByPathReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::batchStatByPath, req);
}

CoTryTask<GetRealPathRsp> MetaOperator::getRealPath(GetRealPathReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::getRealPath, req);
}

CoTryTask<OpenRsp> MetaOperator::open(OpenReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::open, req);
}

CoTryTask<TruncateRsp> MetaOperator::truncate(TruncateReq req) {
  XLOGF(CRITICAL, "truncate is deperated, update client {}", req.client.hostname);
  co_return makeError(StatusCode::kNotImplemented, "truncate is deperated, update client");
}

CoTryTask<SyncRsp> MetaOperator::sync(SyncReq req) {
  // NOTE: don't auth user for sync
  auto node = distributor_->getServer(req.inode);
  if (node == distributor_->nodeId()) {
    auto inodeId = req.inode;
    co_return co_await runInBatch<SyncReq, SyncRsp>(inodeId, std::move(req));
  } else {
    co_return co_await forward_->forward<SyncReq, SyncRsp>(node, std::move(req));
  }
}

CoTryTask<CloseRsp> MetaOperator::close(CloseReq req) {
  // Note: don't auth user here
  auto node = distributor_->getServer(req.inode);
  if (node == distributor_->nodeId()) {
    auto inodeId = req.inode;
    co_return co_await runInBatch<CloseReq, CloseRsp>(inodeId, std::move(req));
  } else {
    co_return co_await forward_->forward<CloseReq, CloseRsp>(node, std::move(req));
  }
}

CoTryTask<CreateRsp> MetaOperator::create(CreateReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());

  XLOGF(DBG, "create {}", req);

  if (req.path.path->has_parent_path()) {
    // try open first.
    auto result = co_await runOp(&MetaStore::tryOpen, req);
    if (result.hasValue() || req.path.path->has_parent_path()) {
      co_return result;
    }
    if (!req.valid()) {
      auto msg = fmt::format("req {} not valid after try open", req);
      XLOG(DFATAL, msg);
      co_return makeError(MetaCode::kFoundBug, std::move(msg));
    }

    XLOGF(DBG, "create {}", req);
  }

  auto node = distributor_->getServer(req.path.parent);
  if (node == distributor_->nodeId()) {
    auto parentId = req.path.parent;
    co_return co_await runInBatch<CreateReq, CreateRsp>(parentId, std::move(req));
  } else {
    co_return co_await forward_->forward<CreateReq, CreateRsp>(node, std::move(req));
  }
}

CoTryTask<MkdirsRsp> MetaOperator::mkdirs(MkdirsReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::mkdirs, req);
}

CoTryTask<SymlinkRsp> MetaOperator::symlink(SymlinkReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::symlink, req);
}

CoTryTask<RemoveRsp> MetaOperator::remove(RemoveReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::remove, req);
}

CoTryTask<RenameRsp> MetaOperator::rename(RenameReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::rename, req);
}

CoTryTask<ListRsp> MetaOperator::list(ListReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::list, req);
}

CoTryTask<HardLinkRsp> MetaOperator::hardLink(HardLinkReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::hardLink, req);
}

CoTryTask<SetAttrRsp> MetaOperator::setAttr(SetAttrReq req) {
  AUTHENTICATE(req.user);
  if (req.path.path) {
    co_return co_await runOp(&MetaStore::setAttr, req);
  }

  auto node = distributor_->getServer(req.path.parent);
  if (node == distributor_->nodeId()) {
    auto parentId = req.path.parent;
    co_return co_await runInBatch<SetAttrReq, SetAttrRsp>(parentId, std::move(req));
  } else {
    co_return co_await forward_->forward<SetAttrReq, SetAttrRsp>(node, std::move(req));
  }
}

CoTryTask<LockDirectoryRsp> MetaOperator::lockDirectory(LockDirectoryReq req) {
  AUTHENTICATE(req.user);
  co_return co_await runOp(&MetaStore::lockDirectory, req);
}

CoTryTask<PruneSessionRsp> MetaOperator::pruneSession(PruneSessionReq req) {
  co_return co_await runOp(&MetaStore::pruneSession, req);
}

CoTryTask<DropUserCacheRsp> MetaOperator::dropUserCache(DropUserCacheReq req) {
  if (req.dropAll) {
    userStore_->cache().clear();
  } else if (req.uid) {
    userStore_->cache().clear(*req.uid);
  }
  co_return DropUserCacheRsp{};
}

CoTryTask<TestRpcRsp> MetaOperator::testRpc(TestRpcReq req) {
  // don't need auth user
  co_return co_await runOp(&MetaStore::testRpc, req);
}

CoTryTask<ImportOriginFileRsp> MetaOperator::importOriginFile(ImportOriginFileReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheFeature(req.cacheProtocolVersion));
  CO_RETURN_ON_ERROR(co_await requireCacheAdmin(req.user));
  CO_RETURN_ON_ERROR(checkCacheTable(req.entry.metadata.tableId));
  co_return co_await runOp(&MetaStore::importOriginFile, req);
}

CoTryTask<BatchImportOriginFilesRsp> MetaOperator::batchImportOriginFiles(BatchImportOriginFilesReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheFeature(req.cacheProtocolVersion));
  CO_RETURN_ON_ERROR(co_await requireCacheAdmin(req.user));

  BatchImportOriginFilesRsp rsp;
  rsp.results.reserve(req.entries.size());
  for (size_t i = 0; i < req.entries.size(); ++i) {
    auto duplicates = std::count_if(req.entries.begin(), req.entries.end(), [&](const auto &entry) {
      return entry.path == req.entries[i].path;
    });
    if (duplicates > 1) {
      rsp.results.emplace_back(makeError(StatusCode::kInvalidArg, "duplicate path in batch import"));
      continue;
    }
    if (auto table = checkCacheTable(req.entries[i].metadata.tableId); table.hasError()) {
      rsp.results.emplace_back(makeError(table.error()));
      continue;
    }
    ImportOriginFileReq item;
    item.user = req.user;
    item.client = req.client;
    item.forward = req.forward;
    item.uuid = req.uuid;
    item.entry = req.entries[i];
    item.cacheProtocolVersion = req.cacheProtocolVersion;
    rsp.results.emplace_back(co_await runOp(&MetaStore::importOriginFile, item));
  }
  co_return rsp;
}

CoTryTask<RefreshOriginFileRsp> MetaOperator::refreshOriginFile(RefreshOriginFileReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());
  if (req.cacheProtocolVersion != cache::kCacheProtocolVersion) {
    co_return makeError(CacheCode::kUpgradeRequired, "incompatible cache protocol version");
  }
  CO_RETURN_ON_ERROR(co_await requireCacheAdmin(req.user));

  auto strategy = kv::FDBRetryStrategy(createRetryConfig());
  auto previous = co_await kv::WithTransaction(strategy).run(
      kvEngine_->createReadWriteTransaction(),
      [&](kv::IReadWriteTransaction &txn) { return OriginNamespaceManager::loadRefresh(txn, req.requestId); });
  CO_RETURN_ON_ERROR(previous);
  if (previous->has_value()) {
    if (!(**previous).matches(req)) {
      co_return makeError(CacheCode::kStateConflict, "refresh request id was reused with different parameters");
    }
    co_return (**previous).response();
  }

  CO_RETURN_ON_ERROR(checkCacheFeature(req.cacheProtocolVersion));
  CO_RETURN_ON_ERROR(checkCacheTable(req.newMetadata.tableId));
  co_return co_await runOp(&MetaStore::refreshOriginFile, req);
}

CoTryTask<GetFileReadPlanRsp> MetaOperator::getFileReadPlan(GetFileReadPlanReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheFeature(req.cacheProtocolVersion));
  co_return co_await runOp(&MetaStore::getFileReadPlan, req);
}

#define META_CACHE_MUTATION_METHOD(NAME, REQ, RSP)                       \
  CoTryTask<RSP> MetaOperator::NAME(REQ req) {                           \
    CO_RETURN_ON_ERROR(req.valid());                                     \
    CO_RETURN_ON_ERROR(checkCacheService(req.service));                  \
    CO_RETURN_ON_ERROR(checkCacheFeature(cache::kCacheProtocolVersion)); \
    co_return co_await runOp(&MetaStore::NAME, req);                     \
  }

META_CACHE_MUTATION_METHOD(enqueueCacheBlocks, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
META_CACHE_MUTATION_METHOD(acquireCacheBlocks, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
META_CACHE_MUTATION_METHOD(commitCacheBlocks, CommitCacheBlocksReq, CommitCacheBlocksRsp);
META_CACHE_MUTATION_METHOD(failCacheBlocks, FailCacheBlocksReq, FailCacheBlocksRsp);
META_CACHE_MUTATION_METHOD(beginCleanCacheBlocks, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
META_CACHE_MUTATION_METHOD(finishCleanCacheBlocks, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);

#undef META_CACHE_MUTATION_METHOD

CoTryTask<GetCacheStatusRsp> MetaOperator::getCacheStatus(GetCacheStatusReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheFeature(req.cacheProtocolVersion));
  CO_RETURN_ON_ERROR(co_await requireCacheAdmin(req.user));
  auto handler = [inode = req.inode](kv::IReadOnlyTransaction &transaction) -> CoTryTask<GetCacheStatusRsp> {
    auto capacity = co_await CacheCapacityStore::snapshotLoad(transaction);
    CO_RETURN_ON_ERROR(capacity);
    auto records =
        co_await CacheBlockStore::snapshotListAll(transaction,
                                                  inode ? std::optional<uint64_t>{inode->u64()} : std::nullopt);
    CO_RETURN_ON_ERROR(records);

    GetCacheStatusRsp response;
    response.logicalCapacity = capacity->logicalCapacity;
    response.usedCapacity = capacity->usedBytes;
    response.reservedCapacity = capacity->reservedBytes;
    response.committedCapacity = capacity->committedBytes;
    std::map<cache::CacheBlockState, uint64_t> states;
    std::map<cache::ChargeKind, CacheChargeCount> charges;
    for (const auto &record : *records) {
      ++states[record.state];
      auto &charge = charges[record.chargeKind];
      charge.kind = record.chargeKind;
      ++charge.count;
      charge.bytes += record.chargedBytes;
    }
    for (const auto &[state, count] : states) response.stateCounts.push_back({state, count});
    for (const auto &[kind, count] : charges) response.chargeCounts.push_back(count);
    co_return response;
  };
  co_return co_await kv::WithTransaction(kv::FDBRetryStrategy(createRetryConfig()))
      .run(kvEngine_->createReadonlyTransaction(), std::move(handler));
}

CoTryTask<ListCacheBlocksRsp> MetaOperator::listCacheBlocks(ListCacheBlocksReq req) {
  AUTHENTICATE(req.user);
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheFeature(req.cacheProtocolVersion));
  CO_RETURN_ON_ERROR(co_await requireCacheAdmin(req.user));
  auto handler = [req](kv::IReadOnlyTransaction &transaction) -> CoTryTask<ListCacheBlocksRsp> {
    auto page = co_await CacheBlockStore::snapshotList(transaction, req.inode.u64(), req.beginBlock, req.limit);
    CO_RETURN_ON_ERROR(page);
    ListCacheBlocksRsp response;
    response.more = page->more;
    response.blocks.reserve(page->records.size());
    for (const auto &record : page->records) {
      response.blocks.push_back({record.key, record.state, record.ready, record.chargeKind, record.chargedBytes});
    }
    co_return response;
  };
  co_return co_await kv::WithTransaction(kv::FDBRetryStrategy(createRetryConfig()))
      .run(kvEngine_->createReadonlyTransaction(), std::move(handler));
}

CoTryTask<ListRecoverableCachePermitsRsp> MetaOperator::listRecoverableCachePermits(
    ListRecoverableCachePermitsReq req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheService(req.service));
  CO_RETURN_ON_ERROR(checkCachePhase2(req.cacheProtocolVersion));
  auto handler = [req](kv::IReadOnlyTransaction &transaction) -> CoTryTask<ListRecoverableCachePermitsRsp> {
    auto records = co_await CacheBlockStore::snapshotListAll(transaction);
    CO_RETURN_ON_ERROR(records);
    auto less = [](const cache::CacheBlockKey &lhs, const cache::CacheBlockKey &rhs) {
      return lhs.inode != rhs.inode ? lhs.inode < rhs.inode : lhs.block < rhs.block;
    };
    std::sort(records->begin(), records->end(), [&](const auto &lhs, const auto &rhs) {
      return less(lhs.key, rhs.key);
    });
    std::vector<RecoverableCachePermit> recoverable;
    for (const auto &record : *records) {
      if ((record.state != cache::CacheBlockState::QUEUED && record.state != cache::CacheBlockState::LOADING) ||
          !record.permit || (req.after && !less(*req.after, record.key))) {
        continue;
      }
      recoverable.push_back(
          {record.key, record.state, record.blockLength, *record.permit, record.loaderId, record.loadEpoch});
    }
    ListRecoverableCachePermitsRsp response;
    response.more = recoverable.size() > req.limit;
    if (response.more) recoverable.resize(req.limit);
    response.items = std::move(recoverable);
    co_return response;
  };
  co_return co_await kv::WithTransaction(kv::FDBRetryStrategy(createRetryConfig()))
      .run(kvEngine_->createReadonlyTransaction(), std::move(handler));
}

CoTryTask<CancelQueuedAdmissionsRsp> MetaOperator::cancelQueuedAdmissions(CancelQueuedAdmissionsReq req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(checkCacheService(req.service));
  CO_RETURN_ON_ERROR(checkCachePhase2(req.cacheProtocolVersion));
  auto handler = [req](kv::IReadWriteTransaction &transaction) -> CoTryTask<CancelQueuedAdmissionsRsp> {
    CancelQueuedAdmissionsRsp response;
    response.results.reserve(req.items.size());
    for (const auto &item : req.items) {
      auto valid = item.valid();
      if (valid.hasError()) {
        response.results.push_back(makeError(valid.error()));
        continue;
      }
      auto loaded = co_await CacheBlockStore::load(transaction, item.key);
      if (loaded.hasError()) {
        response.results.push_back(makeError(loaded.error()));
        continue;
      }
      if (!loaded->has_value()) {
        response.results.push_back(
            CacheBlockMutationResult{item.key, cache::CacheBlockState::NONE, cache::CacheEnqueueOutcome::INVALID});
        continue;
      }
      const auto &record = **loaded;
      if (record.state != cache::CacheBlockState::QUEUED || record.permit != item.expectedPermit) {
        response.results.push_back(makeError(CacheCode::kStateConflict, "queued admission cancellation fence changed"));
        continue;
      }
      auto released = co_await CacheCapacityStore::release(transaction, record.chargeKind, record.chargedBytes);
      CO_RETURN_ON_ERROR(released);
      auto removed = co_await CacheBlockStore::remove(transaction, item.key);
      CO_RETURN_ON_ERROR(removed);
      response.results.push_back(
          CacheBlockMutationResult{item.key, cache::CacheBlockState::NONE, cache::CacheEnqueueOutcome::INVALID});
    }
    co_return response;
  };
  co_return co_await kv::WithTransaction(kv::FDBRetryStrategy(createRetryConfig()))
      .run(kvEngine_->createReadWriteTransaction(), std::move(handler));
}

#define META_PHASE2_DISABLED_METHOD(NAME, REQ, RSP)                                \
  CoTryTask<RSP> MetaOperator::NAME(REQ req) {                                     \
    CO_RETURN_ON_ERROR(req.valid());                                               \
    CO_RETURN_ON_ERROR(checkCachePhase2(req.cacheProtocolVersion));                \
    co_return makeError(StatusCode::kNotImplemented, #NAME " is not implemented"); \
  }
META_PHASE2_DISABLED_METHOD(updateCacheBlockAccess, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
META_PHASE2_DISABLED_METHOD(beginEvictCacheBlocks, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
META_PHASE2_DISABLED_METHOD(listEvictingCacheBlocks, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
META_PHASE2_DISABLED_METHOD(reportCacheStorageEvents, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
META_PHASE2_DISABLED_METHOD(listCacheEventDeadLetters, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
#undef META_PHASE2_DISABLED_METHOD

}  // namespace hf3fs::meta::server
