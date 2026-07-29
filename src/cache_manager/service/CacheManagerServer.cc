#include "cache_manager/service/CacheManagerServer.h"

#include <algorithm>
#include <folly/ScopeGuard.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/logging/xlog.h>

#include "cache/origin/s3/S3ObjectStore.h"
#include "cache_manager/service/CacheManagerSerdeService.h"
#include "common/app/ApplicationBase.h"
#include "core/service/CoreService.h"
#include "stubs/common/RealStubFactory.h"
#include "stubs/mgmtd/MgmtdServiceStub.h"

namespace hf3fs::cache_manager {

CacheManagerServer::CacheManagerServer(const Config &config)
    : net::Server(config.base()),
      config_(config) {}

CacheManagerServer::~CacheManagerServer() { XLOGF(INFO, "Destroying CacheManagerServer"); }

Result<Void> CacheManagerServer::beforeStart() {
  RETURN_ON_ERROR(config_.validateRuntime());
  bool committed = false;
  auto rollback = folly::makeGuard([&] {
    if (!committed) rollbackDependencies();
  });

  backgroundClient_ = std::make_unique<net::Client>(config_.background_client());
  RETURN_ON_ERROR(backgroundClient_->start());

  auto ctxCreator = [this](net::Address addr) { return backgroundClient_->serdeCtx(addr); };
  mgmtdClient_ = std::make_shared<client::MgmtdClientForServer>(
      appInfo().clusterId,
      std::make_unique<stubs::RealStubFactory<mgmtd::MgmtdServiceStub>>(ctxCreator),
      config_.mgmtd_client());
  mgmtdClient_->setAppInfoForHeartbeat(appInfo());
  mgmtdClient_->setConfigListener(ApplicationBase::updateConfig);
  mgmtdClient_->updateHeartbeatPayload(flat::MetaHeartbeatInfo{});
  folly::coro::blockingWait(mgmtdClient_->start(&tpg().bgThreadPool().randomPick()));
  RETURN_ON_ERROR(folly::coro::blockingWait(mgmtdClient_->refreshRoutingInfo(false)));

  auto clientId = ClientId::random(appInfo().hostname);
  storageClient_ = storage::client::StorageClient::create(clientId, config_.storage_client(), *mgmtdClient_);
  if (!storageClient_) return makeError(StatusCode::kInvalidConfig, "failed to create storage client");

  auto metaCtxCreator = [this](net::Address addr) { return backgroundClient_->serdeCtx(addr); };
  metaClient_ = std::make_shared<meta::client::MetaClient>(
      clientId,
      config_.meta_client(),
      std::make_unique<meta::client::MetaClient::StubFactory>(std::move(metaCtxCreator)),
      mgmtdClient_,
      storageClient_,
      false);
  metaClient_->start(tpg().bgThreadPool());

  RealCacheManagerBackend::Stores stores;
  for (size_t i = 0; i < config_.origins_length(); ++i) {
    const auto &origin = config_.origins(i);
    cache::origin::s3::S3ObjectStoreConfig storeConfig;
    storeConfig.ioThreads = std::max(uint32_t{1}, origin.max_concurrent_requests());
    storeConfig.maxConcurrentRequests = origin.max_concurrent_requests();
    storeConfig.maxInflightBytes = origin.max_inflight_bytes();
    cache::origin::s3::AwsS3ClientConfig clientConfig;
    clientConfig.region = origin.region();
    clientConfig.endpoint = origin.endpoint();
    clientConfig.useTls = origin.use_tls();
    clientConfig.pathStyle = origin.path_style();
    clientConfig.maxConnections = origin.max_concurrent_requests();
    auto store = cache::origin::s3::S3ObjectStore::createAws(storeConfig, clientConfig);
    RETURN_ON_ERROR(store);
    stores.emplace(cache::OriginId{origin.origin_id()}, std::move(*store));
  }
  operator_ =
      std::make_unique<CacheManagerOperator>(config_, metaClient_, storageClient_, mgmtdClient_, std::move(stores));
  RETURN_ON_ERROR(operator_->start(tpg().bgThreadPool()));
  RETURN_ON_ERROR(addSerdeService(std::make_unique<CacheManagerSerdeService>(*operator_), true));
  RETURN_ON_ERROR(addSerdeService(std::make_unique<core::CoreService>()));

  committed = true;
  return Void{};
}

Result<Void> CacheManagerServer::beforeStop() {
  if (operator_) operator_->stop();
  if (metaClient_) metaClient_->stop();
  if (mgmtdClient_) folly::coro::blockingWait(mgmtdClient_->stop());
  return Void{};
}

Result<Void> CacheManagerServer::afterStop() {
  if (backgroundClient_) backgroundClient_->stopAndJoin();
  operator_.reset();
  metaClient_.reset();
  storageClient_.reset();
  mgmtdClient_.reset();
  backgroundClient_.reset();
  return Void{};
}

void CacheManagerServer::rollbackDependencies() {
  if (operator_) operator_->stop();
  if (metaClient_) metaClient_->stop();
  if (mgmtdClient_) folly::coro::blockingWait(mgmtdClient_->stop());
  if (backgroundClient_) backgroundClient_->stopAndJoin();
  operator_.reset();
  metaClient_.reset();
  storageClient_.reset();
  mgmtdClient_.reset();
  backgroundClient_.reset();
}

}  // namespace hf3fs::cache_manager
