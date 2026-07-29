#pragma once

#include <memory>

#include "cache_manager/config/Config.h"
#include "cache_manager/service/CacheManagerOperator.h"
#include "common/logging/LogConfig.h"
#include "core/app/ServerAppConfig.h"
#include "core/app/ServerLauncher.h"
#include "core/app/ServerLauncherConfig.h"
#include "core/app/ServerMgmtdClientFetcher.h"

namespace hf3fs::cache_manager {

class CacheManagerServer : public net::Server {
 public:
  static constexpr auto kName = "CacheManager";
  static constexpr auto kNodeType = flat::NodeType::CLIENT;

  struct CommonConfig : public ApplicationBase::Config {
    CommonConfig() {
      using logging::LogConfig;
      log().set_categories({LogConfig::makeRootCategoryConfig(), LogConfig::makeEventCategoryConfig()});
      log().set_handlers({LogConfig::makeNormalHandlerConfig(),
                          LogConfig::makeErrHandlerConfig(),
                          LogConfig::makeFatalHandlerConfig(),
                          LogConfig::makeEventHandlerConfig()});
    }
  };

  using AppConfig = core::ServerAppConfig;
  struct LauncherConfig : public core::ServerLauncherConfig {
    LauncherConfig() { mgmtd_client() = client::MgmtdClientForServer::Config{}; }
  };
  using RemoteConfigFetcher = core::launcher::ServerMgmtdClientFetcher;
  using Launcher = core::ServerLauncher<CacheManagerServer>;
  using Config = cache_manager::Config;

  explicit CacheManagerServer(const Config &config);
  ~CacheManagerServer() override;

  Result<Void> beforeStart() final;
  Result<Void> beforeStop() final;
  Result<Void> afterStop() final;

 private:
  void rollbackDependencies();

  const Config &config_;
  std::unique_ptr<net::Client> backgroundClient_;
  std::shared_ptr<client::MgmtdClientForServer> mgmtdClient_;
  std::shared_ptr<storage::client::StorageClient> storageClient_;
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::unique_ptr<CacheManagerOperator> operator_;
};

}  // namespace hf3fs::cache_manager
