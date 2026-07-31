#pragma once

#include <cstdint>
#include <optional>
#include <set>

#include "client/meta/MetaClient.h"
#include "client/mgmtd/MgmtdClientForServer.h"
#include "client/storage/StorageClient.h"
#include "common/net/Client.h"
#include "common/net/Server.h"
#include "common/utils/ConfigBase.h"
#include "common/utils/Duration.h"

namespace hf3fs::cache_manager {

class OriginConfig : public ConfigBase<OriginConfig> {
  CONFIG_ITEM(origin_id, uint32_t{0});
  CONFIG_ITEM(endpoint, std::string{});
  CONFIG_ITEM(region, std::string{"us-east-1"});
  CONFIG_ITEM(use_tls, true);
  CONFIG_ITEM(path_style, false);
  CONFIG_ITEM(max_concurrent_requests, uint32_t{16}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(max_inflight_bytes, uint64_t{256_MB}, ConfigCheckers::checkPositive);
};

class Config : public ConfigBase<Config> {
 public:
  Result<Void> validateRuntime() const {
    if (service_name().empty() || service_token().empty()) {
      return makeError(StatusCode::kInvalidConfig, "cache manager service identity is empty");
    }
    if (range_size() == 0 || max_inflight_bytes() < range_size()) {
      return makeError(StatusCode::kInvalidConfig, "invalid cache manager range or inflight byte limit");
    }
    std::set<uint32_t> ids;
    for (size_t i = 0; i < origins_length(); ++i) {
      const auto &origin = origins(i);
      if (origin.origin_id() == 0) {
        return makeError(StatusCode::kInvalidConfig, "invalid origin mapping");
      }
      if (!ids.emplace(origin.origin_id()).second) {
        return makeError(StatusCode::kInvalidConfig, "duplicate origin id");
      }
    }
    return Void{};
  }

  const OriginConfig *findOrigin(uint32_t originId) const {
    for (size_t i = 0; i < origins_length(); ++i) {
      if (origins(i).origin_id() == originId) return &origins(i);
    }
    return nullptr;
  }

  CONFIG_OBJ(base, net::Server::Config, [](net::Server::Config &c) {
    c.set_groups_length(2);
    c.groups(0).listener().set_listen_port(8000);
    c.groups(0).set_services({"CacheManagerSerde"});
    c.groups(1).set_network_type(net::Address::TCP);
    c.groups(1).listener().set_listen_port(9000);
    c.groups(1).set_use_independent_thread_pool(true);
    c.groups(1).set_services({"Core"});
  });
  CONFIG_OBJ(background_client, net::Client::Config);
  CONFIG_OBJ(mgmtd_client, client::MgmtdClientForServer::Config);
  CONFIG_OBJ(storage_client, storage::client::StorageClient::Config);
  CONFIG_OBJ(meta_client, meta::client::MetaClient::Config);
  CONFIG_ITEM(service_name, std::string{"cache-manager"});
  CONFIG_ITEM(enable_phase2, false);
  CONFIG_ITEM(service_token, std::string{});
  CONFIG_ITEM(global_concurrency, uint32_t{64}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(max_inflight_bytes, uint64_t{1_GB}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(range_size, uint64_t{4_MB}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(load_lease, 30_s, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(hint_timeout, 500_ms, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(scheduler_interval, 100_ms, [](Duration value) { return value > 0_ns; });
  CONFIG_OBJ_ARRAY(origins, OriginConfig, 64, [](auto &) { return 0; });
};

}  // namespace hf3fs::cache_manager
