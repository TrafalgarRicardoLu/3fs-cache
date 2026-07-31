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
    if (admission_policy() != "second_miss") {
      return makeError(StatusCode::kInvalidConfig, "unknown cache admission policy");
    }
    if (eviction_policy() != "lru") {
      return makeError(StatusCode::kInvalidConfig, "unknown cache eviction policy");
    }
    if (capacity_low_watermark() <= 0.0 || capacity_low_watermark() >= capacity_high_watermark()) {
      return makeError(StatusCode::kInvalidConfig, "cache capacity low watermark must be below high watermark");
    }
    if (eviction_page_size() == 0 || eviction_page_size() > cache::kMaxPhase2BatchItems || eviction_batch_size() == 0 ||
        eviction_batch_size() > cache::kMaxPhase2BatchItems) {
      return makeError(StatusCode::kInvalidConfig, "cache eviction batch configuration exceeds protocol limit");
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
  CONFIG_ITEM(admission_policy, std::string{"second_miss"});
  CONFIG_ITEM(eviction_policy, std::string{"lru"});
  CONFIG_ITEM(second_miss_window, 30_s, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(second_miss_max_entries, uint32_t{65536}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(capacity_high_watermark, 0.9, [](double value) { return value > 0.0 && value < 1.0; });
  CONFIG_ITEM(capacity_low_watermark, 0.8, [](double value) { return value > 0.0 && value < 1.0; });
  CONFIG_ITEM(space_poll_interval, 5_s, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(space_snapshot_max_age, 15_s, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(eviction_interval, 5_s, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(eviction_protection_period, 10_min, [](Duration value) { return value >= 0_ns; });
  CONFIG_ITEM(eviction_page_size, uint32_t{1000}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(eviction_batch_size, uint32_t{256}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(storage_permit_ttl, 60_s, [](Duration value) { return value > 0_ns; });
  CONFIG_ITEM(access_flush_threshold, uint32_t{256}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(access_flush_batch_size, uint32_t{512}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(access_max_entries, uint32_t{65536}, ConfigCheckers::checkPositive);
  CONFIG_ITEM(access_flush_interval, 1_s, [](Duration value) { return value > 0_ns; });
  CONFIG_OBJ_ARRAY(origins, OriginConfig, 64, [](auto &) { return 0; });
};

}  // namespace hf3fs::cache_manager
