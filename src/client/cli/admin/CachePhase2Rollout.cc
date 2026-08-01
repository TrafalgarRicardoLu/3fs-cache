#include "CachePhase2Rollout.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/kv/WithTransaction.h"
#include "common/utils/MagicEnum.hpp"
#include "fdb/FDBRetryStrategy.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "mgmtd/service/CachePhase2Rollout.h"

namespace hf3fs::client::cli {
bool cachePhase2InventoryHealthy(size_t metadataRecords, const cache_manager::GetPhase2CacheStatusRsp &status) {
  if (metadataRecords != 0 || status.evicting != 0 || status.eventBacklog != 0 || status.deadLetters != 0 ||
      status.disks.empty()) {
    return false;
  }
  return std::all_of(status.disks.begin(), status.disks.end(), [&status](const auto &disk) {
    return disk.role == storage::StorageRole::CACHE_ONLY && disk.physicalDiskId.valid().hasValue() &&
           disk.activeGenerations == 0 && disk.permitStoreHealthy && disk.eventJournalWritable &&
           std::abs(disk.enforcedHighWatermark - status.capacityHighWatermark) <=
               std::numeric_limits<double>::epsilon() &&
           !disk.admissionPaused;
  });
}

bool cachePhase2RoutingHealthy(const flat::RoutingInfo &routing, const cache_manager::GetPhase2CacheStatusRsp &status) {
  std::set<storage::PhysicalDiskId> observedDisks;
  for (const auto &disk : status.disks) observedDisks.insert(disk.physicalDiskId);
  std::set<storage::PhysicalDiskId> routedCacheDisks;
  bool foundCacheTable = false;
  for (const auto &[_, versions] : routing.chainTables) {
    if (versions.empty()) continue;
    const auto &table = versions.rbegin()->second;
    if (!table.isCacheData()) continue;
    foundCacheTable = true;
    for (auto chainId : table.chains) {
      auto chain = routing.getChain(chainId);
      if (chain == nullptr || chain->targets.empty()) return false;
      for (const auto &chainTarget : chain->targets) {
        if (chainTarget.publicState != flat::PublicTargetState::SERVING) return false;
        auto target = routing.getTarget(chainTarget.targetId);
        if (target == nullptr || target->storageRole != storage::StorageRole::CACHE_ONLY ||
            !target->physicalDiskId.valid().hasValue() || !observedDisks.contains(target->physicalDiskId)) {
          return false;
        }
        routedCacheDisks.insert(target->physicalDiskId);
      }
    }
  }
  if (!foundCacheTable || routedCacheDisks.empty()) return false;
  for (const auto &[_, target] : routing.targets) {
    if (target.physicalDiskId.valid().hasValue() && routedCacheDisks.contains(target.physicalDiskId) &&
        target.storageRole != storage::StorageRole::CACHE_ONLY) {
      return false;
    }
  }
  return routedCacheDisks == observedDisks;
}

namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-phase2-rollout");
  parser.add_argument("action");
  parser.add_argument("--max-blocks").default_value(uint32_t{1000}).scan<'u', uint32_t>();
  return parser;
}

kv::FDBRetryStrategy retryStrategy() { return kv::FDBRetryStrategy({1_s, 10, true}); }

CoTryTask<std::vector<meta::server::CacheBlockRecord>> loadRecords(AdminEnv &env) {
  auto handler = [](kv::IReadOnlyTransaction &transaction) {
    return meta::server::CacheBlockStore::snapshotListAll(transaction);
  };
  co_return co_await kv::WithTransaction(retryStrategy())
      .run(env.kvEngineGetter()->createReadonlyTransaction(), std::move(handler));
}

CoTryTask<cache_manager::GetPhase2CacheStatusRsp> loadPhase2Status(AdminEnv &env) {
  cache_manager::GetPhase2CacheStatusReq request;
  request.user = env.userInfo;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  co_return co_await env.cacheManagerStubGetter()->getPhase2CacheStatus(request);
}

std::vector<flat::TagPair> rolloutTags(flat::CachePhase2RolloutState state, bool inventoryComplete) {
  auto stateName = state == flat::CachePhase2RolloutState::ENABLED    ? "enabled"
                   : state == flat::CachePhase2RolloutState::DRAINING ? "draining"
                                                                      : "disabled";
  return {{"state", stateName},
          {"inventory_complete", inventoryComplete ? "true" : "false"},
          {"manager_protocol_version", std::to_string(cache::kCacheProtocolVersion)},
          {"client_protocol_version", std::to_string(cache::kCacheProtocolVersion)}};
}

CoTryTask<void> setRollout(AdminEnv &env, flat::CachePhase2RolloutState state, bool inventoryComplete) {
  auto result = co_await env.mgmtdClientGetter()->setUniversalTags(env.userInfo,
                                                                   std::string(flat::kCachePhase2RolloutTagId),
                                                                   rolloutTags(state, inventoryComplete),
                                                                   flat::SetTagMode::REPLACE);
  CO_RETURN_ON_ERROR(result);
  CO_RETURN_ON_ERROR(co_await env.mgmtdClientGetter()->refreshRoutingInfo(true));
  co_return Void{};
}

CoTryTask<uint64_t> drainReady(AdminEnv &env,
                               const std::vector<meta::server::CacheBlockRecord> &records,
                               uint32_t maxBlocks) {
  uint64_t cleaned = 0;
  for (const auto &record : records) {
    if (cleaned == maxBlocks) break;
    if (record.state != cache::CacheBlockState::READY || !record.ready) continue;
    cache_manager::AdminCleanupCacheBlocksReq request;
    request.user = env.userInfo;
    request.inode = meta::InodeId{record.key.inode};
    request.beginBlock = record.key.block;
    request.blockCount = 1;
    request.expectedReady = record.ready;
    request.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto result = co_await env.cacheManagerStubGetter()->adminCleanupCacheBlocks(request);
    CO_RETURN_ON_ERROR(result);
    if (result->results.size() != 1 || result->results.front().hasError()) {
      co_return makeError(CacheCode::kInvalidResponse, "cache drain returned an invalid cleanup result");
    }
    if (result->results.front()->status == cache_manager::CleanupBlockStatus::CLEANED) ++cleaned;
  }
  co_return cleaned;
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  auto action = parser.get<std::string>("action");
  ENSURE_USAGE(action == "status" || action == "drain" || action == "enable" || action == "disable",
               fmt::format("invalid action: {}", action));
  CO_RETURN_ON_ERROR(co_await env.mgmtdClientGetter()->refreshRoutingInfo(true));
  uint64_t cleaned = 0;
  if (action == "drain") {
    CO_RETURN_ON_ERROR(co_await setRollout(env, flat::CachePhase2RolloutState::DRAINING, false));
    auto records = co_await loadRecords(env);
    CO_RETURN_ON_ERROR(records);
    auto drained = co_await drainReady(env, *records, parser.get<uint32_t>("--max-blocks"));
    CO_RETURN_ON_ERROR(drained);
    cleaned = *drained;
  }

  auto records = co_await loadRecords(env);
  CO_RETURN_ON_ERROR(records);
  auto status = co_await loadPhase2Status(env);
  CO_RETURN_ON_ERROR(status);
  auto inventoryReady = cachePhase2InventoryHealthy(records->size(), *status);
  auto routing = env.mgmtdClientGetter()->getRoutingInfo();
  auto routingReady = routing && routing->raw() && cachePhase2RoutingHealthy(*routing->raw(), *status);
  auto healthy = inventoryReady && routingReady;
  if (action == "enable") {
    if (!healthy) co_return makeError(CacheCode::kUnavailable, "cache phase two inventory is not empty and healthy");
    CO_RETURN_ON_ERROR(co_await setRollout(env, flat::CachePhase2RolloutState::ENABLED, true));
  } else if (action == "disable") {
    if (!healthy) co_return makeError(CacheCode::kUnavailable, "cache phase two rollback inventory is not drained");
    CO_RETURN_ON_ERROR(co_await setRollout(env, flat::CachePhase2RolloutState::DISABLED, true));
  }

  auto storedTags = co_await env.mgmtdClientGetter()->getUniversalTags(std::string(flat::kCachePhase2RolloutTagId));
  CO_RETURN_ON_ERROR(storedTags);
  mgmtd::CachePhase2RolloutRequest rollout;
  if (!storedTags->empty()) {
    auto parsed = mgmtd::parseCachePhase2Rollout(*storedTags);
    CO_RETURN_ON_ERROR(parsed);
    rollout = *parsed;
  }

  Dispatcher::OutputTable table{{"Field", "Value"},
                                {"action", action},
                                {"cluster_state", std::string(magic_enum::enum_name(rollout.state))},
                                {"cleaned", std::to_string(cleaned)},
                                {"metadata_records", std::to_string(records->size())},
                                {"evicting", std::to_string(status->evicting)},
                                {"event_backlog", std::to_string(status->eventBacklog)},
                                {"dead_letters", std::to_string(status->deadLetters)},
                                {"routing_complete", routingReady ? "true" : "false"},
                                {"inventory_complete", healthy ? "true" : "false"}};
  for (const auto &disk : status->disks) {
    auto prefix = fmt::format("disk.{}", disk.physicalDiskId.uuid.toHexString());
    table.push_back({prefix + ".active_generations", std::to_string(disk.activeGenerations)});
    table.push_back({prefix + ".permit_store_healthy", disk.permitStoreHealthy ? "true" : "false"});
    table.push_back({prefix + ".event_journal_writable", disk.eventJournalWritable ? "true" : "false"});
    table.push_back({prefix + ".enforced_high_watermark", std::to_string(disk.enforcedHighWatermark)});
  }
  co_return table;
}

}  // namespace

CoTryTask<void> registerCachePhase2RolloutHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
