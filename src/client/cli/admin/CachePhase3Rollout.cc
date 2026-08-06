#include "CachePhase3Rollout.h"

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/MagicEnum.hpp"
#include "mgmtd/service/CachePhase3Rollout.h"

namespace hf3fs::client::cli {

bool cachePhase3InventoryHealthy(const cache_manager::GetCacheStatusRsp &status) {
  return status.activeJobs == 0 && status.pinnedBytes == 0 && status.exclusiveQueuedClaims == 0;
}

namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-phase3-rollout");
  parser.add_argument("action");
  return parser;
}

std::vector<flat::TagPair> rolloutTags(flat::CachePhase3RolloutState state,
                                       bool inventoryComplete,
                                       bool phase2Enabled) {
  auto stateName = state == flat::CachePhase3RolloutState::ENABLED    ? "enabled"
                   : state == flat::CachePhase3RolloutState::DRAINING ? "draining"
                                                                      : "disabled";
  return {{"state", stateName},
          {"inventory_complete", inventoryComplete ? "true" : "false"},
          {"phase2_enabled", phase2Enabled ? "true" : "false"},
          {"manager_protocol_version", std::to_string(cache::kCachePhase3ProtocolVersion)},
          {"meta_protocol_version", std::to_string(cache::kCachePhase3ProtocolVersion)},
          {"cli_protocol_version", std::to_string(cache::kCachePhase3ProtocolVersion)}};
}

CoTryTask<void> setRollout(AdminEnv &env,
                           flat::CachePhase3RolloutState state,
                           bool inventoryComplete,
                           bool phase2Enabled) {
  auto result = co_await env.mgmtdClientGetter()->setUniversalTags(env.userInfo,
                                                                   std::string(flat::kCachePhase3RolloutTagId),
                                                                   rolloutTags(state, inventoryComplete, phase2Enabled),
                                                                   flat::SetTagMode::REPLACE);
  CO_RETURN_ON_ERROR(result);
  CO_RETURN_ON_ERROR(co_await env.mgmtdClientGetter()->refreshRoutingInfo(true));
  co_return Void{};
}

CoTryTask<cache_manager::GetCacheStatusRsp> loadStatus(AdminEnv &env) {
  cache_manager::GetCacheStatusReq request;
  request.user = env.userInfo;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  co_return co_await env.cacheManagerStubGetter()->getCacheStatus(request);
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
  auto routing = env.mgmtdClientGetter()->getRoutingInfo();
  if (!routing || !routing->raw()) co_return makeError(CacheCode::kUnavailable, "routing information is unavailable");
  auto phase2Enabled = routing->raw()->cachePhase2State == flat::CachePhase2RolloutState::ENABLED;
  auto status = co_await loadStatus(env);
  CO_RETURN_ON_ERROR(status);
  auto inventoryComplete = cachePhase3InventoryHealthy(*status);

  if (action == "drain") {
    CO_RETURN_ON_ERROR(
        co_await setRollout(env, flat::CachePhase3RolloutState::DRAINING, inventoryComplete, phase2Enabled));
  } else if (action == "enable") {
    if (!phase2Enabled || !inventoryComplete) {
      co_return makeError(CacheCode::kUnavailable, "phase three enable prerequisites are not satisfied");
    }
    CO_RETURN_ON_ERROR(co_await setRollout(env, flat::CachePhase3RolloutState::ENABLED, true, phase2Enabled));
  } else if (action == "disable") {
    if (!inventoryComplete) {
      co_return makeError(CacheCode::kUnavailable, "phase three orchestration inventory is not drained");
    }
    CO_RETURN_ON_ERROR(co_await setRollout(env, flat::CachePhase3RolloutState::DISABLED, true, phase2Enabled));
  }

  auto stored = co_await env.mgmtdClientGetter()->getUniversalTags(std::string(flat::kCachePhase3RolloutTagId));
  CO_RETURN_ON_ERROR(stored);
  mgmtd::CachePhase3RolloutRequest rollout;
  if (!stored->empty()) {
    auto parsed = mgmtd::parseCachePhase3Rollout(*stored);
    CO_RETURN_ON_ERROR(parsed);
    rollout = *parsed;
  }
  co_return Dispatcher::OutputTable{{"Field", "Value"},
                                    {"action", action},
                                    {"cluster_state", std::string(magic_enum::enum_name(rollout.state))},
                                    {"phase2_enabled", phase2Enabled ? "true" : "false"},
                                    {"active_jobs", std::to_string(status->activeJobs)},
                                    {"pinned_bytes", std::to_string(status->pinnedBytes)},
                                    {"exclusive_queued_claims", std::to_string(status->exclusiveQueuedClaims)},
                                    {"inventory_complete", inventoryComplete ? "true" : "false"}};
}

}  // namespace

CoTryTask<void> registerCachePhase3RolloutHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
