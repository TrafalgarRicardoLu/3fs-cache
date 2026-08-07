#include "CachePhase4Rollout.h"

#include <folly/experimental/coro/Sleep.h>

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/MagicEnum.hpp"
#include "mgmtd/service/CachePhase4Rollout.h"

namespace hf3fs::client::cli {

bool cachePhase4EnableHealthy(const cache_manager::GetCacheStatusRsp &status) {
  return status.phase4Enabled && status.recoveryHealthy && status.reconcileDryRun &&
         status.reconcile.state == cache::ReconcileRunState::HEALTHY && status.reconcile.conflicts == 0 &&
         status.reconcile.retryable == 0;
}

bool cachePhase4DrainHealthy(const cache_manager::GetCacheStatusRsp &status) {
  return status.recoveryHealthy && status.nonterminalRecoveryWork == 0 &&
         status.reconcile.state == cache::ReconcileRunState::HEALTHY && status.reconcile.conflicts == 0 &&
         status.reconcile.retryable == 0;
}

bool cachePhase4DrainTimedOut(uint64_t elapsedMs, uint64_t timeoutMs) { return elapsedMs >= timeoutMs; }

namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-phase4-rollout");
  parser.add_argument("action");
  parser.add_argument("--timeout-ms").default_value(uint64_t{30000}).scan<'u', uint64_t>();
  parser.add_argument("--poll-ms").default_value(uint64_t{200}).scan<'u', uint64_t>();
  return parser;
}

std::vector<flat::TagPair> rolloutTags(flat::CachePhase4RolloutState state,
                                       bool drainComplete,
                                       bool phase2Enabled,
                                       bool phase3Enabled,
                                       const cache_manager::GetCacheStatusRsp &status) {
  auto stateName = state == flat::CachePhase4RolloutState::ENABLED    ? "enabled"
                   : state == flat::CachePhase4RolloutState::DRAINING ? "draining"
                                                                      : "disabled";
  auto reconcileHealthy = status.reconcile.state == cache::ReconcileRunState::HEALTHY;
  return {{"state", stateName},
          {"drain_complete", drainComplete ? "true" : "false"},
          {"phase2_enabled", phase2Enabled ? "true" : "false"},
          {"phase3_enabled", phase3Enabled ? "true" : "false"},
          {"recovery_healthy", status.recoveryHealthy ? "true" : "false"},
          {"reconcile_healthy", reconcileHealthy ? "true" : "false"},
          {"dry_run_complete", status.reconcileDryRun ? "true" : "false"},
          {"newer_conflict_free", status.reconcile.conflicts == 0 ? "true" : "false"},
          {"manager_protocol_version", std::to_string(cache::kCachePhase4ProtocolVersion)},
          {"meta_protocol_version", std::to_string(cache::kCachePhase4ProtocolVersion)},
          {"storage_protocol_version", std::to_string(cache::kCachePhase4ProtocolVersion)},
          {"cli_protocol_version", std::to_string(cache::kCachePhase4ProtocolVersion)}};
}

CoTryTask<void> setRollout(AdminEnv &env,
                           flat::CachePhase4RolloutState state,
                           bool drainComplete,
                           bool phase2Enabled,
                           bool phase3Enabled,
                           const cache_manager::GetCacheStatusRsp &status) {
  auto result = co_await env.mgmtdClientGetter()->setUniversalTags(
      env.userInfo,
      std::string(flat::kCachePhase4RolloutTagId),
      rolloutTags(state, drainComplete, phase2Enabled, phase3Enabled, status),
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

CoTryTask<cache_manager::GetCacheStatusRsp> waitForDrain(AdminEnv &env, uint64_t timeoutMs, uint64_t pollMs) {
  auto started = SteadyClock::now();
  while (true) {
    auto status = co_await loadStatus(env);
    CO_RETURN_ON_ERROR(status);
    if (cachePhase4DrainHealthy(*status)) co_return *status;
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(SteadyClock::now() - started).count();
    if (cachePhase4DrainTimedOut(static_cast<uint64_t>(std::max<int64_t>(0, elapsed)), timeoutMs)) {
      co_return makeError(CacheCode::kUnavailable, "cache phase four recovery drain timed out");
    }
    co_await folly::coro::sleep(std::chrono::milliseconds(std::max<uint64_t>(1, pollMs)));
  }
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
  auto phase3Enabled = routing->raw()->cachePhase3State == flat::CachePhase3RolloutState::ENABLED;
  auto status = co_await loadStatus(env);
  CO_RETURN_ON_ERROR(status);

  if (action == "drain") {
    CO_RETURN_ON_ERROR(co_await setRollout(env,
                                           flat::CachePhase4RolloutState::DRAINING,
                                           false,
                                           phase2Enabled,
                                           phase3Enabled,
                                           *status));
    status = co_await waitForDrain(env, parser.get<uint64_t>("--timeout-ms"), parser.get<uint64_t>("--poll-ms"));
    CO_RETURN_ON_ERROR(status);
  } else if (action == "enable") {
    if (!phase2Enabled || !phase3Enabled || !cachePhase4EnableHealthy(*status)) {
      co_return makeError(CacheCode::kUnavailable, "cache phase four enable prerequisites are not satisfied");
    }
    CO_RETURN_ON_ERROR(
        co_await setRollout(env, flat::CachePhase4RolloutState::ENABLED, false, phase2Enabled, phase3Enabled, *status));
  } else if (action == "disable") {
    if (!cachePhase4DrainHealthy(*status)) {
      co_return makeError(CacheCode::kUnavailable, "cache phase four recovery work is not drained");
    }
    CO_RETURN_ON_ERROR(
        co_await setRollout(env, flat::CachePhase4RolloutState::DISABLED, true, phase2Enabled, phase3Enabled, *status));
  }

  auto stored = co_await env.mgmtdClientGetter()->getUniversalTags(std::string(flat::kCachePhase4RolloutTagId));
  CO_RETURN_ON_ERROR(stored);
  mgmtd::CachePhase4RolloutRequest rollout;
  if (!stored->empty()) {
    auto parsed = mgmtd::parseCachePhase4Rollout(*stored);
    CO_RETURN_ON_ERROR(parsed);
    rollout = *parsed;
  }
  co_return Dispatcher::OutputTable{{"Field", "Value"},
                                    {"action", action},
                                    {"cluster_state", std::string(magic_enum::enum_name(rollout.state))},
                                    {"recovery_healthy", status->recoveryHealthy ? "true" : "false"},
                                    {"reconcile_state", std::string(magic_enum::enum_name(status->reconcile.state))},
                                    {"reconcile_dry_run", status->reconcileDryRun ? "true" : "false"},
                                    {"newer_conflicts", std::to_string(status->reconcile.conflicts)},
                                    {"nonterminal_recovery_work", std::to_string(status->nonterminalRecoveryWork)},
                                    {"drain_complete", cachePhase4DrainHealthy(*status) ? "true" : "false"}};
}

}  // namespace

CoTryTask<void> registerCachePhase4RolloutHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
