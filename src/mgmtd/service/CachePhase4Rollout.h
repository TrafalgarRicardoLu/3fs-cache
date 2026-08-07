#pragma once

#include <charconv>
#include <fmt/format.h>

#include "fbs/cache/Common.h"
#include "fbs/mgmtd/RoutingInfo.h"

namespace hf3fs::mgmtd {

struct CachePhase4RolloutRequest {
  flat::CachePhase4RolloutState state{flat::CachePhase4RolloutState::DISABLED};
  bool drainComplete{false};
  bool phase2Enabled{false};
  bool phase3Enabled{false};
  bool recoveryHealthy{false};
  bool reconcileHealthy{false};
  bool dryRunComplete{false};
  bool newerConflictFree{false};
  uint32_t managerProtocolVersion{0};
  uint32_t metaProtocolVersion{0};
  uint32_t storageProtocolVersion{0};
  uint32_t cliProtocolVersion{0};
};

inline Result<CachePhase4RolloutRequest> parseCachePhase4Rollout(const std::vector<flat::TagPair> &tags) {
  CachePhase4RolloutRequest result;
  bool hasState = false;
  for (const auto &tag : tags) {
    if (tag.key == "state") {
      hasState = true;
      if (tag.value == "disabled")
        result.state = flat::CachePhase4RolloutState::DISABLED;
      else if (tag.value == "draining")
        result.state = flat::CachePhase4RolloutState::DRAINING;
      else if (tag.value == "enabled")
        result.state = flat::CachePhase4RolloutState::ENABLED;
      else
        return makeError(StatusCode::kInvalidArg, "unknown cache phase four rollout state");
    } else if (tag.key == "drain_complete" || tag.key == "phase2_enabled" || tag.key == "phase3_enabled" ||
               tag.key == "recovery_healthy" || tag.key == "reconcile_healthy" || tag.key == "dry_run_complete" ||
               tag.key == "newer_conflict_free") {
      if (tag.value != "true" && tag.value != "false")
        return makeError(StatusCode::kInvalidArg, "invalid cache phase four rollout boolean");
      auto value = tag.value == "true";
      if (tag.key == "drain_complete") result.drainComplete = value;
      if (tag.key == "phase2_enabled") result.phase2Enabled = value;
      if (tag.key == "phase3_enabled") result.phase3Enabled = value;
      if (tag.key == "recovery_healthy") result.recoveryHealthy = value;
      if (tag.key == "reconcile_healthy") result.reconcileHealthy = value;
      if (tag.key == "dry_run_complete") result.dryRunComplete = value;
      if (tag.key == "newer_conflict_free") result.newerConflictFree = value;
    } else if (tag.key == "manager_protocol_version" || tag.key == "meta_protocol_version" ||
               tag.key == "storage_protocol_version" || tag.key == "cli_protocol_version") {
      uint32_t value = 0;
      auto parsed = std::from_chars(tag.value.data(), tag.value.data() + tag.value.size(), value);
      if (parsed.ec != std::errc{} || parsed.ptr != tag.value.data() + tag.value.size())
        return makeError(StatusCode::kInvalidArg, "invalid cache phase four protocol capability");
      if (tag.key == "manager_protocol_version") result.managerProtocolVersion = value;
      if (tag.key == "meta_protocol_version") result.metaProtocolVersion = value;
      if (tag.key == "storage_protocol_version") result.storageProtocolVersion = value;
      if (tag.key == "cli_protocol_version") result.cliProtocolVersion = value;
    } else {
      return makeError(StatusCode::kInvalidArg, "unknown cache phase four rollout field");
    }
  }
  if (!hasState) return makeError(StatusCode::kInvalidArg, "cache phase four rollout state is missing");
  return result;
}

inline Result<Void> validateCachePhase4Transition(flat::CachePhase4RolloutState current,
                                                  const CachePhase4RolloutRequest &requested,
                                                  const std::vector<flat::NodeInfo> &nodes) {
  if (current == flat::CachePhase4RolloutState::ENABLED && requested.state == flat::CachePhase4RolloutState::DISABLED) {
    return makeError(StatusCode::kInvalidArg, "cache phase four must enter draining before disable");
  }
  if (requested.state == flat::CachePhase4RolloutState::ENABLED) {
    if ((current != flat::CachePhase4RolloutState::DRAINING && current != flat::CachePhase4RolloutState::ENABLED) ||
        !requested.phase2Enabled || !requested.phase3Enabled || !requested.recoveryHealthy ||
        !requested.reconcileHealthy || !requested.dryRunComplete || !requested.newerConflictFree) {
      return makeError(StatusCode::kInvalidArg,
                       "cache phase four enable requires healthy recovery and a conflict-free dry-run reconcile");
    }
    if (requested.managerProtocolVersion < cache::kCachePhase4ProtocolVersion ||
        requested.metaProtocolVersion < cache::kCachePhase4ProtocolVersion ||
        requested.storageProtocolVersion < cache::kCachePhase4ProtocolVersion ||
        requested.cliProtocolVersion < cache::kCachePhase4ProtocolVersion) {
      return makeError(CacheCode::kUpgradeRequired, "cache phase four service capability is missing");
    }
    bool metaCapable = false;
    bool storageCapable = false;
    for (const auto &node : nodes) {
      if (node.status != flat::NodeStatus::HEARTBEAT_CONNECTED && node.status != flat::NodeStatus::PRIMARY_MGMTD)
        continue;
      if (node.type != flat::NodeType::META && node.type != flat::NodeType::STORAGE) continue;
      if (node.cacheSchemaVersion < cache::kCachePhase4SchemaVersion ||
          node.cacheProtocolVersion < cache::kCachePhase4ProtocolVersion) {
        return makeError(CacheCode::kUpgradeRequired,
                         fmt::format("node {} is missing cache phase four capability", node.app.nodeId));
      }
      metaCapable |= node.type == flat::NodeType::META;
      storageCapable |= node.type == flat::NodeType::STORAGE;
    }
    if (!metaCapable || !storageCapable) {
      return makeError(CacheCode::kUpgradeRequired, "cache phase four node capability inventory is incomplete");
    }
  }
  if (current == flat::CachePhase4RolloutState::DRAINING &&
      requested.state == flat::CachePhase4RolloutState::DISABLED && !requested.drainComplete) {
    return makeError(StatusCode::kInvalidArg, "cache phase four disable requires completed recovery drain");
  }
  return Void{};
}

}  // namespace hf3fs::mgmtd
