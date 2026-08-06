#pragma once

#include <charconv>
#include <fmt/format.h>
#include <set>

#include "fbs/cache/Common.h"
#include "fbs/mgmtd/RoutingInfo.h"

namespace hf3fs::mgmtd {

struct CachePhase3RolloutRequest {
  flat::CachePhase3RolloutState state{flat::CachePhase3RolloutState::DISABLED};
  bool inventoryComplete{false};
  bool phase2Enabled{false};
  uint32_t managerProtocolVersion{0};
  uint32_t metaProtocolVersion{0};
  uint32_t cliProtocolVersion{0};
};

inline Result<CachePhase3RolloutRequest> parseCachePhase3Rollout(const std::vector<flat::TagPair> &tags) {
  CachePhase3RolloutRequest result;
  bool hasState = false;
  for (const auto &tag : tags) {
    if (tag.key == "state") {
      hasState = true;
      if (tag.value == "disabled")
        result.state = flat::CachePhase3RolloutState::DISABLED;
      else if (tag.value == "draining")
        result.state = flat::CachePhase3RolloutState::DRAINING;
      else if (tag.value == "enabled")
        result.state = flat::CachePhase3RolloutState::ENABLED;
      else
        return makeError(StatusCode::kInvalidArg, "unknown cache phase three rollout state");
    } else if (tag.key == "inventory_complete" || tag.key == "phase2_enabled") {
      if (tag.value != "true" && tag.value != "false")
        return makeError(StatusCode::kInvalidArg, "invalid cache phase three rollout boolean");
      auto value = tag.value == "true";
      if (tag.key == "inventory_complete")
        result.inventoryComplete = value;
      else
        result.phase2Enabled = value;
    } else if (tag.key == "manager_protocol_version" || tag.key == "meta_protocol_version" ||
               tag.key == "cli_protocol_version") {
      uint32_t value = 0;
      auto parsed = std::from_chars(tag.value.data(), tag.value.data() + tag.value.size(), value);
      if (parsed.ec != std::errc{} || parsed.ptr != tag.value.data() + tag.value.size())
        return makeError(StatusCode::kInvalidArg, "invalid cache phase three protocol capability");
      if (tag.key == "manager_protocol_version")
        result.managerProtocolVersion = value;
      else if (tag.key == "meta_protocol_version")
        result.metaProtocolVersion = value;
      else
        result.cliProtocolVersion = value;
    } else {
      return makeError(StatusCode::kInvalidArg, "unknown cache phase three rollout field");
    }
  }
  if (!hasState) return makeError(StatusCode::kInvalidArg, "cache phase three rollout state is missing");
  return result;
}

inline Result<Void> validateCachePhase3Transition(flat::CachePhase3RolloutState current,
                                                  const CachePhase3RolloutRequest &requested,
                                                  const std::vector<flat::NodeInfo> &nodes) {
  if (current == flat::CachePhase3RolloutState::ENABLED && requested.state == flat::CachePhase3RolloutState::DISABLED) {
    return makeError(StatusCode::kInvalidArg, "cache phase three must enter draining before disable");
  }
  if (requested.state == flat::CachePhase3RolloutState::ENABLED) {
    if ((current != flat::CachePhase3RolloutState::DRAINING && current != flat::CachePhase3RolloutState::ENABLED) ||
        !requested.inventoryComplete || !requested.phase2Enabled) {
      return makeError(StatusCode::kInvalidArg,
                       "cache phase three enable requires phase two and a completed orchestration inventory");
    }
    if (requested.managerProtocolVersion < cache::kCachePhase3ProtocolVersion ||
        requested.metaProtocolVersion < cache::kCachePhase3ProtocolVersion ||
        requested.cliProtocolVersion < cache::kCachePhase3ProtocolVersion) {
      return makeError(CacheCode::kUpgradeRequired, "cache phase three manager, Meta or CLI capability is missing");
    }
    bool metaCapable = false;
    for (const auto &node : nodes) {
      if (node.status != flat::NodeStatus::HEARTBEAT_CONNECTED && node.status != flat::NodeStatus::PRIMARY_MGMTD)
        continue;
      if (node.type != flat::NodeType::META) continue;
      if (node.cacheSchemaVersion < cache::kCacheSchemaVersion ||
          node.cacheProtocolVersion < cache::kCachePhase3ProtocolVersion) {
        return makeError(CacheCode::kUpgradeRequired,
                         fmt::format("Meta node {} is missing cache phase three capability", node.app.nodeId));
      }
      metaCapable = true;
    }
    if (!metaCapable) return makeError(CacheCode::kUpgradeRequired, "Meta phase three capability inventory is empty");
  }
  if (current == flat::CachePhase3RolloutState::DRAINING &&
      requested.state == flat::CachePhase3RolloutState::DISABLED && !requested.inventoryComplete) {
    return makeError(StatusCode::kInvalidArg, "cache phase three disable requires an empty orchestration inventory");
  }
  return Void{};
}

}  // namespace hf3fs::mgmtd
