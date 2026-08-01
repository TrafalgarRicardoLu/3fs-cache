#pragma once

#include <charconv>
#include <fmt/format.h>
#include <set>

#include "fbs/cache/Common.h"
#include "fbs/mgmtd/RoutingInfo.h"

namespace hf3fs::mgmtd {

struct CachePhase2RolloutRequest {
  flat::CachePhase2RolloutState state{flat::CachePhase2RolloutState::DISABLED};
  bool inventoryComplete{false};
  uint32_t managerProtocolVersion{0};
  uint32_t clientProtocolVersion{0};
};

inline Result<CachePhase2RolloutRequest> parseCachePhase2Rollout(const std::vector<flat::TagPair> &tags) {
  CachePhase2RolloutRequest result;
  bool hasState = false;
  for (const auto &tag : tags) {
    if (tag.key == "state") {
      hasState = true;
      if (tag.value == "disabled")
        result.state = flat::CachePhase2RolloutState::DISABLED;
      else if (tag.value == "draining")
        result.state = flat::CachePhase2RolloutState::DRAINING;
      else if (tag.value == "enabled")
        result.state = flat::CachePhase2RolloutState::ENABLED;
      else
        return makeError(StatusCode::kInvalidArg, "unknown cache phase two rollout state");
    } else if (tag.key == "inventory_complete") {
      if (tag.value != "true" && tag.value != "false")
        return makeError(StatusCode::kInvalidArg, "invalid cache phase two inventory result");
      result.inventoryComplete = tag.value == "true";
    } else if (tag.key == "manager_protocol_version" || tag.key == "client_protocol_version") {
      uint32_t value = 0;
      auto parsed = std::from_chars(tag.value.data(), tag.value.data() + tag.value.size(), value);
      if (parsed.ec != std::errc{} || parsed.ptr != tag.value.data() + tag.value.size())
        return makeError(StatusCode::kInvalidArg, "invalid cache phase two protocol capability");
      if (tag.key == "manager_protocol_version")
        result.managerProtocolVersion = value;
      else
        result.clientProtocolVersion = value;
    } else {
      return makeError(StatusCode::kInvalidArg, "unknown cache phase two rollout field");
    }
  }
  if (!hasState) return makeError(StatusCode::kInvalidArg, "cache phase two rollout state is missing");
  return result;
}

inline Result<Void> validateCachePhase2Transition(flat::CachePhase2RolloutState current,
                                                  const CachePhase2RolloutRequest &requested,
                                                  const std::vector<flat::NodeInfo> &nodes) {
  if (current == flat::CachePhase2RolloutState::ENABLED && requested.state == flat::CachePhase2RolloutState::DISABLED) {
    return makeError(StatusCode::kInvalidArg, "cache phase two must enter draining before disable");
  }
  if (requested.state == flat::CachePhase2RolloutState::ENABLED) {
    if ((current != flat::CachePhase2RolloutState::DRAINING && current != flat::CachePhase2RolloutState::ENABLED) ||
        !requested.inventoryComplete) {
      return makeError(StatusCode::kInvalidArg, "cache phase two enable requires a completed drain and inventory");
    }
    if (requested.managerProtocolVersion < cache::kCacheProtocolVersion ||
        requested.clientProtocolVersion < cache::kCacheProtocolVersion) {
      return makeError(CacheCode::kUpgradeRequired, "cache manager or client phase two capability is missing");
    }
    std::set<flat::NodeType> capableTypes;
    for (const auto &node : nodes) {
      if (node.status != flat::NodeStatus::HEARTBEAT_CONNECTED && node.status != flat::NodeStatus::PRIMARY_MGMTD) {
        continue;
      }
      if (node.type != flat::NodeType::META && node.type != flat::NodeType::STORAGE &&
          node.type != flat::NodeType::MGMTD) {
        continue;
      }
      if (node.cacheSchemaVersion < cache::kCacheSchemaVersion ||
          node.cacheProtocolVersion < cache::kCacheProtocolVersion) {
        return makeError(CacheCode::kUpgradeRequired,
                         fmt::format("node {} is missing cache phase two capability", node.app.nodeId));
      }
      capableTypes.insert(node.type);
    }
    if (!capableTypes.contains(flat::NodeType::META) || !capableTypes.contains(flat::NodeType::STORAGE) ||
        !capableTypes.contains(flat::NodeType::MGMTD)) {
      return makeError(CacheCode::kUpgradeRequired, "cache phase two server capability inventory is incomplete");
    }
  }
  if (current == flat::CachePhase2RolloutState::DRAINING &&
      requested.state == flat::CachePhase2RolloutState::DISABLED && !requested.inventoryComplete) {
    return makeError(StatusCode::kInvalidArg, "cache phase two disable requires an empty inventory");
  }
  return Void{};
}

}  // namespace hf3fs::mgmtd
