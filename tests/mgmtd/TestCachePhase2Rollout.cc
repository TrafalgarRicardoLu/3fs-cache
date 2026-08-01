#include <gtest/gtest.h>

#include "mgmtd/service/CachePhase2Rollout.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::mgmtd {
namespace {

flat::NodeInfo capableNode(flat::NodeType type, flat::NodeId id) {
  flat::NodeInfo node;
  node.type = type;
  node.app.nodeId = id;
  node.status = flat::NodeStatus::HEARTBEAT_CONNECTED;
  node.cacheSchemaVersion = cache::kCacheSchemaVersion;
  node.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return node;
}

TEST(CachePhase2Rollout, RequiresDrainInventoryAndEveryCapability) {
  CachePhase2RolloutRequest enable{flat::CachePhase2RolloutState::ENABLED,
                                   true,
                                   cache::kCacheProtocolVersion,
                                   cache::kCacheProtocolVersion};
  std::vector nodes{capableNode(flat::NodeType::META, flat::NodeId{1}),
                    capableNode(flat::NodeType::STORAGE, flat::NodeId{2}),
                    capableNode(flat::NodeType::MGMTD, flat::NodeId{3})};

  ASSERT_ERROR(validateCachePhase2Transition(flat::CachePhase2RolloutState::DISABLED, enable, nodes),
               StatusCode::kInvalidArg);
  ASSERT_OK(validateCachePhase2Transition(flat::CachePhase2RolloutState::DRAINING, enable, nodes));
  ASSERT_OK(validateCachePhase2Transition(flat::CachePhase2RolloutState::ENABLED, enable, nodes));

  nodes.back().cacheProtocolVersion = cache::kCacheProtocolVersion - 1;
  ASSERT_ERROR(validateCachePhase2Transition(flat::CachePhase2RolloutState::DRAINING, enable, nodes),
               CacheCode::kUpgradeRequired);
}

TEST(CachePhase2Rollout, RollbackCannotSkipDrainOrEmptyInventory) {
  CachePhase2RolloutRequest disable{flat::CachePhase2RolloutState::DISABLED, true};
  ASSERT_ERROR(validateCachePhase2Transition(flat::CachePhase2RolloutState::ENABLED, disable, {}),
               StatusCode::kInvalidArg);
  disable.inventoryComplete = false;
  ASSERT_ERROR(validateCachePhase2Transition(flat::CachePhase2RolloutState::DRAINING, disable, {}),
               StatusCode::kInvalidArg);
  disable.inventoryComplete = true;
  ASSERT_OK(validateCachePhase2Transition(flat::CachePhase2RolloutState::DRAINING, disable, {}));
}

TEST(CachePhase2Rollout, ParsesOnlyKnownFields) {
  auto parsed = parseCachePhase2Rollout({{"state", "draining"},
                                         {"inventory_complete", "false"},
                                         {"manager_protocol_version", "2"},
                                         {"client_protocol_version", "2"}});
  ASSERT_OK(parsed);
  EXPECT_EQ(parsed->state, flat::CachePhase2RolloutState::DRAINING);
  ASSERT_ERROR(parseCachePhase2Rollout({{"state", "enabled"}, {"token", "secret"}}), StatusCode::kInvalidArg);
}

}  // namespace
}  // namespace hf3fs::mgmtd
