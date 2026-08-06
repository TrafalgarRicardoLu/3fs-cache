#include <gtest/gtest.h>

#include "mgmtd/service/CachePhase3Rollout.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::mgmtd {
namespace {

flat::NodeInfo capableMeta() {
  flat::NodeInfo node;
  node.type = flat::NodeType::META;
  node.app.nodeId = flat::NodeId{1};
  node.status = flat::NodeStatus::HEARTBEAT_CONNECTED;
  node.cacheSchemaVersion = cache::kCacheSchemaVersion;
  node.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  return node;
}

TEST(CachePhase3Rollout, RequiresPhase2InventoryAndAllCapabilities) {
  CachePhase3RolloutRequest enable{flat::CachePhase3RolloutState::ENABLED,
                                   true,
                                   true,
                                   cache::kCachePhase3ProtocolVersion,
                                   cache::kCachePhase3ProtocolVersion,
                                   cache::kCachePhase3ProtocolVersion};
  std::vector nodes{capableMeta()};
  ASSERT_ERROR(validateCachePhase3Transition(flat::CachePhase3RolloutState::DISABLED, enable, nodes),
               StatusCode::kInvalidArg);
  ASSERT_OK(validateCachePhase3Transition(flat::CachePhase3RolloutState::DRAINING, enable, nodes));
  enable.phase2Enabled = false;
  ASSERT_ERROR(validateCachePhase3Transition(flat::CachePhase3RolloutState::DRAINING, enable, nodes),
               StatusCode::kInvalidArg);
  enable.phase2Enabled = true;
  enable.cliProtocolVersion = cache::kCacheProtocolVersion;
  ASSERT_ERROR(validateCachePhase3Transition(flat::CachePhase3RolloutState::DRAINING, enable, nodes),
               CacheCode::kUpgradeRequired);
}

TEST(CachePhase3Rollout, DisableCannotSkipDrainOrInventory) {
  CachePhase3RolloutRequest disable;
  disable.state = flat::CachePhase3RolloutState::DISABLED;
  disable.inventoryComplete = true;
  ASSERT_ERROR(validateCachePhase3Transition(flat::CachePhase3RolloutState::ENABLED, disable, {}),
               StatusCode::kInvalidArg);
  disable.inventoryComplete = false;
  ASSERT_ERROR(validateCachePhase3Transition(flat::CachePhase3RolloutState::DRAINING, disable, {}),
               StatusCode::kInvalidArg);
  disable.inventoryComplete = true;
  ASSERT_OK(validateCachePhase3Transition(flat::CachePhase3RolloutState::DRAINING, disable, {}));
}

TEST(CachePhase3Rollout, ParserRejectsUnknownAndSecretFields) {
  auto parsed = parseCachePhase3Rollout({{"state", "draining"},
                                         {"inventory_complete", "false"},
                                         {"phase2_enabled", "true"},
                                         {"manager_protocol_version", "3"},
                                         {"meta_protocol_version", "3"},
                                         {"cli_protocol_version", "3"}});
  ASSERT_OK(parsed);
  EXPECT_EQ(parsed->state, flat::CachePhase3RolloutState::DRAINING);
  ASSERT_ERROR(parseCachePhase3Rollout({{"state", "enabled"}, {"token", "secret"}}), StatusCode::kInvalidArg);
}

}  // namespace
}  // namespace hf3fs::mgmtd
