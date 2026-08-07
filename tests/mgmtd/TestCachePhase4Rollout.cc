#include <gtest/gtest.h>

#include "mgmtd/service/CachePhase4Rollout.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::mgmtd {
namespace {

flat::NodeInfo capableNode(flat::NodeType type, uint32_t version = cache::kCachePhase4ProtocolVersion) {
  flat::NodeInfo node;
  node.type = type;
  node.app.nodeId = flat::NodeId{type == flat::NodeType::META ? 1u : 2u};
  node.status = flat::NodeStatus::HEARTBEAT_CONNECTED;
  node.cacheSchemaVersion = version;
  node.cacheProtocolVersion = version;
  return node;
}

CachePhase4RolloutRequest enableRequest() {
  CachePhase4RolloutRequest request;
  request.state = flat::CachePhase4RolloutState::ENABLED;
  request.phase2Enabled = true;
  request.phase3Enabled = true;
  request.recoveryHealthy = true;
  request.reconcileHealthy = true;
  request.dryRunComplete = true;
  request.newerConflictFree = true;
  request.managerProtocolVersion = cache::kCachePhase4ProtocolVersion;
  request.metaProtocolVersion = cache::kCachePhase4ProtocolVersion;
  request.storageProtocolVersion = cache::kCachePhase4ProtocolVersion;
  request.cliProtocolVersion = cache::kCachePhase4ProtocolVersion;
  return request;
}

TEST(CachePhase4Rollout, RequiresUpgradeOrderAndHealthyDryRun) {
  auto enable = enableRequest();
  std::vector nodes{capableNode(flat::NodeType::META), capableNode(flat::NodeType::STORAGE)};
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::DISABLED, enable, nodes),
               StatusCode::kInvalidArg);
  ASSERT_OK(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, enable, nodes));
  enable.dryRunComplete = false;
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, enable, nodes),
               StatusCode::kInvalidArg);
  enable.dryRunComplete = true;
  enable.newerConflictFree = false;
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, enable, nodes),
               StatusCode::kInvalidArg);
}

TEST(CachePhase4Rollout, RejectsOldOrMissingNodes) {
  auto enable = enableRequest();
  std::vector oldStorage{capableNode(flat::NodeType::META),
                         capableNode(flat::NodeType::STORAGE, cache::kCachePhase3ProtocolVersion)};
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, enable, oldStorage),
               CacheCode::kUpgradeRequired);
  std::vector metaOnly{capableNode(flat::NodeType::META)};
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, enable, metaOnly),
               CacheCode::kUpgradeRequired);
}

TEST(CachePhase4Rollout, RollbackRequiresDrainAndParserFailsClosed) {
  CachePhase4RolloutRequest disable;
  disable.state = flat::CachePhase4RolloutState::DISABLED;
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::ENABLED, disable, {}),
               StatusCode::kInvalidArg);
  ASSERT_ERROR(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, disable, {}),
               StatusCode::kInvalidArg);
  disable.drainComplete = true;
  ASSERT_OK(validateCachePhase4Transition(flat::CachePhase4RolloutState::DRAINING, disable, {}));

  auto parsed = parseCachePhase4Rollout({{"state", "draining"},
                                         {"drain_complete", "false"},
                                         {"recovery_healthy", "true"},
                                         {"manager_protocol_version", "4"}});
  ASSERT_OK(parsed);
  EXPECT_EQ(parsed->state, flat::CachePhase4RolloutState::DRAINING);
  ASSERT_ERROR(parseCachePhase4Rollout({{"state", "enabled"}, {"credential", "secret"}}), StatusCode::kInvalidArg);
}

}  // namespace
}  // namespace hf3fs::mgmtd
