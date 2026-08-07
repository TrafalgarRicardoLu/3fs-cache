#pragma once

#include "RoutingInfo.h"
#include "common/utils/MagicEnum.hpp"
#include "common/utils/Result.h"

namespace hf3fs::mgmtd {

inline const flat::TargetInfo *findStorageTarget(const RoutingInfo &routingInfo, flat::TargetId targetId) {
  if (auto it = routingInfo.getTargets().find(targetId); it != routingInfo.getTargets().end()) {
    return &it->second.base();
  }
  if (auto it = routingInfo.getOrphanTargets().find(targetId); it != routingInfo.getOrphanTargets().end()) {
    return &it->second;
  }
  return nullptr;
}

inline Result<Void> validateTargetStorageRole(const RoutingInfo &routingInfo,
                                              flat::TargetId targetId,
                                              std::optional<storage::StorageRole> expectedRole,
                                              bool requireIdentity) {
  const auto *target = findStorageTarget(routingInfo, targetId);
  if (!target) {
    if (!requireIdentity) return Void{};
    return makeError(CacheCode::kRoleMismatch, fmt::format("target {} has no storage heartbeat identity", targetId));
  }

  const auto hasDiskId = target->physicalDiskId != storage::PhysicalDiskId{};
  const auto hasRole = target->storageRole != storage::StorageRole::INVALID;
  if (!hasDiskId && !hasRole && !requireIdentity) return Void{};
  if (!hasDiskId || !hasRole ||
      (target->storageRole != storage::StorageRole::USER_DATA &&
       target->storageRole != storage::StorageRole::CACHE_ONLY &&
       target->storageRole != storage::StorageRole::WRITE_STAGING)) {
    return makeError(CacheCode::kRoleMismatch, fmt::format("target {} has incomplete storage identity", targetId));
  }
  if (expectedRole && target->storageRole != *expectedRole) {
    return makeError(CacheCode::kRoleMismatch,
                     fmt::format("target {} storage role {} does not match expected {}",
                                 targetId,
                                 magic_enum::enum_name(target->storageRole),
                                 magic_enum::enum_name(*expectedRole)));
  }

  auto checkSameDisk = [&](const flat::TargetInfo &other) -> Result<Void> {
    if (other.targetId != targetId && other.physicalDiskId == target->physicalDiskId &&
        other.storageRole != storage::StorageRole::INVALID && other.storageRole != target->storageRole) {
      return makeError(CacheCode::kRoleMismatch,
                       fmt::format("physical disk {} is shared by targets {} and {} with different roles",
                                   target->physicalDiskId.uuid,
                                   targetId,
                                   other.targetId));
    }
    return Void{};
  };
  for (const auto &[_, other] : routingInfo.getTargets()) RETURN_ON_ERROR(checkSameDisk(other.base()));
  for (const auto &[_, other] : routingInfo.getOrphanTargets()) RETURN_ON_ERROR(checkSameDisk(other));
  return Void{};
}

inline Result<Void> validateChainStorageRole(const RoutingInfo &routingInfo,
                                             flat::ChainId chainId,
                                             storage::StorageRole expectedRole,
                                             bool requireIdentity) {
  auto chain = routingInfo.chains.find(chainId);
  if (chain == routingInfo.chains.end()) {
    return makeError(MgmtdCode::kChainNotFound, fmt::format("chain {} not found", chainId));
  }
  for (const auto &target : chain->second.targets) {
    RETURN_ON_ERROR(validateTargetStorageRole(routingInfo, target.targetId, expectedRole, requireIdentity));
  }
  return Void{};
}

inline Result<Void> validateChainTableStorageRoles(const RoutingInfo &routingInfo,
                                                   const flat::ChainTable &chainTable,
                                                   bool requireIdentity) {
  auto expectedRole = chainTable.isCacheData()      ? storage::StorageRole::CACHE_ONLY
                      : chainTable.isWriteStaging() ? storage::StorageRole::WRITE_STAGING
                                                    : storage::StorageRole::USER_DATA;
  for (auto chainId : chainTable.chains) {
    RETURN_ON_ERROR(validateChainStorageRole(routingInfo, chainId, expectedRole, requireIdentity));
  }
  return Void{};
}

inline Result<std::optional<storage::StorageRole>> storageRoleForChain(const RoutingInfo &routingInfo,
                                                                       flat::ChainId chainId) {
  std::optional<storage::StorageRole> result;
  for (const auto &[_, versions] : routingInfo.chainTables) {
    if (versions.empty()) continue;
    const auto &table = versions.rbegin()->second;
    if (std::find(table.chains.begin(), table.chains.end(), chainId) == table.chains.end()) continue;
    auto role = table.isCacheData()      ? storage::StorageRole::CACHE_ONLY
                : table.isWriteStaging() ? storage::StorageRole::WRITE_STAGING
                                         : storage::StorageRole::USER_DATA;
    if (result && *result != role) {
      return makeError(CacheCode::kRoleMismatch, fmt::format("chain {} belongs to conflicting table roles", chainId));
    }
    result = role;
  }
  return result;
}

}  // namespace hf3fs::mgmtd
