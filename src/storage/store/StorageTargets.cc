#include "storage/store/StorageTargets.h"

#include <boost/filesystem/operations.hpp>
#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/Collect.h>
#include <folly/experimental/coro/Sleep.h>
#include <folly/experimental/coro/Task.h>
#include <fstream>
#include <limits>
#include <memory>
#include <sys/statvfs.h>
#include <unordered_map>

#include "chunk_engine/src/cxx.rs.h"
#include "common/monitor/Sample.h"
#include "common/utils/CPUExecutorGroup.h"
#include "common/utils/Duration.h"
#include "common/utils/LogCommands.h"
#include "common/utils/MagicEnum.hpp"
#include "common/utils/Result.h"
#include "common/utils/SysResource.h"
#include "storage/service/Components.h"

namespace hf3fs::storage {
namespace {

constexpr std::string_view kCachePermitPrefix = "phase2/cache-permit/";
constexpr std::string_view kCachePermitStoreDirectory = ".cache-space-permits";
constexpr std::string_view kCacheEventStoreDirectory = ".cache-events";

struct CachePermitStoreKey {
  SERDE_STRUCT_FIELD(managerEpoch, Uuid::zero());
  SERDE_STRUCT_FIELD(admissionAttemptId, Uuid::zero());
  SERDE_STRUCT_FIELD(permitGeneration, uint64_t{0});
};

Result<Void> writePhysicalDiskConfig(const Path &diskPath, const PhysicalDiskConfig &config) {
  auto configPath = diskPath / kPhysicalDiskConfigFileName;
  auto tempPath = diskPath / fmt::format("{}.tmp", kPhysicalDiskConfigFileName);
  {
    std::ofstream file(tempPath, std::ios::out | std::ios::trunc);
    if (!file || !(file << serde::toTomlString(config))) {
      return makeError(StorageCode::kStorageInitFailed, fmt::format("write physical disk config {} failed", tempPath));
    }
  }
  boost::system::error_code ec;
  boost::filesystem::rename(tempPath, configPath, ec);
  if (UNLIKELY(ec.failed())) {
    return makeError(StorageCode::kStorageInitFailed,
                     fmt::format("install physical disk config {} failed: {}", configPath, ec.message()));
  }
  return Void{};
}

Result<PhysicalDiskConfig> loadOrCreatePhysicalDiskConfig(const Path &diskPath, StorageRole expectedRole) {
  if (expectedRole != StorageRole::USER_DATA && expectedRole != StorageRole::CACHE_ONLY) {
    return makeError(CacheCode::kRoleMismatch, fmt::format("invalid configured storage role for {}", diskPath));
  }

  auto configPath = diskPath / kPhysicalDiskConfigFileName;
  PhysicalDiskConfig config;
  if (boost::filesystem::exists(configPath)) {
    RETURN_AND_LOG_ON_ERROR(serde::fromTomlFile(config, configPath));
    RETURN_AND_LOG_ON_ERROR(config.physical_disk_id.valid());
    if (config.storage_role != expectedRole) {
      auto msg = fmt::format("physical disk {} role mismatch: persisted {}, configured {}",
                             diskPath,
                             magic_enum::enum_name(config.storage_role),
                             magic_enum::enum_name(expectedRole));
      XLOG(CRITICAL, msg);
      return makeError(CacheCode::kRoleMismatch, std::move(msg));
    }
    return config;
  }

  if (!boost::filesystem::is_empty(diskPath)) {
    auto msg = fmt::format("physical disk {} contains legacy data but has no {}; migration is required",
                           diskPath,
                           kPhysicalDiskConfigFileName);
    XLOG(CRITICAL, msg);
    return makeError(CacheCode::kRoleMismatch, std::move(msg));
  }

  config.physical_disk_id.uuid = Uuid::random();
  config.storage_role = expectedRole;
  RETURN_AND_LOG_ON_ERROR(writePhysicalDiskConfig(diskPath, config));
  return config;
}

uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs) {
  if (rhs > std::numeric_limits<uint64_t>::max() - lhs) return std::numeric_limits<uint64_t>::max();
  return lhs + rhs;
}

}  // namespace

using namespace std::chrono_literals;

Result<Void> CacheSpacePermitRecord::valid() const {
  RETURN_ON_ERROR(physicalDiskId.valid());
  RETURN_ON_ERROR(permit.valid());
  if (footprintBytes == 0 || expiresAtNs == 0) return makeError(CacheCode::kPermitConflict, "invalid permit record");
  if (state != cache::CachePermitState::RESERVED && state != cache::CachePermitState::PINNED)
    return makeError(CacheCode::kPermitConflict, "invalid permit state");
  return Void{};
}

CacheSpacePermitStore::CacheSpacePermitStore(std::unique_ptr<kv::KVStore> store, size_t maxRecords, uint64_t maxBytes)
    : store_(std::move(store)),
      maxRecords_(maxRecords),
      maxBytes_(maxBytes) {}

Result<std::map<std::string, CacheSpacePermitRecord>> CacheSpacePermitStore::loadAll() const {
  if (!store_) return makeError(CacheCode::kJournalFull, "cache permit store is unavailable");
  std::map<std::string, CacheSpacePermitRecord> records;
  auto limit = static_cast<uint32_t>(std::min<size_t>(maxRecords_ + 1, std::numeric_limits<uint32_t>::max()));
  RETURN_ON_ERROR(store_->iterateKeysWithPrefix(kCachePermitPrefix, limit, [&](auto key, auto value) -> Result<Void> {
    CacheSpacePermitRecord record;
    RETURN_ON_ERROR(serde::deserialize(record, value));
    RETURN_ON_ERROR(record.valid());
    records.emplace(std::string{key}, std::move(record));
    return Void{};
  }));
  if (records.size() > maxRecords_) return makeError(CacheCode::kJournalFull, "cache permit record limit exceeded");
  return records;
}

Result<Void> CacheSpacePermitStore::put(std::string_view key, const CacheSpacePermitRecord &record) const {
  if (!store_) return makeError(CacheCode::kJournalFull, "cache permit store is unavailable");
  return store_->put(key, serde::serializeBytes(record), true);
}

Result<Void> CacheSpacePermitStore::remove(std::string_view key) const {
  if (!store_) return makeError(CacheCode::kJournalFull, "cache permit store is unavailable");
  return store_->remove(key);
}

CacheSpaceGate::CacheSpaceGate(PhysicalDiskId diskId, std::unique_ptr<CacheSpacePermitStore> store)
    : diskId_(diskId),
      store_(std::move(store)) {}

std::string CacheSpaceGate::key(const PermitIdentity &permit) {
  auto keyBytes = serde::serializeBytes(
      CachePermitStoreKey{permit.managerEpoch, permit.placement.admissionAttemptId, permit.permitGeneration});
  return std::string{kCachePermitPrefix} + keyBytes.toString();
}

Result<Void> CacheSpaceGate::init() {
  RETURN_ON_ERROR(diskId_.valid());
  if (!store_) return makeError(CacheCode::kJournalFull, "cache permit store is unavailable");
  CHECK_RESULT(records, store_->loadAll());
  uint64_t bytes = 0;
  for (const auto &[key, record] : records) {
    if (record.physicalDiskId != diskId_)
      return makeError(CacheCode::kPermitConflict, "cache permit belongs to another physical disk");
    bytes = saturatingAdd(bytes, key.size());
    bytes = saturatingAdd(bytes, serde::serializeBytes(record).size());
  }
  if (bytes > store_->maxBytes()) return makeError(CacheCode::kJournalFull, "cache permit byte limit exceeded");
  auto lock = std::unique_lock(mutex_);
  records_ = std::move(records);
  serializedBytes_ = bytes;
  return Void{};
}

Result<Void> CacheSpaceGate::expireLocked(uint64_t nowNs) {
  for (auto it = records_.begin(); it != records_.end();) {
    if (it->second.state == cache::CachePermitState::PINNED || it->second.expiresAtNs > nowNs) {
      ++it;
      continue;
    }
    RETURN_ON_ERROR(store_->remove(it->first));
    auto bytes = it->first.size() + serde::serializeBytes(it->second).size();
    serializedBytes_ = serializedBytes_ > bytes ? serializedBytes_ - bytes : 0;
    it = records_.erase(it);
  }
  return Void{};
}

Result<Void> CacheSpaceGate::persistLocked(std::string_view key, const CacheSpacePermitRecord &record, bool replacing) {
  auto value = serde::serializeBytes(record);
  uint64_t previousBytes = 0;
  if (replacing) {
    auto previous = records_.find(std::string{key});
    if (previous != records_.end()) previousBytes = key.size() + serde::serializeBytes(previous->second).size();
  } else if (records_.size() >= store_->maxRecords()) {
    return makeError(CacheCode::kJournalFull, "cache permit record limit reached");
  }
  auto newBytes = key.size() + value.size();
  auto projected = serializedBytes_ > previousBytes ? serializedBytes_ - previousBytes : 0;
  projected = saturatingAdd(projected, newBytes);
  if (projected > store_->maxBytes()) return makeError(CacheCode::kJournalFull, "cache permit byte limit reached");
  RETURN_ON_ERROR(store_->put(key, record));
  records_.insert_or_assign(std::string{key}, record);
  serializedBytes_ = projected;
  return Void{};
}

Result<uint64_t> CacheSpaceGate::reservedBytes(uint64_t nowNs) {
  auto lock = std::unique_lock(mutex_);
  RETURN_ON_ERROR(expireLocked(nowNs));
  uint64_t reserved = 0;
  for (const auto &[_, record] : records_) reserved = saturatingAdd(reserved, record.footprintBytes);
  return reserved;
}

Result<CachePermitResult> CacheSpaceGate::prepare(const CachePermitRequestItem &item,
                                                  uint64_t footprintBytes,
                                                  const CacheDiskPhysicalCapacity &capacity,
                                                  double highWatermark,
                                                  uint64_t nowNs) {
  RETURN_ON_ERROR(item.valid());
  if (footprintBytes == 0 || item.expiresAtNs <= nowNs) return makeError(CacheCode::kPermitExpired);
  auto lock = std::unique_lock(mutex_);
  RETURN_ON_ERROR(expireLocked(nowNs));
  auto recordKey = key(item.permit);
  auto existing = records_.find(recordKey);
  if (existing != records_.end()) {
    if (existing->second.permit != item.permit || existing->second.footprintBytes != footprintBytes)
      return makeError(CacheCode::kPermitConflict, "permit identity was reused with different footprint");
    return CachePermitResult{existing->second.permit, existing->second.state, existing->second.expiresAtNs};
  }
  uint64_t permits = 0;
  for (const auto &[_, record] : records_) permits = saturatingAdd(permits, record.footprintBytes);
  auto projected =
      static_cast<long double>(capacity.physicalUsedBytes) + capacity.reservedBytes + permits + footprintBytes;
  if (capacity.capacityBytes == 0 || highWatermark <= 0.0 || highWatermark > 1.0 ||
      projected > static_cast<long double>(capacity.capacityBytes) * highWatermark)
    return makeError(CacheCode::kCapacityExceeded, "cache physical high watermark exceeded");
  CacheSpacePermitRecord record{diskId_,
                                item.permit,
                                footprintBytes,
                                item.expiresAtNs,
                                cache::CachePermitState::RESERVED};
  RETURN_ON_ERROR(persistLocked(recordKey, record, false));
  return CachePermitResult{record.permit, record.state, record.expiresAtNs};
}

Result<CachePermitResult> CacheSpaceGate::renew(const CachePermitRequestItem &item, uint64_t nowNs) {
  RETURN_ON_ERROR(item.valid());
  if (item.expiresAtNs <= nowNs) return makeError(CacheCode::kPermitExpired);
  auto lock = std::unique_lock(mutex_);
  RETURN_ON_ERROR(expireLocked(nowNs));
  auto recordKey = key(item.permit);
  auto existing = records_.find(recordKey);
  if (existing == records_.end()) return makeError(CacheCode::kPermitExpired, "permit is not active");
  if (existing->second.permit != item.permit) return makeError(CacheCode::kPermitConflict);
  auto updated = existing->second;
  updated.expiresAtNs = std::max(updated.expiresAtNs, item.expiresAtNs);
  RETURN_ON_ERROR(persistLocked(recordKey, updated, true));
  return CachePermitResult{updated.permit, updated.state, updated.expiresAtNs};
}

Result<Void> CacheSpaceGate::release(const PermitIdentity &permit, uint64_t nowNs) {
  RETURN_ON_ERROR(permit.valid());
  auto lock = std::unique_lock(mutex_);
  RETURN_ON_ERROR(expireLocked(nowNs));
  auto recordKey = key(permit);
  auto existing = records_.find(recordKey);
  if (existing == records_.end()) return Void{};
  if (existing->second.permit != permit) return makeError(CacheCode::kPermitConflict);
  if (existing->second.state == cache::CachePermitState::PINNED)
    return makeError(CacheCode::kPermitConflict, "executing permit is pinned");
  RETURN_ON_ERROR(store_->remove(recordKey));
  auto bytes = recordKey.size() + serde::serializeBytes(existing->second).size();
  serializedBytes_ = serializedBytes_ > bytes ? serializedBytes_ - bytes : 0;
  records_.erase(existing);
  return Void{};
}

Result<CachePermitResult> CacheSpaceGate::query(const PermitIdentity &permit, uint64_t nowNs) {
  RETURN_ON_ERROR(permit.valid());
  auto lock = std::unique_lock(mutex_);
  RETURN_ON_ERROR(expireLocked(nowNs));
  auto existing = records_.find(key(permit));
  if (existing == records_.end()) return makeError(CacheCode::kNotFound, "permit is not active");
  if (existing->second.permit != permit) return makeError(CacheCode::kPermitConflict);
  return CachePermitResult{existing->second.permit, existing->second.state, existing->second.expiresAtNs};
}

Result<CachePermitResult> CacheSpaceGate::pin(const PermitIdentity &permit, uint64_t nowNs) {
  RETURN_ON_ERROR(permit.valid());
  auto lock = std::unique_lock(mutex_);
  RETURN_ON_ERROR(expireLocked(nowNs));
  auto recordKey = key(permit);
  auto existing = records_.find(recordKey);
  if (existing == records_.end()) return makeError(CacheCode::kPermitExpired, "permit is not active");
  if (existing->second.permit != permit) return makeError(CacheCode::kPermitConflict);
  auto updated = existing->second;
  updated.state = cache::CachePermitState::PINNED;
  RETURN_ON_ERROR(persistLocked(recordKey, updated, true));
  return CachePermitResult{updated.permit, updated.state, updated.expiresAtNs};
}

Result<Void> CacheSpaceGate::consume(const PermitIdentity &permit) {
  RETURN_ON_ERROR(permit.valid());
  auto lock = std::unique_lock(mutex_);
  auto recordKey = key(permit);
  auto existing = records_.find(recordKey);
  if (existing == records_.end()) return Void{};
  if (existing->second.permit != permit) return makeError(CacheCode::kPermitConflict);
  RETURN_ON_ERROR(store_->remove(recordKey));
  auto bytes = recordKey.size() + serde::serializeBytes(existing->second).size();
  serializedBytes_ = serializedBytes_ > bytes ? serializedBytes_ - bytes : 0;
  records_.erase(existing);
  return Void{};
}

CacheDiskPhysicalCapacity calculateCacheDiskPhysicalCapacity(const CacheTargetPhysicalUsage &targetUsage,
                                                             uint64_t engineAllocatedBytes,
                                                             uint64_t engineReservedBytes,
                                                             uint64_t filesystemAvailableBytes) {
  CacheDiskPhysicalCapacity result;
  auto engineActive = engineAllocatedBytes >= engineReservedBytes ? engineAllocatedBytes - engineReservedBytes : 0;
  result.physicalUsedBytes = saturatingAdd(targetUsage.activeBytes, targetUsage.unrecycledBytes);
  result.physicalUsedBytes = saturatingAdd(result.physicalUsedBytes, engineActive);
  result.reservedBytes = saturatingAdd(targetUsage.reservedBytes, engineReservedBytes);
  result.allocatableBytes = filesystemAvailableBytes;
  result.capacityBytes = saturatingAdd(result.physicalUsedBytes, result.reservedBytes);
  result.capacityBytes = saturatingAdd(result.capacityBytes, result.allocatableBytes);
  return result;
}

StorageTargets::~StorageTargets() { void(); }

Result<Void> StorageTargets::init(CPUExecutorGroup &executor) {
  manufacturers_.clear();
  pathToDiskIndex_.clear();
  diskConfigs_.clear();
  engines_.clear();
  cacheSpaceGates_.clear();
  cacheEventJournals_.clear();

  auto diskInfoResult = SysResource::scanDiskInfo();
  RETURN_AND_LOG_ON_ERROR(diskInfoResult);
  std::unordered_map<uint32_t, std::string> deviceIdToManufacturer;
  for (auto &info : *diskInfoResult) {
    deviceIdToManufacturer[info.deviceId] = info.manufacturer;
  }

  targetPaths_ = config_.target_paths();
  auto &diskRoles = config_.disk_roles();
  if (!diskRoles.empty() && diskRoles.size() != targetPaths_.size()) {
    auto msg =
        fmt::format("disk_roles size {} does not match target_paths size {}", diskRoles.size(), targetPaths_.size());
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }
  for (auto &path : targetPaths_) {
    struct stat st;
    int succ = ::stat(path.c_str(), &st);
    if (succ != 0) {
      auto msg = fmt::format("stat {} failed: {}", path, errno);
      XLOG(ERR, msg);
      return makeError(StorageCode::kStorageStatFailed, std::move(msg));
    }
    manufacturers_.push_back(deviceIdToManufacturer[st.st_dev]);
  }

  diskConfigs_.resize(targetPaths_.size());
  if (!diskRoles.empty()) {
    for (size_t index = 0; index < targetPaths_.size(); ++index) {
      CHECK_RESULT(config, loadOrCreatePhysicalDiskConfig(targetPaths_[index], diskRoles[index]));
      diskConfigs_[index] = std::move(config);
    }
  }

  for (size_t index = 0; index < diskConfigs_.size(); ++index) {
    const auto &disk = diskConfigs_[index];
    if (disk.storage_role != StorageRole::CACHE_ONLY) continue;
    kv::KVStore::Options options;
    options.type = config_.cache_permit_store().type();
    options.path = targetPaths_[index] / std::string{kCachePermitStoreDirectory};
    options.createIfMissing = true;
    auto kvStore = kv::KVStore::create(config_.cache_permit_store(), options);
    if (!kvStore) return makeError(CacheCode::kJournalFull, "failed to open cache permit store");
    auto permitStore = std::make_unique<CacheSpacePermitStore>(std::move(kvStore),
                                                               config_.cache_permit_max_records(),
                                                               config_.cache_permit_max_bytes());
    auto gate = std::make_unique<CacheSpaceGate>(disk.physical_disk_id, std::move(permitStore));
    RETURN_ON_ERROR(gate->init());
    cacheSpaceGates_.emplace(disk.physical_disk_id, std::move(gate));

    options.type = config_.cache_event_store().type();
    options.path = targetPaths_[index] / std::string{kCacheEventStoreDirectory};
    auto eventStore = kv::KVStore::create(config_.cache_event_store(), options);
    if (!eventStore) return makeError(CacheCode::kJournalFull, "failed to open cache event journal");
    auto journal = std::make_unique<CacheEventJournal>(std::move(eventStore),
                                                       config_.cache_event_journal_max_records(),
                                                       config_.cache_event_journal_max_bytes());
    RETURN_ON_ERROR(journal->init());
    cacheEventJournals_.emplace(disk.physical_disk_id, std::move(journal));
  }

  uint32_t i = 0;
  for (auto &path : targetPaths_) {
    pathToDiskIndex_[path] = i++;
  }

  std::vector<folly::coro::TaskWithExecutor<Result<rust::Box<chunk_engine::Engine>>>> tasks;
  for (auto &path : targetPaths_) {
    auto engine_path = path / "engine";
    bool create = !boost::filesystem::exists(engine_path);
    create |= config_.create_engine_path();
    tasks.push_back(folly::coro::co_invoke([engine_path, create]() -> CoTryTask<rust::Box<chunk_engine::Engine>> {
                      std::string error;
                      auto engine = chunk_engine::create(engine_path.c_str(), create, sizeof(ChainId), error);
                      if (!error.empty()) {
                        co_return makeError(StorageCode::kStorageStatFailed, std::move(error));
                      }
                      co_return rust::Box<chunk_engine::Engine>::from_raw(engine);
                    }).scheduleOn(&executor.pickNext()));
  }

  auto results = folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));
  for (auto &result : results) {
    RETURN_AND_LOG_ON_ERROR(result);
    engines_.push_back(std::move(result.value()));
  }

  return Void{};
}

Result<Void> StorageTargets::create(const CreateConfig &createConfig) {
  CPUExecutorGroup executor(1, "Creator");
  RETURN_AND_LOG_ON_ERROR(init(executor));
  auto targetPaths = config_.target_paths();
  auto targetNumPerPath = config_.target_num_per_path();
  auto targetIdSize = createConfig.target_ids().size();
  if (targetPaths.empty()) {
    auto msg = fmt::format("List of target path is empty");
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }
  if (targetNumPerPath == 0) {
    auto msg = fmt::format("Target num per path is 0!");
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }
  if (targetPaths.size() * targetNumPerPath != targetIdSize) {
    auto msg = fmt::format("Unable to arrange target. path size {}, target num per path {}, target id size {}",
                           targetPaths.size(),
                           targetNumPerPath,
                           targetIdSize);
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, msg);
  }

  size_t idx = 0;
  for (auto &targetId : createConfig.target_ids()) {
    auto diskIndex = idx / targetNumPerPath;
    auto storageTarget = StorageTarget::enable_shared_from_this::create(config_.storage_target(),
                                                                        globalFileStore_,
                                                                        diskIndex,
                                                                        &*engines_[diskIndex]);
    PhysicalConfig targetConfig;
    targetConfig.path = targetPaths[diskIndex] / std::to_string(targetId);
    targetConfig.target_id = targetId;
    targetConfig.allow_disk_without_uuid = createConfig.allow_disk_without_uuid();
    targetConfig.allow_existing_targets = createConfig.allow_existing_targets();
    targetConfig.physical_disk_id = diskConfigs_[diskIndex].physical_disk_id;
    targetConfig.storage_role = diskConfigs_[diskIndex].storage_role;
    targetConfig.physical_file_count = createConfig.physical_file_count();
    targetConfig.chunk_size_list = createConfig.chunk_size_list();
    targetConfig.only_chunk_engine = createConfig.only_chunk_engine();
    RETURN_AND_LOG_ON_ERROR(storageTarget->create(targetConfig));
    ++idx;
    RETURN_AND_LOG_ON_ERROR(targetMap_.addStorageTarget(std::move(storageTarget)));
  }
  return Void{};
}

Result<Void> StorageTargets::create(const CreateTargetReq &req) {
  if (req.diskIndex >= config_.target_paths().size()) {
    auto msg = fmt::format("disk index exceed {} >= {}", req.diskIndex, config_.target_paths().size());
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }
  if (req.chainId == ChainId{}) {
    auto msg = fmt::format("target {} without chain id", req.targetId);
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }

  folly::coro::Baton baton;
  auto lock = targetLocks_.lock(baton, fmt::to_string(req.chainId));
  if (!lock.locked()) {
    folly::coro::blockingWait(lock.lock());
  }
  if (auto existingTarget = targetMap_.snapshot()->getByChainId(VersionedChainId{req.chainId, {}}, true)) {
    auto existingTargetId = (*existingTarget)->targetId;
    if (existingTargetId != req.targetId) {
      auto msg = fmt::format("target {} is existing with same chain id {}, req target {}",
                             existingTargetId,
                             req.chainId,
                             req.targetId);
      XLOG(ERR, msg);
      return makeError(StorageCode::kStorageInitFailed, std::move(msg));
    }
    if (req.addChunkSize) {
      RETURN_AND_LOG_ON_ERROR((*existingTarget)->storageTarget->addChunkSize(req.chunkSizeList));
    }
  } else if (req.addChunkSize) {
    auto msg = fmt::format("target {} {} is not existing", req.chainId, req.targetId);
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }
  if (targetMap_.snapshot()->getTarget(req.targetId)) {
    if (req.allowExistingTarget) {
      auto targetPath = config_.target_paths()[req.diskIndex] / std::to_string(req.targetId);
      if (!boost::filesystem::exists(targetPath)) {
        auto msg = fmt::format("target {} is existing in memory, but not found in disk", req.targetId);
        XLOG(ERR, msg);
        return makeError(StorageCode::kStorageInitFailed, std::move(msg));
      }
      XLOGF(INFO, "target {} is already existing, return succ", req.targetId);
      return Void{};
    } else {
      auto msg = fmt::format("target {} is already existing", req.targetId);
      XLOG(ERR, msg);
      return makeError(StorageCode::kStorageInitFailed, std::move(msg));
    }
  }

  auto storageTarget = StorageTarget::enable_shared_from_this::create(config_.storage_target(),
                                                                      globalFileStore_,
                                                                      req.diskIndex,
                                                                      &*engines_[req.diskIndex]);
  PhysicalConfig targetConfig;
  auto targetPath = config_.target_paths()[req.diskIndex] / std::to_string(req.targetId);
  targetConfig.path = targetPath;
  targetConfig.target_id = req.targetId;
  targetConfig.chain_id = req.chainId;
  targetConfig.allow_disk_without_uuid = config_.allow_disk_without_uuid();
  targetConfig.allow_existing_targets = req.allowExistingTarget;
  targetConfig.physical_disk_id = diskConfigs_[req.diskIndex].physical_disk_id;
  targetConfig.storage_role = diskConfigs_[req.diskIndex].storage_role;
  targetConfig.physical_file_count = req.physicalFileCount;
  targetConfig.chunk_size_list = req.chunkSizeList;
  targetConfig.kv_store_type = config_.storage_target().kv_store().type();
  targetConfig.only_chunk_engine = req.onlyChunkEngine;
  RETURN_AND_LOG_ON_ERROR(storageTarget->create(targetConfig));
  XLOGF(INFO, "Create storage target {} at {}", storageTarget->targetId(), targetPath.string());
  RETURN_AND_LOG_ON_ERROR(targetMap_.addStorageTarget(std::move(storageTarget)));
  return Void{};
}

Result<Void> StorageTargets::load(CPUExecutorGroup &executor) {
  RETURN_AND_LOG_ON_ERROR(init(executor));
  std::vector<folly::coro::TaskWithExecutor<Result<Void>>> tasks;
  for (auto &parentPath : config_.target_paths()) {
    auto writable = CheckWorker::checkWritable(parentPath);
    if (!writable) {
      XLOGF(DFATAL, "path {} isn't writable, skip it", parentPath);
    }
    for (auto &targetPath : boost::filesystem::directory_iterator(parentPath)) {
      auto targetConfigPath = targetPath / kPhysicalConfigFileName;
      if (boost::filesystem::is_directory(targetPath) && boost::filesystem::is_regular_file(targetConfigPath)) {
        tasks.push_back(folly::coro::co_invoke([this, targetPath]() -> CoTryTask<Void> {
                          co_return loadTarget(targetPath);
                        }).scheduleOn(&executor.pickNext()));
      }
    }
  }
  auto results = folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));
  for (auto &result : results) {
    RETURN_AND_LOG_ON_ERROR(result);
  }
  if (config_.collect_all_fds()) {
    globalFileStore_.collect(fds_);
  }
  return Void{};
}

// load a target.
Result<Void> StorageTargets::loadTarget(const Path &targetPath) {
  auto diskPath = targetPath.parent_path();
  if (UNLIKELY(!pathToDiskIndex_.contains(diskPath))) {
    auto msg = fmt::format("Target path ({}) not belongs to any of disk paths", targetPath);
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }

  auto diskIndex = pathToDiskIndex_[diskPath];
  auto storageTarget = StorageTarget::enable_shared_from_this::create(config_.storage_target(),
                                                                      globalFileStore_,
                                                                      diskIndex,
                                                                      &*engines_[diskIndex]);
  RETURN_AND_LOG_ON_ERROR(storageTarget->load(targetPath));
  const auto &diskConfig = diskConfigs_[diskIndex];
  if (diskConfig.storage_role != StorageRole::INVALID &&
      (storageTarget->physicalDiskId() != diskConfig.physical_disk_id ||
       storageTarget->storageRole() != diskConfig.storage_role)) {
    auto msg = fmt::format("target {} identity does not match physical disk {}", targetPath, diskPath);
    XLOG(CRITICAL, msg);
    return makeError(CacheCode::kRoleMismatch, std::move(msg));
  }
  XLOGF(INFO, "Load storage target {} at {}", storageTarget->targetId(), targetPath.string());
  auto targetId = storageTarget->targetId();
  if (UNLIKELY(targetPath.filename().string() != fmt::format("{}", targetId.toUnderType()))) {
    auto msg = fmt::format("Target id {} and path {} mismatch!", targetId, targetPath);
    XLOG(ERR, msg);
    return makeError(StorageCode::kStorageInitFailed, std::move(msg));
  }
  RETURN_AND_LOG_ON_ERROR(targetMap_.addStorageTarget(std::move(storageTarget)));
  return Void{};
}

Result<std::vector<SpaceInfo>> StorageTargets::spaceInfos(bool force) {
  folly::coro::Baton baton;
  auto lock = targetLocks_.lock(baton, "spaceInfos");
  if (!lock.locked()) {
    folly::coro::blockingWait(lock.lock());
  }

  auto now = RelativeTime::now();
  auto elapsedTime = now - spaceInfoUpdatedTime_;
  if (elapsedTime < config_.space_info_cache_timeout() && !force) {
    return cachedSpaceInfos_;
  }

  std::unordered_map<std::string, CacheTargetPhysicalUsage> cacheUsage;
  std::unordered_map<std::string, uint64_t> diskUnusedSize;
  std::unordered_map<std::string, std::vector<hf3fs::flat::TargetId>> pathToTargetIds;
  auto snapshot = targetMap_.snapshot();
  for (auto &[targetId, target] : snapshot->getTargets()) {
    pathToTargetIds[target.path.parent_path().string()].emplace_back(targetId);
    if (target.storageTarget != nullptr) {
      diskUnusedSize[target.path.parent_path().string()] += target.storageTarget->unusedSize();
      if (target.storageRole == StorageRole::CACHE_ONLY && !target.storageTarget->useChunkEngine()) {
        CHECK_RESULT(usage, target.storageTarget->cachePhysicalUsage());
        auto &diskUsage = cacheUsage[target.path.parent_path().string()];
        diskUsage.activeBytes = saturatingAdd(diskUsage.activeBytes, usage.activeBytes);
        diskUsage.reservedBytes = saturatingAdd(diskUsage.reservedBytes, usage.reservedBytes);
        diskUsage.unrecycledBytes = saturatingAdd(diskUsage.unrecycledBytes, usage.unrecycledBytes);
      }
    }
  }

  std::vector<SpaceInfo> ret;
  for (auto &[path, index] : pathToDiskIndex_) {
    SpaceInfo info;
    info.path = targetPaths_[index].string();
    info.targetIds = pathToTargetIds[info.path];

    boost::system::error_code ec{};
    auto spaceInfo = boost::filesystem::space(path, ec);
    if (UNLIKELY(ec.failed())) {
      auto msg = fmt::format("get space info of directory {} failed: {}", path, ec.message());
      XLOG(ERR, msg);
      return makeError(StorageCode::kChunkOpenFailed, std::move(msg));
    }
    auto usedSize = engines_[index]->raw_used_size();
    info.capacity = spaceInfo.capacity;
    info.free = spaceInfo.free + diskUnusedSize[info.path] + usedSize.reserved_size;
    info.available = spaceInfo.available;
    info.manufacturer = manufacturers_[index];
    if (index < diskConfigs_.size()) {
      info.physicalDiskId = diskConfigs_[index].physical_disk_id;
      info.storageRole = diskConfigs_[index].storage_role;
    }
    if (info.storageRole == StorageRole::CACHE_ONLY) {
      auto capacity = calculateCacheDiskPhysicalCapacity(cacheUsage[info.path],
                                                         usedSize.allocated_size,
                                                         usedSize.reserved_size,
                                                         spaceInfo.available);
      info.cachePhysicalUsedBytes = capacity.physicalUsedBytes;
      info.cacheReservedBytes = capacity.reservedBytes;
      info.cacheAllocatableBytes = capacity.allocatableBytes;
      info.cacheCapacityBytes = capacity.capacityBytes;
      auto gate = cacheSpaceGate(info.physicalDiskId);
      if (gate != nullptr) {
        CHECK_RESULT(permitReserved,
                     gate->reservedBytes(static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000));
        info.cacheReservedBytes = saturatingAdd(info.cacheReservedBytes, permitReserved);
        info.cacheAllocatableBytes =
            permitReserved < info.cacheAllocatableBytes ? info.cacheAllocatableBytes - permitReserved : uint64_t{0};
      }
      info.enforcedAdmissionHighWatermark = config_.cache_admission_high_watermark();
      info.sampledAtNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
    }
    ret.push_back(std::move(info));
  }
  cachedSpaceInfos_ = ret;
  spaceInfoUpdatedTime_ = RelativeTime::now();
  return ret;
}

}  // namespace hf3fs::storage
