#pragma once

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <map>
#include <mutex>

#include "chunk_engine/src/cxx.rs.h"
#include "common/utils/CPUExecutorGroup.h"
#include "common/utils/CoLockManager.h"
#include "common/utils/ConfigBase.h"
#include "common/utils/RobinHood.h"
#include "fbs/mgmtd/HeartbeatInfo.h"
#include "fbs/storage/Common.h"
#include "kv/KVStore.h"
#include "storage/cache/event/CacheEventJournal.h"
#include "storage/service/TargetMap.h"
#include "storage/store/StorageTarget.h"

namespace hf3fs::test {
struct StorageTargetsHelper;
}

namespace hf3fs::storage {

struct CacheDiskPhysicalCapacity {
  uint64_t capacityBytes = 0;
  uint64_t physicalUsedBytes = 0;
  uint64_t allocatableBytes = 0;
  uint64_t reservedBytes = 0;
};

struct CacheSpacePermitRecord {
  SERDE_STRUCT_FIELD(physicalDiskId, PhysicalDiskId{});
  SERDE_STRUCT_FIELD(permit, PermitIdentity{});
  SERDE_STRUCT_FIELD(footprintBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(expiresAtNs, uint64_t{0});
  SERDE_STRUCT_FIELD(state, cache::CachePermitState::INVALID);

 public:
  Result<Void> valid() const;
};

class CacheSpacePermitStore {
 public:
  CacheSpacePermitStore(std::unique_ptr<kv::KVStore> store, size_t maxRecords, uint64_t maxBytes);

  Result<std::map<std::string, CacheSpacePermitRecord>> loadAll() const;
  Result<Void> put(std::string_view key, const CacheSpacePermitRecord &record) const;
  Result<Void> remove(std::string_view key) const;
  size_t maxRecords() const { return maxRecords_; }
  uint64_t maxBytes() const { return maxBytes_; }

 private:
  std::unique_ptr<kv::KVStore> store_;
  size_t maxRecords_;
  uint64_t maxBytes_;
};

class CacheSpaceGate {
 public:
  CacheSpaceGate(PhysicalDiskId diskId, std::unique_ptr<CacheSpacePermitStore> store);

  Result<Void> init();
  Result<CachePermitResult> prepare(const CachePermitRequestItem &item,
                                    uint64_t footprintBytes,
                                    const CacheDiskPhysicalCapacity &capacity,
                                    double highWatermark,
                                    uint64_t nowNs);
  Result<CachePermitResult> renew(const CachePermitRequestItem &item, uint64_t nowNs);
  Result<Void> release(const PermitIdentity &permit, uint64_t nowNs);
  Result<CachePermitResult> query(const PermitIdentity &permit, uint64_t nowNs);
  Result<CachePermitResult> pin(const PermitIdentity &permit, uint64_t nowNs);
  Result<Void> consume(const PermitIdentity &permit);
  Result<uint64_t> reservedBytes(uint64_t nowNs);
  const PhysicalDiskId &diskId() const { return diskId_; }

 private:
  Result<Void> expireLocked(uint64_t nowNs);
  Result<Void> persistLocked(std::string_view key, const CacheSpacePermitRecord &record, bool replacing);
  static std::string key(const PermitIdentity &permit);

  PhysicalDiskId diskId_;
  std::unique_ptr<CacheSpacePermitStore> store_;
  std::mutex mutex_;
  std::map<std::string, CacheSpacePermitRecord> records_;
  uint64_t serializedBytes_ = 0;
};

CacheDiskPhysicalCapacity calculateCacheDiskPhysicalCapacity(const CacheTargetPhysicalUsage &targetUsage,
                                                             uint64_t engineAllocatedBytes,
                                                             uint64_t engineReservedBytes,
                                                             uint64_t filesystemAvailableBytes);

class StorageTargets {
 public:
  class Config : public ConfigBase<Config> {
    CONFIG_ITEM(target_paths, std::vector<Path>{}, [](auto &vec) { return !vec.empty(); });
    CONFIG_ITEM(disk_roles, std::vector<StorageRole>{});
    CONFIG_ITEM(target_num_per_path, 0u);
    CONFIG_HOT_UPDATED_ITEM(collect_all_fds, true);
    CONFIG_HOT_UPDATED_ITEM(space_info_cache_timeout, 5_s);
    CONFIG_HOT_UPDATED_ITEM(allow_disk_without_uuid, false);
    CONFIG_HOT_UPDATED_ITEM(create_engine_path, true);
    CONFIG_HOT_UPDATED_ITEM(cache_admission_high_watermark, 0.9, [](double value) {
      return value > 0.0 && value <= 1.0;
    });
    CONFIG_OBJ(cache_permit_store, kv::KVStore::Config);
    CONFIG_ITEM(cache_permit_max_records, size_t{100000}, ConfigCheckers::checkPositive);
    CONFIG_ITEM(cache_permit_max_bytes, uint64_t{64_MB}, ConfigCheckers::checkPositive);
    CONFIG_OBJ(cache_event_store, kv::KVStore::Config);
    CONFIG_ITEM(cache_event_journal_max_records, size_t{100000}, ConfigCheckers::checkPositive);
    CONFIG_ITEM(cache_event_journal_max_bytes, uint64_t{1_GB}, ConfigCheckers::checkPositive);
    CONFIG_OBJ(storage_target, StorageTarget::Config);
  };

  class CreateConfig : public ConfigBase<CreateConfig> {
    CONFIG_ITEM(target_ids, std::vector<flat::TargetId::UnderlyingType>{});
    CONFIG_ITEM(physical_file_count, 256u);
    CONFIG_ITEM(allow_disk_without_uuid, false);
    CONFIG_ITEM(allow_existing_targets, false);
    CONFIG_ITEM(chunk_size_list, (std::vector<Size>{512_KB, 1_MB, 2_MB, 4_MB, 16_MB, 64_MB}));
    CONFIG_ITEM(only_chunk_engine, false);
  };

  StorageTargets(const Config &config, AtomicallyTargetMap &targetMap)
      : config_(config),
        targetMap_(targetMap) {}
  ~StorageTargets();

  Result<Void> init(CPUExecutorGroup &executor);

  // create a batch of storage targets.
  Result<Void> create(const CreateConfig &createConfig);

  // create new storage target.
  Result<Void> create(const CreateTargetReq &req);

  // open a batch of storage targets.
  Result<Void> load(CPUExecutorGroup &executor);

  // load a target.
  Result<Void> loadTarget(const Path &targetPath);

  // get fd list.
  auto &fds() const { return fds_; }

  // get space info.
  Result<std::vector<SpaceInfo>> spaceInfos(bool force);

  // get target paths.
  auto &targetPaths() const { return targetPaths_; }

  // get manufacturers.
  auto &manufacturers() const { return manufacturers_; }

  // global file store.
  auto &globalFileStore() { return globalFileStore_; }

  // chunk engines.
  auto &engines() const { return engines_; }

  CacheSpaceGate *cacheSpaceGate(const PhysicalDiskId &diskId) const {
    auto gate = cacheSpaceGates_.find(diskId);
    return gate == cacheSpaceGates_.end() ? nullptr : gate->second.get();
  }

  CacheEventJournal *cacheEventJournal(const PhysicalDiskId &diskId) const {
    auto journal = cacheEventJournals_.find(diskId);
    return journal == cacheEventJournals_.end() ? nullptr : journal->second.get();
  }

  // remove target.
  Result<Void> removeChunkEngineTarget(ChainId chainId, uint32_t diskIndex) {
    auto &engine = *engines_[diskIndex];
    return ChunkEngine::removeAllChunks(engine, chainId);
  }

 private:
  friend struct test::StorageTargetsHelper;
  ConstructLog<"storage::StorageTargets"> constructLog_;
  const Config &config_;
  AtomicallyTargetMap &targetMap_;
  GlobalFileStore globalFileStore_;

  std::vector<Path> targetPaths_;
  std::vector<std::string> manufacturers_;
  std::vector<PhysicalDiskConfig> diskConfigs_;
  std::map<Path, uint32_t> pathToDiskIndex_;
  std::vector<rust::Box<chunk_engine::Engine>> engines_;
  std::map<PhysicalDiskId, std::unique_ptr<CacheSpaceGate>> cacheSpaceGates_;
  std::map<PhysicalDiskId, std::unique_ptr<CacheEventJournal>> cacheEventJournals_;

  CoLockManager<> targetLocks_;
  RelativeTime spaceInfoUpdatedTime_;
  std::vector<SpaceInfo> cachedSpaceInfos_;

  std::vector<int> fds_;
};

}  // namespace hf3fs::storage
