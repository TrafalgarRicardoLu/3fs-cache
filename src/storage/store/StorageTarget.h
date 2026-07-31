#pragma once

#include <bit>
#include <folly/Synchronized.h>
#include <span>
#include <unordered_map>

#include "chunk_engine/src/cxx.rs.h"
#include "common/monitor/Recorder.h"
#include "common/utils/CoLockManager.h"
#include "common/utils/ConfigBase.h"
#include "common/utils/LockManager.h"
#include "common/utils/Path.h"
#include "storage/aio/BatchReadJob.h"
#include "storage/store/ChunkEngine.h"
#include "storage/store/ChunkStore.h"
#include "storage/store/PhysicalConfig.h"
#include "storage/store/cache/LocalCacheAccess.h"
#include "storage/update/UpdateJob.h"

namespace hf3fs::storage {

struct CacheTargetPhysicalUsage {
  uint64_t activeBytes = 0;
  uint64_t reservedBytes = 0;
  uint64_t unrecycledBytes = 0;
};

inline Result<uint64_t> physicalFootprint(bool chunkEngine,
                                          std::span<const Size> allocationUnits,
                                          uint32_t chunkSize,
                                          uint64_t payloadLength) {
  if (chunkSize == 0 || payloadLength == 0 || payloadLength > chunkSize)
    return makeError(StatusCode::kInvalidArg, "invalid cache chunk footprint request");
  if (std::find(allocationUnits.begin(), allocationUnits.end(), Size{chunkSize}) == allocationUnits.end())
    return makeError(StatusCode::kInvalidArg, "unsupported cache chunk allocation unit");
  if (chunkEngine) {
    constexpr uint64_t kSmallestEngineAllocation = 64_KB;
    constexpr uint64_t kLargestEngineAllocation = 64_MB;
    if (payloadLength > kLargestEngineAllocation)
      return makeError(StatusCode::kInvalidArg, "cache payload exceeds chunk engine allocation range");
    return std::max<uint64_t>(std::bit_ceil(payloadLength), kSmallestEngineAllocation);
  }
  return uint64_t{chunkSize};
}

class StorageTarget : public enable_shared_from_this<StorageTarget> {
 protected:
  StorageTarget(const ChunkStore::Config &config,
                GlobalFileStore &globalFileStore,
                uint32_t diskIndex,
                chunk_engine::Engine *engine);

 public:
  using Config = ChunkStore::Config;

  ~StorageTarget();

  // create storage target.
  Result<Void> create(const PhysicalConfig &config);

  // load storage target.
  Result<Void> load(const Path &path);

  // add new chunk size.
  Result<Void> addChunkSize(const std::vector<Size> &sizeList);

  // get target id. [guaranteed loaded]
  TargetId targetId() const { return TargetId{targetConfig_.target_id}; }

  // get chain id. [guaranteed loaded]
  ChainId chainId() const { return ChainId{targetConfig_.chain_id}; }

  // set chain id.
  Result<Void> setChainId(ChainId chainId);

  // get disk index.
  uint32_t diskIndex() const { return diskIndex_; }

  // get persistent physical disk identity and role.
  const PhysicalDiskId &physicalDiskId() const { return targetConfig_.physical_disk_id; }
  StorageRole storageRole() const { return targetConfig_.storage_role; }

  // get target path. [guaranteed loaded]
  Path path() const { return targetConfig_.path; }

  // get all chunk metadata
  Result<Void> getAllMetadata(ChunkMetaVector &metadataVec);
  Result<Void> getAllMetadataMap(std::unordered_map<ChunkId, ChunkMetadata> &metas);

  // lock chunk.
  auto lockChunk(folly::coro::Baton &baton, const ChunkId &chunk, const std::string &tag) {
    return chunkLocks_.lock(baton, chunk.data(), tag);
  }

  // try lock channel.
  auto tryLockChannel(folly::coro::Baton &baton, const std::string &key) { return channelLocks_.tryLock(baton, key); }

  // prepare aio read.
  Result<Void> aioPrepareRead(AioReadJob &job);

  // finish aio read.
  Result<Void> aioFinishRead(AioReadJob &job);

  // update chunk (write/remove/truncate).
  void updateChunk(UpdateJob &job, folly::CPUThreadPoolExecutor &executor);

  // query chunks: the chunk ids in result are in reverse lexicographical order
  Result<std::vector<std::pair<ChunkId, ChunkMetadata>>> queryChunks(const ChunkIdRange &chunkIdRange);

  // query chunk.
  Result<ChunkMetadata> queryChunk(const ChunkId &chunkId);

  Result<CacheChunkGenerationInfo> replaceCacheChunk(const ReplaceCacheChunkItem &item,
                                                     folly::CPUThreadPoolExecutor &executor);
  Result<CacheChunkGenerationInfo> retireCacheChunk(const RetireCacheChunkItem &item);
  Result<CacheChunkGenerationInfo> retireCacheChunkDurable(const RetireCacheChunkItem &item);
  Result<CacheChunkGenerationInfo> queryCacheChunk(const ChunkId &chunkId);
  Result<CacheChunkGenerationInfo> queryCacheChunk(const CacheChunkKey &key);
  Result<std::optional<CacheChunkDescriptor>> queryCacheChunkDescriptor(const ChunkId &chunkId);
  Result<std::optional<CacheChunkDescriptor>> queryCacheChunkDescriptor(const CacheChunkKey &key);
  CoTryTask<bool> recordCacheAccess(const ChunkId &chunkId,
                                    cache::CacheGeneration generation,
                                    uint64_t observedAtNs,
                                    Duration persistInterval);

  // recycle a batch of chunks. return true if all holes are punched.
  Result<bool> punchHole() {
    if (useChunkEngine()) {
      return true;
    } else {
      return chunkStore_.punchHole();
    }
  }

  // sync meta kv.
  Result<Void> sync() {
    if (useChunkEngine()) {
      return Void{};
    } else {
      return chunkStore_.sync();
    }
  }

  // report unrecycled size.
  Result<Void> reportUnrecycledSize();

  // get used size.
  uint64_t usedSize() const {
    if (useChunkEngine()) {
      return ChunkEngine::chainUsedSize(*engine_, ChainId{targetConfig_.chain_id});
    } else {
      return chunkStore_.usedSize();
    }
  }

  // get unused size.
  uint64_t unusedSize() const { return unusedSize_; }

  Result<CacheTargetPhysicalUsage> cachePhysicalUsage() {
    if (useChunkEngine()) return CacheTargetPhysicalUsage{};
    int64_t reserved = 0;
    int64_t unrecycled = 0;
    RETURN_ON_ERROR(chunkStore_.unusedSize(reserved, unrecycled));
    return CacheTargetPhysicalUsage{usedSize(),
                                    static_cast<uint64_t>(std::max<int64_t>(reserved, 0)),
                                    static_cast<uint64_t>(std::max<int64_t>(unrecycled, 0))};
  }

  Result<uint64_t> physicalFootprint(uint32_t chunkSize, uint64_t payloadLength) const {
    auto sizes = chunkSizeList_.lock();
    std::vector<Size> allocationUnits(sizes->begin(), sizes->end());
    return storage::physicalFootprint(useChunkEngine(), allocationUnits, chunkSize, payloadLength);
  }

  // get all uncommitted chunk ids.
  Result<std::vector<ChunkId>> uncommitted() {
    if (useChunkEngine()) {
      return ChunkEngine::queryUncommittedChunks(*engine_, chainId());
    } else {
      return chunkStore_.uncommitted();
    }
  }

  // reset uncommitted chunk to committed state.
  Result<Void> resetUncommitted(ChainVer chainVer) {
    if (useChunkEngine()) {
      return ChunkEngine::resetUncommittedChunks(*engine_, chainId(), chainVer);
    } else {
      return chunkStore_.resetUncommitted(chainVer);
    }
  }

  // enable or disable emergency recycling.
  void setEmergencyRecycling(bool enable) {
    if (useChunkEngine()) {
      return;
    } else {
      return chunkStore_.setEmergencyRecycling(enable);
    }
  }

  // record real read.
  void recordRealRead(uint32_t bytes, Duration latency) const;

  // disk monitor tag.
  auto &tag() const { return diskTag_; }

  // check alive or not.
  std::weak_ptr<bool> aliveWeakPtr() const { return alive_; }

  // global serial number.
  auto generationId() const { return generationId_; }

  // release self.
  Result<Void> release() {
    released_ = true;
    return sync();
  }

  // check if chunk engine is used.
  inline bool useChunkEngine() const { return targetConfig_.only_chunk_engine; }

 private:
  const Config &config_;
  std::shared_ptr<bool> alive_ = std::make_shared<bool>();
  uint32_t diskIndex_;
  uint32_t generationId_;
  chunk_engine::Engine *engine_{};
  std::atomic<uint64_t> unusedSize_{};
  monitor::TagSet diskTag_;
  monitor::TagSet targetTag_;
  monitor::Recorder::TagRef<monitor::CountRecorder> readCountPerDisk_;
  monitor::Recorder::TagRef<monitor::CountRecorder> readBytesPerDisk_;
  monitor::Recorder::TagRef<monitor::CountRecorder> readSuccBytesPerDisk_;
  monitor::Recorder::TagRef<monitor::LatencyRecorder> readSuccLatencyPerDisk_;
  monitor::Recorder::TagRef<monitor::ValueRecorder> targetUsedSize_;
  monitor::Recorder::TagRef<monitor::ValueRecorder> targetReservedSize_;
  monitor::Recorder::TagRef<monitor::ValueRecorder> targetUnrecycledSize_;
  PhysicalConfig targetConfig_;
  ChunkStore chunkStore_;
  LocalCacheAccess localCacheAccess_;
  CoLockManager<> chunkLocks_;
  CoLockManager<> channelLocks_;
  folly::Synchronized<std::set<Size>, std::mutex> chunkSizeList_;
  bool released_ = false;
};

}  // namespace hf3fs::storage
