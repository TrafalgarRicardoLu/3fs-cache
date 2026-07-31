#pragma once

#include <functional>
#include <map>
#include <memory>
#include <span>

#include "cache/origin/ObjectStore.h"
#include "cache_manager/admission/CapacityGate.h"
#include "cache_manager/config/Config.h"
#include "cache_manager/scheduler/HintCoalescer.h"
#include "client/mgmtd/ICommonMgmtdClient.h"
#include "fbs/meta/Service.h"
#include "fbs/storage/Cache.h"

namespace hf3fs::cache_manager {

class CacheManagerBackend {
 public:
  virtual ~CacheManagerBackend() = default;
  virtual CoTryTask<meta::Inode> stat(meta::InodeId inode) = 0;
  virtual CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase> items) = 0;
  virtual CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &item) = 0;
  virtual CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &object,
                                                   cache::ByteRange range) = 0;
  virtual CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &inode,
                                                               cache::CacheBlockIndex block,
                                                               const meta::CacheBlockLease &lease,
                                                               std::vector<uint8_t> data) = 0;
  virtual CoTryTask<void> commit(const meta::CacheBlockRequestBase &item,
                                 const meta::CacheBlockLease &lease,
                                 const storage::CacheChunkGenerationInfo &stored) = 0;
  virtual CoTryTask<void> fail(const cache::CacheBlockKey &key, const meta::CacheBlockLease &lease) = 0;
  virtual CoTryTask<void> validateReport(const ReportCacheBlockInvalidReq &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<void> authorizeAdmin(const flat::UserInfo &, std::optional<meta::InodeId>) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<meta::BeginCleanCacheBlockResult> beginClean(const meta::BeginCleanCacheBlockItem &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<storage::CacheChunkGenerationInfo> retire(const meta::Inode &,
                                                              cache::CacheBlockIndex,
                                                              cache::CacheGeneration) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<storage::CacheChunkGenerationInfo> query(const meta::Inode &, cache::CacheBlockIndex) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<void> finishClean(const meta::FinishCleanCacheBlockItem &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual std::shared_ptr<client::RoutingInfo> routingInfo() { return nullptr; }
  virtual CoTryTask<void> refreshRouting() { co_return makeError(StatusCode::kNotImplemented); }
  virtual CoTryTask<storage::QueryCacheSpaceRsp> queryCacheSpace(const storage::QueryCacheSpaceReq &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<storage::PermitIdentity> makePermit(const meta::Inode &,
                                                        cache::CacheBlockIndex,
                                                        uint64_t,
                                                        Uuid,
                                                        Uuid,
                                                        uint64_t) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<storage::CachePermitResult> preparePermit(const storage::PermitIdentity &, uint64_t) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<storage::CachePermitResult> renewPermit(const storage::PermitIdentity &, uint64_t) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<storage::CachePermitResult> queryPermit(const storage::PermitIdentity &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<void> releasePermit(const storage::PermitIdentity &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<meta::ListRecoverableCachePermitsRsp> listRecoverablePermits(std::optional<cache::CacheBlockKey>,
                                                                                 uint32_t) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<void> cancelQueuedAdmission(const cache::CacheBlockKey &, const storage::PermitIdentity &) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<meta::UpdateCacheBlockAccessRsp> updateAccess(std::vector<meta::UpdateCacheBlockAccessItem>) {
    co_return makeError(StatusCode::kNotImplemented);
  }
  virtual CoTryTask<meta::ListReadyCacheBlocksRsp> listReadyCacheBlocks(std::optional<cache::CacheBlockKey>, uint32_t) {
    co_return makeError(StatusCode::kNotImplemented);
  }
};

class RealCacheManagerBackend final : public CacheManagerBackend {
 public:
  using Stores = std::map<cache::OriginId, std::shared_ptr<cache::origin::ObjectStore>>;

  RealCacheManagerBackend(const Config &config,
                          std::shared_ptr<meta::client::MetaClient> metaClient,
                          std::shared_ptr<storage::client::StorageClient> storageClient,
                          std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                          Stores stores);

  CoTryTask<meta::Inode> stat(meta::InodeId inode) final;
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase> items) final;
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &item) final;
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &object, cache::ByteRange range) final;
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &inode,
                                                       cache::CacheBlockIndex block,
                                                       const meta::CacheBlockLease &lease,
                                                       std::vector<uint8_t> data) final;
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &item,
                         const meta::CacheBlockLease &lease,
                         const storage::CacheChunkGenerationInfo &stored) final;
  CoTryTask<void> fail(const cache::CacheBlockKey &key, const meta::CacheBlockLease &lease) final;
  CoTryTask<void> validateReport(const ReportCacheBlockInvalidReq &req) final;
  CoTryTask<void> authorizeAdmin(const flat::UserInfo &user, std::optional<meta::InodeId> inode) final;
  CoTryTask<meta::BeginCleanCacheBlockResult> beginClean(const meta::BeginCleanCacheBlockItem &item) final;
  CoTryTask<storage::CacheChunkGenerationInfo> retire(const meta::Inode &inode,
                                                      cache::CacheBlockIndex block,
                                                      cache::CacheGeneration generation) final;
  CoTryTask<storage::CacheChunkGenerationInfo> query(const meta::Inode &inode, cache::CacheBlockIndex block) final;
  CoTryTask<void> finishClean(const meta::FinishCleanCacheBlockItem &item) final;
  std::shared_ptr<client::RoutingInfo> routingInfo() final;
  CoTryTask<void> refreshRouting() final;
  CoTryTask<storage::QueryCacheSpaceRsp> queryCacheSpace(const storage::QueryCacheSpaceReq &req) final;
  CoTryTask<storage::PermitIdentity> makePermit(const meta::Inode &inode,
                                                cache::CacheBlockIndex block,
                                                uint64_t blockLength,
                                                Uuid managerEpoch,
                                                Uuid admissionAttemptId,
                                                uint64_t permitGeneration) final;
  CoTryTask<storage::CachePermitResult> preparePermit(const storage::PermitIdentity &permit,
                                                      uint64_t expiresAtNs) final;
  CoTryTask<storage::CachePermitResult> renewPermit(const storage::PermitIdentity &permit, uint64_t expiresAtNs) final;
  CoTryTask<storage::CachePermitResult> queryPermit(const storage::PermitIdentity &permit) final;
  CoTryTask<void> releasePermit(const storage::PermitIdentity &permit) final;
  CoTryTask<meta::ListRecoverableCachePermitsRsp> listRecoverablePermits(std::optional<cache::CacheBlockKey> after,
                                                                         uint32_t limit) final;
  CoTryTask<void> cancelQueuedAdmission(const cache::CacheBlockKey &key,
                                        const storage::PermitIdentity &expectedPermit) final;
  CoTryTask<meta::UpdateCacheBlockAccessRsp> updateAccess(std::vector<meta::UpdateCacheBlockAccessItem> items) final;
  CoTryTask<meta::ListReadyCacheBlocksRsp> listReadyCacheBlocks(std::optional<cache::CacheBlockKey> after,
                                                                uint32_t limit) final;

 private:
  meta::CacheServiceIdentity service() const;
  Result<storage::CacheChunkKey> storageKey(const meta::Inode &inode, cache::CacheBlockIndex block) const;

  const Config &config_;
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  std::shared_ptr<storage::client::StorageClient> storageClient_;
  std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient_;
  Stores stores_;
};

class CacheLoader {
 public:
  using WallClockNsFn = std::function<uint64_t()>;

  CacheLoader(std::shared_ptr<CacheManagerBackend> backend,
              CapacityGate &capacityGate,
              Duration permitTtl = 0_ns,
              WallClockNsFn wallClockNs = {})
      : backend_(std::move(backend)),
        capacityGate_(capacityGate),
        permitTtl_(permitTtl),
        wallClockNs_(std::move(wallClockNs)) {}

  CoTryTask<void> load(const LoadHint &hint);
  CoTryTask<void> loadBatch(std::vector<LoadHint> hints);
  static Result<std::vector<cache::ByteRange>> mergeRanges(std::span<const LoadHint> hints, uint64_t blockSize);

 private:
  CoTryTask<void> fail(const cache::CacheBlockKey &key, const meta::CacheBlockLease &lease, Status error);

  std::shared_ptr<CacheManagerBackend> backend_;
  CapacityGate &capacityGate_;
  Duration permitTtl_;
  WallClockNsFn wallClockNs_;
};

}  // namespace hf3fs::cache_manager
