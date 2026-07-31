#include "cache_manager/loader/CacheLoader.h"

#include <algorithm>
#include <limits>

namespace hf3fs::cache_manager {
namespace {

uint64_t loaderWallClockNs() { return static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000; }

}  // namespace

RealCacheManagerBackend::RealCacheManagerBackend(const Config &config,
                                                 std::shared_ptr<meta::client::MetaClient> metaClient,
                                                 std::shared_ptr<storage::client::StorageClient> storageClient,
                                                 std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
                                                 Stores stores)
    : config_(config),
      metaClient_(std::move(metaClient)),
      storageClient_(std::move(storageClient)),
      mgmtdClient_(std::move(mgmtdClient)),
      stores_(std::move(stores)) {}

meta::CacheServiceIdentity RealCacheManagerBackend::service() const {
  return {config_.service_name(), config_.service_token()};
}

CoTryTask<meta::Inode> RealCacheManagerBackend::stat(meta::InodeId inode) {
  co_return co_await metaClient_->stat(flat::UserInfo{}, inode, std::nullopt, false);
}

std::shared_ptr<client::RoutingInfo> RealCacheManagerBackend::routingInfo() { return mgmtdClient_->getRoutingInfo(); }

CoTryTask<storage::QueryCacheSpaceRsp> RealCacheManagerBackend::queryCacheSpace(
    const storage::QueryCacheSpaceReq &req) {
  co_return co_await storageClient_->queryCacheSpace(req);
}

CoTryTask<storage::PermitIdentity> RealCacheManagerBackend::makePermit(const meta::Inode &inode,
                                                                       cache::CacheBlockIndex block,
                                                                       uint64_t blockLength,
                                                                       Uuid managerEpoch,
                                                                       Uuid admissionAttemptId,
                                                                       uint64_t permitGeneration) {
  auto routing = mgmtdClient_->getRoutingInfo();
  if (!routing || !routing->raw()) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  auto offset = uint64_t{block.toUnderType()} * inode.fileLayout().chunkSize;
  auto chainId = inode.getChainId(inode, offset, *routing->raw());
  CO_RETURN_ON_ERROR(chainId);
  auto chain = routing->getChain(*chainId);
  if (!chain || chain->targets.empty()) co_return makeError(CacheCode::kUnavailable, "cache chain is unavailable");
  std::vector<flat::TargetId> targets;
  targets.reserve(chain->targets.size());
  for (const auto &target : chain->targets) targets.push_back(target.targetId);
  auto placement = storage::PlacementIdentity::create({*chainId, chain->chainVersion},
                                                      targets,
                                                      *std::min_element(targets.begin(), targets.end()),
                                                      admissionAttemptId);
  CO_RETURN_ON_ERROR(placement);

  storage::QueryCacheSpaceReq footprintRequest;
  for (auto targetId : placement->expectedReplicaTargets) {
    footprintRequest.footprints.push_back({targetId, static_cast<uint32_t>(inode.fileLayout().chunkSize), blockLength});
  }
  footprintRequest.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto footprintResponse = co_await storageClient_->queryCacheSpace(footprintRequest);
  CO_RETURN_ON_ERROR(footprintResponse);
  if (footprintResponse->footprintResults.size() != placement->expectedReplicaTargets.size()) {
    co_return makeError(CacheCode::kInvalidResponse, "cache footprint result count mismatch");
  }
  storage::FootprintByTarget footprints;
  for (size_t index = 0; index < footprintResponse->footprintResults.size(); ++index) {
    const auto &result = footprintResponse->footprintResults[index];
    CO_RETURN_ON_ERROR(result);
    auto expectedTarget = placement->expectedReplicaTargets[index];
    if (result->targetId != expectedTarget || result->footprintBytes == 0) {
      co_return makeError(CacheCode::kInvalidResponse, "cache footprint result target mismatch");
    }
    footprints.emplace(expectedTarget, result->footprintBytes);
  }
  storage::PermitIdentity permit{managerEpoch, std::move(*placement), permitGeneration, std::move(footprints)};
  CO_RETURN_ON_ERROR(permit.valid());
  co_return permit;
}

CoTryTask<storage::CachePermitResult> RealCacheManagerBackend::preparePermit(const storage::PermitIdentity &permit,
                                                                             uint64_t expiresAtNs) {
  storage::PrepareCachePermitsReq request;
  request.items.push_back({permit, expiresAtNs});
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto response = co_await storageClient_->prepareCachePermits(request);
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != 1)
    co_return makeError(CacheCode::kInvalidResponse, "invalid permit prepare response");
  CO_RETURN_ON_ERROR(response->results.front());
  co_return *response->results.front();
}

CoTryTask<storage::CachePermitResult> RealCacheManagerBackend::renewPermit(const storage::PermitIdentity &permit,
                                                                           uint64_t expiresAtNs) {
  storage::RenewCachePermitsReq request;
  request.items.push_back({permit, expiresAtNs});
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto response = co_await storageClient_->renewCachePermits(request);
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid permit renew response");
  CO_RETURN_ON_ERROR(response->results.front());
  co_return *response->results.front();
}

CoTryTask<storage::CachePermitResult> RealCacheManagerBackend::queryPermit(const storage::PermitIdentity &permit) {
  storage::QueryCachePermitsReq request;
  request.permits.push_back(permit);
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto response = co_await storageClient_->queryCachePermits(request);
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid permit query response");
  CO_RETURN_ON_ERROR(response->results.front());
  co_return *response->results.front();
}

CoTryTask<void> RealCacheManagerBackend::releasePermit(const storage::PermitIdentity &permit) {
  storage::ReleaseCachePermitsReq request;
  request.permits.push_back(permit);
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto response = co_await storageClient_->releaseCachePermits(request);
  CO_RETURN_ON_ERROR(response);
  if (response->results.size() != 1)
    co_return makeError(CacheCode::kInvalidResponse, "invalid permit release response");
  CO_RETURN_ON_ERROR(response->results.front());
  co_return Void{};
}

CoTryTask<meta::EnqueueCacheBlocksRsp> RealCacheManagerBackend::enqueue(
    std::vector<meta::CacheBlockRequestBase> items) {
  meta::EnqueueCacheBlocksReq req;
  req.service = service();
  req.items = std::move(items);
  co_return co_await metaClient_->enqueueCacheBlocks(std::move(req));
}

CoTryTask<meta::CacheBlockLease> RealCacheManagerBackend::acquire(const meta::CacheBlockRequestBase &item) {
  meta::AcquireCacheBlocksReq req;
  req.service = service();
  req.items.push_back(item);
  auto result = co_await metaClient_->acquireCacheBlocks(std::move(req));
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid acquire result count");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return result->results.front()->lease;
}

CoTryTask<std::vector<uint8_t>> RealCacheManagerBackend::getRange(const cache::ImmutableObjectIdentity &object,
                                                                  cache::ByteRange range) {
  auto store = stores_.find(object.originId);
  if (store == stores_.end()) co_return makeError(StatusCode::kInvalidConfig, "origin is not configured");
  co_return co_await store->second->getRange(object, range);
}

CoTryTask<storage::CacheChunkGenerationInfo> RealCacheManagerBackend::replace(const meta::Inode &inode,
                                                                              cache::CacheBlockIndex block,
                                                                              const meta::CacheBlockLease &lease,
                                                                              std::vector<uint8_t> data) {
  auto routing = mgmtdClient_->getRoutingInfo();
  if (!routing || !routing->raw()) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  const auto blockSize = inode.fileLayout().chunkSize;
  if (block.toUnderType() > std::numeric_limits<uint64_t>::max() / blockSize) {
    co_return makeError(StatusCode::kInvalidArg, "cache block offset overflow");
  }
  auto offset = uint64_t{block.toUnderType()} * blockSize;
  auto chunkId = inode.getChunkId(inode.id, offset);
  CO_RETURN_ON_ERROR(chunkId);
  auto chainId = inode.getChainId(inode, offset, *routing->raw());
  CO_RETURN_ON_ERROR(chainId);
  auto chain = routing->getChain(*chainId);
  if (!chain) co_return makeError(CacheCode::kUnavailable, "cache chain is unavailable");
  auto table = routing->raw()->getChainTable(inode.fileLayout().tableId);
  if (!table || table->checksumType != flat::ChainTableChecksumType::CRC32C) {
    co_return makeError(CacheCode::kFeatureDisabled, "cache loader requires a CRC32C CACHE_DATA table");
  }

  storage::ReplaceCacheChunksReq req;
  req.userInfo = flat::UserInfo{};
  storage::ReplaceCacheChunkItem item;
  item.key = {{*chainId, chain->chainVersion}, storage::ChunkId(chunkId->pack())};
  item.cacheGeneration = lease.cacheGeneration;
  item.operationId = Uuid::random();
  item.data = std::move(data);
  item.chunkSize = blockSize;
  item.checksumType = storage::ChecksumType::CRC32C;
  if (lease.permit) {
    item.logicalKey = cache::CacheBlockKey{inode.id.u64(), block};
    item.permit = lease.permit;
    req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  }
  req.items.push_back(std::move(item));
  auto result = co_await storageClient_->replaceCacheChunks(req);
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid replace result count");
  if (lease.permit && result->descriptors.size() != 1)
    co_return makeError(CacheCode::kInvalidResponse, "invalid replace descriptor count");
  CO_RETURN_ON_ERROR(result->results.front());
  const auto &stored = *result->results.front();
  if (stored.cacheGeneration != lease.cacheGeneration || stored.retired || stored.length != req.items[0].data.size()) {
    co_return makeError(CacheCode::kInvalidResponse, "storage returned an unexpected cache generation");
  }
  if (lease.permit) {
    const auto &descriptor = result->descriptors.front();
    if (!descriptor || descriptor->logicalKey != *req.items[0].logicalKey ||
        descriptor->generation != lease.cacheGeneration || descriptor->placement != lease.permit->placement) {
      co_return makeError(CacheCode::kPlacementMismatch, "storage returned a mismatched cache descriptor");
    }
  }
  co_return stored;
}

CoTryTask<void> RealCacheManagerBackend::commit(const meta::CacheBlockRequestBase &item,
                                                const meta::CacheBlockLease &lease,
                                                const storage::CacheChunkGenerationInfo &stored) {
  meta::CommitCacheBlocksReq req;
  req.service = service();
  meta::CommitCacheBlockItem commit;
  commit.key = item.key;
  commit.loaderId = lease.loaderId;
  commit.loadEpoch = lease.loadEpoch;
  commit.cacheGeneration = lease.cacheGeneration;
  commit.blockLength = item.blockLength;
  commit.checksumType = static_cast<uint8_t>(stored.checksum.type);
  commit.checksumValue = stored.checksum.value;
  commit.permit = lease.permit;
  if (lease.permit) commit.placement = lease.permit->placement;
  req.items.push_back(std::move(commit));
  auto result = co_await metaClient_->commitCacheBlocks(std::move(req));
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid commit result count");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return Void{};
}

CoTryTask<void> RealCacheManagerBackend::fail(const cache::CacheBlockKey &key, const meta::CacheBlockLease &lease) {
  meta::FailCacheBlocksReq req;
  req.service = service();
  req.items.push_back({key, lease.loaderId, lease.loadEpoch});
  auto result = co_await metaClient_->failCacheBlocks(std::move(req));
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid fail result count");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return Void{};
}

Result<storage::CacheChunkKey> RealCacheManagerBackend::storageKey(const meta::Inode &inode,
                                                                   cache::CacheBlockIndex block) const {
  auto routing = mgmtdClient_->getRoutingInfo();
  if (!routing || !routing->raw()) return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  auto offset = uint64_t{block.toUnderType()} * inode.fileLayout().chunkSize;
  auto chunkId = inode.getChunkId(inode.id, offset);
  RETURN_ON_ERROR(chunkId);
  auto chainId = inode.getChainId(inode, offset, *routing->raw());
  RETURN_ON_ERROR(chainId);
  auto chain = routing->getChain(*chainId);
  if (!chain) return makeError(CacheCode::kUnavailable, "cache chain is unavailable");
  return storage::CacheChunkKey{{*chainId, chain->chainVersion}, storage::ChunkId(chunkId->pack())};
}

CoTryTask<void> RealCacheManagerBackend::validateReport(const ReportCacheBlockInvalidReq &req) {
  auto inode = co_await stat(req.inode);
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile);
  auto offset = uint64_t{req.block.toUnderType()} * inode->fileLayout().chunkSize;
  meta::GetFileReadPlanReq plan;
  plan.user = req.user;
  plan.openSessionId = req.openSessionId;
  plan.inode = req.inode;
  plan.offset = offset;
  plan.length = 1;
  plan.cacheProtocolVersion = req.cacheProtocolVersion;
  auto result = co_await metaClient_->getFileReadPlan(std::move(plan));
  CO_RETURN_ON_ERROR(result);
  if (result->blocks.size() != 1 || result->blocks.front().key != cache::CacheBlockKey{req.inode.u64(), req.block} ||
      result->blocks.front().ready != req.expectedReady) {
    co_return makeError(CacheCode::kStateConflict, "reported cache READY identity changed");
  }
  co_return Void{};
}

CoTryTask<void> RealCacheManagerBackend::authorizeAdmin(const flat::UserInfo &user,
                                                        std::optional<meta::InodeId> inode) {
  meta::GetCacheStatusReq req;
  req.user = user;
  req.inode = inode;
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await metaClient_->getCacheStatus(std::move(req));
  CO_RETURN_ON_ERROR(result);
  co_return Void{};
}

CoTryTask<meta::BeginCleanCacheBlockResult> RealCacheManagerBackend::beginClean(
    const meta::BeginCleanCacheBlockItem &item) {
  meta::BeginCleanCacheBlocksReq req;
  req.service = service();
  req.items.push_back(item);
  auto result = co_await metaClient_->beginCleanCacheBlocks(std::move(req));
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid begin clean result");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return *result->results.front();
}

CoTryTask<storage::CacheChunkGenerationInfo> RealCacheManagerBackend::retire(const meta::Inode &inode,
                                                                             cache::CacheBlockIndex block,
                                                                             cache::CacheGeneration generation) {
  auto key = storageKey(inode, block);
  CO_RETURN_ON_ERROR(key);
  storage::RetireCacheChunkGenerationsReq req;
  req.userInfo = flat::UserInfo{};
  req.items.push_back({*key, generation, Uuid::random()});
  auto result = co_await storageClient_->retireCacheChunkGenerations(req);
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid retire result");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return *result->results.front();
}

CoTryTask<storage::CacheChunkGenerationInfo> RealCacheManagerBackend::query(const meta::Inode &inode,
                                                                            cache::CacheBlockIndex block) {
  auto key = storageKey(inode, block);
  CO_RETURN_ON_ERROR(key);
  storage::QueryCacheChunkGenerationsReq req;
  req.userInfo = flat::UserInfo{};
  req.keys.push_back(*key);
  auto result = co_await storageClient_->queryCacheChunkGenerations(req);
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid query result");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return *result->results.front();
}

CoTryTask<void> RealCacheManagerBackend::finishClean(const meta::FinishCleanCacheBlockItem &item) {
  meta::FinishCleanCacheBlocksReq req;
  req.service = service();
  req.items.push_back(item);
  auto result = co_await metaClient_->finishCleanCacheBlocks(std::move(req));
  CO_RETURN_ON_ERROR(result);
  if (result->results.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid finish clean result");
  CO_RETURN_ON_ERROR(result->results.front());
  co_return Void{};
}

Result<std::vector<cache::ByteRange>> CacheLoader::mergeRanges(std::span<const LoadHint> hints, uint64_t blockSize) {
  if (blockSize == 0) return makeError(StatusCode::kInvalidArg, "block size is zero");
  std::vector<cache::ByteRange> ranges;
  ranges.reserve(hints.size());
  for (const auto &hint : hints) {
    if (hint.blockLength == 0 || hint.block.toUnderType() > std::numeric_limits<uint64_t>::max() / blockSize) {
      return makeError(StatusCode::kInvalidArg, "invalid cache load hint");
    }
    ranges.push_back({uint64_t{hint.block.toUnderType()} * blockSize, hint.blockLength});
  }
  std::sort(ranges.begin(), ranges.end(), [](const auto &lhs, const auto &rhs) { return lhs.offset < rhs.offset; });
  std::vector<cache::ByteRange> merged;
  for (const auto &range : ranges) {
    auto end = range.end();
    RETURN_ON_ERROR(end);
    if (merged.empty()) {
      merged.push_back(range);
      continue;
    }
    auto mergedEnd = merged.back().end();
    RETURN_ON_ERROR(mergedEnd);
    if (*mergedEnd == range.offset) {
      merged.back().length += range.length;
    } else {
      merged.push_back(range);
    }
  }
  return merged;
}

CoTryTask<void> CacheLoader::fail(const cache::CacheBlockKey &key, const meta::CacheBlockLease &lease, Status error) {
  (void)co_await backend_->fail(key, lease);
  if (lease.permit) (void)co_await backend_->releasePermit(*lease.permit);
  co_return makeError(std::move(error));
}

CoTryTask<void> CacheLoader::load(const LoadHint &hint) { co_return co_await loadBatch({hint}); }

CoTryTask<void> CacheLoader::loadBatch(std::vector<LoadHint> hints) {
  if (hints.empty()) co_return Void{};
  std::sort(hints.begin(), hints.end(), [](const auto &lhs, const auto &rhs) { return lhs.block < rhs.block; });
  if (std::any_of(hints.begin(), hints.end(), [&](const auto &hint) { return hint.inode != hints.front().inode; })) {
    co_return makeError(StatusCode::kInvalidArg, "cache load batch spans multiple inodes");
  }
  auto inode = co_await backend_->stat(hints.front().inode);
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile, "cache hint inode is not an OriginFile");
  const auto &origin = inode->asOriginFile();
  if (origin.superseded || origin.cacheAdmissionDisabled) {
    co_return makeError(CacheCode::kStateConflict, "OriginFile cache admission is disabled");
  }
  auto object = origin.object;
  auto blockSize = uint64_t{inode->fileLayout().chunkSize};
  struct Work {
    LoadHint hint;
    meta::CacheBlockRequestBase item;
    std::optional<meta::CacheBlockLease> lease;
    std::optional<storage::CacheChunkGenerationInfo> stored;
  };
  std::vector<Work> work;
  work.reserve(hints.size());
  std::optional<Status> firstError;
  auto remember = [&](const Status &error) {
    if (!firstError) firstError = error;
  };

  for (auto &hint : hints) {
    auto offset = uint64_t{hint.block.toUnderType()} * blockSize;
    if (offset >= inode->fileLength()) {
      remember(Status(StatusCode::kInvalidArg, "cache block is beyond EOF"));
      continue;
    }
    auto blockLength = std::min(blockSize, inode->fileLength() - offset);
    if (hint.blockLength != blockLength) {
      remember(Status(CacheCode::kStateConflict, "cache block length changed"));
      continue;
    }
    Work item{hint, {hint.key(), blockLength}, std::nullopt, std::nullopt};
    auto lease = co_await backend_->acquire(item.item);
    if (lease.hasError()) {
      remember(lease.error());
    } else {
      item.lease = std::move(*lease);
    }
    work.push_back(std::move(item));
  }

  for (size_t begin = 0; begin < work.size();) {
    while (begin < work.size() && !work[begin].lease) ++begin;
    if (begin == work.size()) break;
    size_t end = begin + 1;
    uint64_t rangeLength = work[begin].item.blockLength;
    while (end < work.size() && work[end].lease &&
           work[end].hint.block.toUnderType() == work[end - 1].hint.block.toUnderType() + 1) {
      rangeLength += work[end].item.blockLength;
      ++end;
    }
    auto offset = uint64_t{work[begin].hint.block.toUnderType()} * blockSize;
    auto permit = capacityGate_.tryAcquire(object.originId, rangeLength);
    while (permit.hasError() && permit.error().code() == CacheCode::kCapacityExceeded && end > begin + 1) {
      --end;
      rangeLength -= work[end].item.blockLength;
      permit = capacityGate_.tryAcquire(object.originId, rangeLength);
    }
    if (permit.hasError()) {
      remember(permit.error());
      for (size_t i = begin; i < end; ++i) (void)co_await fail(work[i].item.key, *work[i].lease, permit.error());
      begin = end;
      continue;
    }
    auto data = co_await backend_->getRange(object, {offset, rangeLength});
    if (data.hasError() || data->size() != rangeLength) {
      auto error = data.hasError() ? data.error() : Status(CacheCode::kInvalidResponse, "origin range length mismatch");
      remember(error);
      for (size_t i = begin; i < end; ++i) (void)co_await fail(work[i].item.key, *work[i].lease, error);
      begin = end;
      continue;
    }
    size_t dataOffset = 0;
    for (size_t i = begin; i < end; ++i) {
      auto length = work[i].item.blockLength;
      std::vector<uint8_t> blockData(data->begin() + dataOffset, data->begin() + dataOffset + length);
      dataOffset += length;
      if (work[i].lease->permit) {
        auto nowNs = wallClockNs_ ? wallClockNs_() : loaderWallClockNs();
        auto ttlNs = permitTtl_.count();
        if (ttlNs <= 0 || nowNs > std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(ttlNs)) {
          auto error = Status(CacheCode::kPermitExpired, "invalid cache permit renewal deadline");
          remember(error);
          (void)co_await fail(work[i].item.key, *work[i].lease, error);
          continue;
        }
        auto renewed = co_await backend_->renewPermit(*work[i].lease->permit, nowNs + static_cast<uint64_t>(ttlNs));
        if (renewed.hasError() || renewed->state != cache::CachePermitState::RESERVED ||
            renewed->expiresAtNs <= nowNs) {
          auto error = renewed.hasError()
                           ? renewed.error()
                           : Status(CacheCode::kPermitExpired, "cache permit is not reserved and unexpired");
          remember(error);
          (void)co_await fail(work[i].item.key, *work[i].lease, error);
          continue;
        }
      }
      auto stored = co_await backend_->replace(*inode, work[i].hint.block, *work[i].lease, std::move(blockData));
      if (stored.hasError()) {
        remember(stored.error());
        (void)co_await fail(work[i].item.key, *work[i].lease, stored.error());
      } else {
        work[i].stored = std::move(*stored);
      }
    }
    begin = end;
  }

  auto hasStored = std::any_of(work.begin(), work.end(), [](const auto &item) { return item.stored.has_value(); });
  if (hasStored) {
    auto current = co_await backend_->stat(hints.front().inode);
    auto currentError = current.hasError() || !current->isOriginFile() || current->asOriginFile().object != object ||
                        current->asOriginFile().superseded || current->asOriginFile().cacheAdmissionDisabled;
    if (currentError) {
      auto error = current.hasError() ? current.error()
                                      : Status(CacheCode::kVersionMismatch, "OriginFile changed while loading");
      remember(error);
      for (auto &item : work) {
        if (item.stored) (void)co_await fail(item.item.key, *item.lease, error);
      }
    } else {
      for (auto &item : work) {
        if (!item.stored) continue;
        auto committed = co_await backend_->commit(item.item, *item.lease, *item.stored);
        if (committed.hasError()) {
          remember(committed.error());
          (void)co_await fail(item.item.key, *item.lease, committed.error());
        }
      }
    }
  }
  if (firstError) co_return makeError(std::move(*firstError));
  co_return Void{};
}

}  // namespace hf3fs::cache_manager
