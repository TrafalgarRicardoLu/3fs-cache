#include "ChunkEngine.h"

#include <cstring>
#include <folly/ScopeGuard.h>

#include "chunk_engine/src/cxx.rs.h"
#include "fbs/storage/Common.h"
#include "storage/update/UpdateJob.h"

namespace hf3fs::storage {

Result<std::vector<LocalEvictionCandidate>> ChunkEngine::listActiveCacheChunks(chunk_engine::Engine &engine,
                                                                               TargetId targetId) {
  std::string error;
  auto chunks = engine.query_all_raw_chunks({}, error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));
  std::vector<LocalEvictionCandidate> result;
  for (size_t i = 0; i < chunks->len(); ++i) {
    auto rawId = chunks->chunk_id(i);
    if (rawId.length() <= sizeof(ChainId)) continue;
    auto tag = decodeCacheTag(chunks->chunk_etag(i));
    if (!tag || tag->state != CacheChunkState::ACTIVE || !tag->descriptor || tag->descriptor->targetId != targetId ||
        tag->descriptor->generation != tag->generation) {
      continue;
    }
    auto chunkId = ChunkId(std::string_view{reinterpret_cast<const char *>(rawId.data()) + sizeof(ChainId),
                                            rawId.length() - sizeof(ChainId)});
    auto length = chunks->chunk_meta(i).len;
    auto footprint = std::max<uint64_t>(std::bit_ceil(uint64_t{length}), 64_KB);
    result.push_back({std::move(chunkId), std::move(*tag->descriptor), footprint});
  }
  return result;
}

Result<std::vector<CacheInventoryEntry>> ChunkEngine::listCacheInventory(chunk_engine::Engine &engine,
                                                                         TargetId targetId,
                                                                         const PhysicalDiskId &physicalDiskId) {
  std::string error;
  auto chunks = engine.query_all_raw_chunks({}, error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));
  std::vector<CacheInventoryEntry> result;
  for (size_t i = 0; i < chunks->len(); ++i) {
    auto rawId = chunks->chunk_id(i);
    if (rawId.length() <= sizeof(ChainId)) continue;
    auto tag = decodeCacheTag(chunks->chunk_etag(i));
    if (!tag || tag->state != CacheChunkState::ACTIVE || !tag->descriptor || tag->descriptor->targetId != targetId ||
        tag->descriptor->generation != tag->generation) {
      continue;
    }
    ChainId chainId;
    std::memcpy(&chainId, rawId.data(), sizeof(chainId));
    if (chainId != tag->descriptor->placement.versionedChain.chainId) {
      return makeError(CacheCode::kPlacementMismatch, "cache descriptor chain differs from physical chunk key");
    }
    auto chunkId = ChunkId(std::string_view{reinterpret_cast<const char *>(rawId.data()) + sizeof(chainId),
                                            rawId.length() - sizeof(chainId)});
    const auto &meta = chunks->chunk_meta(i);
    CacheInventoryEntry entry{targetId,
                              physicalDiskId,
                              {tag->descriptor->placement.versionedChain, std::move(chunkId)},
                              std::move(*tag->descriptor),
                              {tag->generation, false, meta.len, ChecksumInfo{ChecksumType::CRC32C, ~meta.checksum}}};
    RETURN_ON_ERROR(entry.valid());
    result.push_back(std::move(entry));
  }
  return result;
}
namespace {

monitor::OperationRecorder storageUpdateRecorder{"storage.engine_update"};
monitor::OperationRecorder storageCommitRecorder{"storage.engine_commit"};

std::string cacheChunkKey(ChainId chainId, const ChunkId &chunkId) {
  std::string key;
  key.reserve(sizeof(chainId) + chunkId.data().size());
  key.append(reinterpret_cast<const char *>(&chainId), sizeof(chainId));
  key.append(chunkId.data());
  return key;
}

}  // namespace

std::string ChunkEngine::encodeCacheTag(CacheTag tag) {
  std::string encoded{kCacheTagMagic};
  encoded.push_back(static_cast<char>(tag.state));
  auto generation = tag.generation.toUnderType();
  encoded.append(reinterpret_cast<const char *>(&generation), sizeof(generation));
  encoded.append(tag.operationId.asStringView());
  if (tag.descriptor) encoded.append(serde::serialize(*tag.descriptor));
  return encoded;
}

std::optional<ChunkEngine::CacheTag> ChunkEngine::decodeCacheTag(rust::Slice<const uint8_t> tag) {
  constexpr auto expectedSize = kCacheTagMagic.size() + 1 + sizeof(uint64_t) + sizeof(Uuid);
  if (tag.size() < expectedSize ||
      std::string_view{reinterpret_cast<const char *>(tag.data()), kCacheTagMagic.size()} != kCacheTagMagic) {
    return std::nullopt;
  }
  CacheTag decoded;
  decoded.state = static_cast<CacheChunkState>(tag[kCacheTagMagic.size()]);
  uint64_t generation;
  std::memcpy(&generation, tag.data() + kCacheTagMagic.size() + 1, sizeof(generation));
  decoded.generation = cache::CacheGeneration{generation};
  std::memcpy(decoded.operationId.data,
              tag.data() + kCacheTagMagic.size() + 1 + sizeof(generation),
              sizeof(decoded.operationId.data));
  if (tag.size() > expectedSize) {
    CacheChunkDescriptor descriptor;
    auto serialized =
        std::string_view{reinterpret_cast<const char *>(tag.data() + expectedSize), tag.size() - expectedSize};
    if (serde::deserialize(descriptor, serialized).hasError() || descriptor.valid().hasError()) return std::nullopt;
    decoded.descriptor = std::move(descriptor);
  }
  return decoded;
}

Result<uint32_t> ChunkEngine::update(chunk_engine::Engine &engine, UpdateJob &job) {
  auto recordGuard = storageUpdateRecorder.record();

  // 1. prepare.
  const auto &updateIO = job.updateIO();
  const auto &chunkId = updateIO.key.chunkId;
  const auto &options = job.options();
  const auto &state = job.state();
  auto &result = job.result();

  auto chainId = updateIO.key.vChainId.chainId;
  std::string key;
  key.reserve(sizeof(chainId) + chunkId.data().size());
  key.append((const char *)&chainId, sizeof(chainId));
  key.append(chunkId.data());

  // 2. start update.
  chunk_engine::UpdateReq req{};
  if (updateIO.isTruncate()) {
    req.is_truncate = true;
  } else if (updateIO.isRemove()) {
    req.is_remove = true;
  }
  req.is_syncing = options.isSyncing;
  req.update_ver = updateIO.updateVer;
  req.chain_ver = job.commitChainVer();
  if (updateIO.checksum.type == ChecksumType::CRC32C) {
    req.checksum = ~updateIO.checksum.value;
  } else if (state.data) {
    req.without_checksum = true;
  }
  if (updateIO.isWrite()) {
    req.length = updateIO.length;
    req.offset = updateIO.offset;
  } else {
    req.length = 0;
    req.offset = updateIO.length;
  }
  req.data = reinterpret_cast<uint64_t>(state.data);
  req.last_request_id = job.requestCtx().tag.requestId;
  auto clientId = job.requestCtx().tag.clientId.uuid.asStringView();
  req.last_client_low = *(const uint64_t *)clientId.data();
  req.last_client_high = *(const uint64_t *)(clientId.data() + 8);

  std::string error{};
  auto chunk = engine.update_raw_chunk(toSlice(key), req, error);
  result.updateVer = result.commitVer = ChunkVer{req.out_commit_ver};
  result.commitChainVer = ChainVer{req.out_chain_ver};
  if (req.is_remove && req.out_non_existent) {
    result.checksum = ChecksumInfo{ChecksumType::NONE, 0};
  } else {
    result.checksum = ChecksumInfo{ChecksumType::CRC32C, ~req.out_checksum};
  }

  if (UNLIKELY(!error.empty())) {
    return makeError(req.out_error_code, std::move(error));
  }

  job.chunkEngineJob().set(engine, chunk);

  recordGuard.succ();
  if (updateIO.isTruncate() || updateIO.isExtend()) {
    return chunk->raw_meta().len;
  }
  return updateIO.length;
}

Result<uint32_t> ChunkEngine::commit(chunk_engine::Engine &engine, UpdateJob &job, bool sync) {
  auto recordGuard = storageCommitRecorder.record();

  const auto &commitIO = job.commitIO();
  const auto &chunkId = commitIO.key.chunkId;
  auto &result = job.result();

  auto chainId = commitIO.key.vChainId.chainId;
  std::string key;
  key.reserve(sizeof(chainId) + chunkId.data().size());
  key.append((const char *)&chainId, sizeof(chainId));
  key.append(chunkId.data());

  auto chunk = job.chunkEngineJob().chunk();
  chunk->set_chain_ver(job.commitChainVer());
  auto &meta = chunk->raw_meta();
  result.updateVer = result.commitVer = ChunkVer{meta.chunk_ver};
  result.commitChainVer = ChainVer{meta.chain_ver};

  std::string error;
  engine.commit_raw_chunk(chunk, sync, error);
  job.chunkEngineJob().release();
  if (UNLIKELY(!error.empty())) {
    return makeError(StorageCode::kChunkMetadataSetError, std::move(error));
  }

  recordGuard.succ();
  return uint32_t{};
}

Result<CacheChunkGenerationInfo> ChunkEngine::replaceCacheChunk(chunk_engine::Engine &engine,
                                                                const ReplaceCacheChunkItem &item,
                                                                ChainId chainId,
                                                                bool sync) {
  if (item.checksumType != ChecksumType::CRC32C) {
    return makeError(StatusCode::kInvalidArg, "chunk engine cache replacement requires CRC32C");
  }
  auto key = cacheChunkKey(chainId, item.key.chunkId);
  std::string error;
  auto current = engine.get_raw_chunk(toSlice(key), error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));

  uint32_t currentVersion = 0;
  if (current != nullptr) {
    auto release = folly::makeGuard([&] { engine.release_raw_chunk(current); });
    const auto &meta = current->raw_meta();
    currentVersion = meta.chunk_ver;
    auto tag = decodeCacheTag(current->raw_etag());
    if (!tag) return makeError(CacheCode::kStateConflict, "chunk has no cache generation tag");
    if (tag->generation == item.cacheGeneration) {
      auto checksum = ChecksumInfo::create(item.checksumType, item.data.data(), item.data.size());
      if (tag->state == CacheChunkState::ACTIVE && tag->operationId == item.operationId &&
          tag->descriptor == item.descriptor && meta.len == item.data.size() &&
          ChecksumInfo{ChecksumType::CRC32C, ~meta.checksum} == checksum) {
        return CacheChunkGenerationInfo{tag->generation, false, meta.len, checksum};
      }
      return makeError(tag->state == CacheChunkState::RETIRED ? CacheCode::kStaleGeneration
                                                              : CacheCode::kStateConflict);
    }
    if (tag->generation > item.cacheGeneration) return makeError(CacheCode::kStaleGeneration);
  }

  auto tag = encodeCacheTag(CacheTag{CacheChunkState::ACTIVE, item.cacheGeneration, item.operationId, item.descriptor});
  auto checksum = ChecksumInfo::create(item.checksumType, item.data.data(), item.data.size());
  chunk_engine::UpdateReq request{};
  request.is_syncing = true;
  request.update_ver = std::max(currentVersion + 1, 1u);
  request.chain_ver = chainId == item.key.vChainId.chainId ? item.key.vChainId.chainVer.toUnderType() : 0;
  request.checksum = ~checksum.value;
  request.length = item.data.size();
  request.data = reinterpret_cast<uint64_t>(item.data.data());
  request.desired_tag = toSlice(tag);

  auto writing = engine.update_raw_chunk(toSlice(key), request, error);
  if (!error.empty()) return makeError(request.out_error_code, std::move(error));
  engine.commit_raw_chunk(writing, sync, error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataSetError, std::move(error));
  return CacheChunkGenerationInfo{item.cacheGeneration, false, item.data.size(), checksum};
}

Result<CacheChunkGenerationInfo> ChunkEngine::retireCacheChunk(chunk_engine::Engine &engine,
                                                               const RetireCacheChunkItem &item,
                                                               ChainId chainId,
                                                               bool sync) {
  auto key = cacheChunkKey(chainId, item.key.chunkId);
  std::string error;
  auto current = engine.get_raw_chunk(toSlice(key), error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));

  uint32_t currentVersion = 0;
  std::optional<CacheChunkDescriptor> descriptor;
  if (current != nullptr) {
    auto release = folly::makeGuard([&] { engine.release_raw_chunk(current); });
    currentVersion = current->raw_meta().chunk_ver;
    auto tag = decodeCacheTag(current->raw_etag());
    if (!tag) return makeError(CacheCode::kStateConflict, "chunk has no cache generation tag");
    descriptor = tag->descriptor;
    if (tag->generation > item.expectedGeneration) return makeError(CacheCode::kGenerationAdvanced);
    if (tag->generation == item.expectedGeneration && tag->state == CacheChunkState::RETIRED) {
      return CacheChunkGenerationInfo{tag->generation, true, 0, ChecksumInfo{}};
    }
  }

  auto tag = encodeCacheTag(CacheTag{CacheChunkState::RETIRED, item.expectedGeneration, item.operationId, descriptor});
  static const uint8_t tombstoneByte = 0;
  auto checksum = ChecksumInfo::create(ChecksumType::CRC32C, &tombstoneByte, 1);
  chunk_engine::UpdateReq request{};
  request.is_syncing = true;
  request.update_ver = std::max(currentVersion + 1, 1u);
  request.chain_ver = chainId == item.key.vChainId.chainId ? item.key.vChainId.chainVer.toUnderType() : 0;
  request.checksum = ~checksum.value;
  request.length = 1;
  request.data = reinterpret_cast<uint64_t>(&tombstoneByte);
  request.desired_tag = toSlice(tag);

  auto writing = engine.update_raw_chunk(toSlice(key), request, error);
  if (!error.empty()) return makeError(request.out_error_code, std::move(error));
  engine.commit_raw_chunk(writing, sync, error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataSetError, std::move(error));
  return CacheChunkGenerationInfo{item.expectedGeneration, true, 0, ChecksumInfo{}};
}

Result<CacheChunkGenerationInfo> ChunkEngine::retireCacheChunkDurable(chunk_engine::Engine &engine,
                                                                      const RetireCacheChunkItem &item,
                                                                      ChainId chainId) {
  RETURN_ON_ERROR(retireCacheChunk(engine, item, chainId, true));
  auto query = queryCacheChunk(engine, item.key.chunkId, chainId);
  RETURN_ON_ERROR(query);
  if (query->cacheGeneration > item.expectedGeneration) return makeError(CacheCode::kGenerationAdvanced);
  if (query->cacheGeneration != item.expectedGeneration || !query->retired) {
    return makeError(CacheCode::kStateConflict, "cache generation is not durably retired");
  }
  return *query;
}

Result<CacheChunkGenerationInfo> ChunkEngine::queryCacheChunk(chunk_engine::Engine &engine,
                                                              const ChunkId &chunkId,
                                                              ChainId chainId) {
  auto key = cacheChunkKey(chainId, chunkId);
  std::string error;
  auto current = engine.get_raw_chunk(toSlice(key), error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));
  if (current == nullptr) return makeError(CacheCode::kNotFound);
  auto release = folly::makeGuard([&] { engine.release_raw_chunk(current); });
  auto tag = decodeCacheTag(current->raw_etag());
  if (!tag) return makeError(CacheCode::kNotFound);
  if (tag->state == CacheChunkState::RETIRED) {
    return CacheChunkGenerationInfo{tag->generation, true, 0, ChecksumInfo{}};
  }
  const auto &meta = current->raw_meta();
  return CacheChunkGenerationInfo{tag->generation, false, meta.len, ChecksumInfo{ChecksumType::CRC32C, ~meta.checksum}};
}

Result<std::optional<CacheChunkDescriptor>> ChunkEngine::queryCacheChunkDescriptor(chunk_engine::Engine &engine,
                                                                                   const ChunkId &chunkId,
                                                                                   ChainId chainId) {
  auto key = cacheChunkKey(chainId, chunkId);
  std::string error;
  auto current = engine.get_raw_chunk(toSlice(key), error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));
  if (current == nullptr) return makeError(CacheCode::kNotFound);
  auto release = folly::makeGuard([&] { engine.release_raw_chunk(current); });
  auto tag = decodeCacheTag(current->raw_etag());
  if (!tag) return makeError(CacheCode::kNotFound);
  return tag->descriptor;
}

Result<bool> ChunkEngine::updateCacheChunkAccess(chunk_engine::Engine &engine,
                                                 const ChunkId &chunkId,
                                                 ChainId chainId,
                                                 cache::CacheGeneration generation,
                                                 uint64_t observedAtNs,
                                                 bool sync) {
  auto key = cacheChunkKey(chainId, chunkId);
  std::string error;
  auto current = engine.get_raw_chunk(toSlice(key), error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataGetError, std::move(error));
  if (current == nullptr) return false;

  uint32_t version;
  uint32_t chainVersion;
  uint32_t checksum;
  uint32_t length;
  std::string oldTag;
  std::string newTag;
  {
    auto release = folly::makeGuard([&] { engine.release_raw_chunk(current); });
    auto tag = decodeCacheTag(current->raw_etag());
    if (!tag || tag->state != CacheChunkState::ACTIVE || tag->generation != generation || !tag->descriptor ||
        tag->descriptor->generation != generation) {
      return false;
    }
    if (tag->descriptor->lastAccessAtNs >= observedAtNs) return false;
    const auto &meta = current->raw_meta();
    if (meta.chunk_ver == std::numeric_limits<uint32_t>::max()) {
      return makeError(CacheCode::kStateConflict, "cache chunk version exhausted");
    }
    version = meta.chunk_ver;
    chainVersion = meta.chain_ver;
    checksum = meta.checksum;
    length = meta.len;
    oldTag.assign(reinterpret_cast<const char *>(current->raw_etag().data()), current->raw_etag().size());
    tag->descriptor->lastAccessAtNs = observedAtNs;
    newTag = encodeCacheTag(std::move(*tag));
  }

  static const uint8_t emptyWrite = 0;
  chunk_engine::UpdateReq request{};
  request.is_syncing = true;
  request.update_ver = version + 1;
  request.chain_ver = chainVersion;
  request.checksum = checksum;
  request.offset = length;
  request.length = 0;
  request.data = reinterpret_cast<uint64_t>(&emptyWrite);
  request.expected_tag = toSlice(oldTag);
  request.desired_tag = toSlice(newTag);

  auto writing = engine.update_raw_chunk(toSlice(key), request, error);
  if (!error.empty()) return makeError(request.out_error_code, std::move(error));
  engine.commit_raw_chunk(writing, sync, error);
  if (!error.empty()) return makeError(StorageCode::kChunkMetadataSetError, std::move(error));
  return true;
}

}  // namespace hf3fs::storage
