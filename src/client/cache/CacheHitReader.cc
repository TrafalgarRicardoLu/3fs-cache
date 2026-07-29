#include "client/cache/CacheHitReader.h"

#include <limits>

namespace hf3fs::client::cache {

Result<Void> StorageCacheHitReader::validate(const meta::ReadBlockPlan &plan,
                                             std::span<const uint8_t> data,
                                             hf3fs::cache::CacheGeneration generation) {
  if (!plan.ready) return makeError(CacheCode::kInvalidResponse, "cache hit has no READY identity");
  RETURN_ON_ERROR(plan.ready->valid());
  if (generation != plan.ready->cacheGeneration) {
    return makeError(CacheCode::kStaleGeneration, "cache hit generation mismatch");
  }
  if (data.size() != plan.actualBlockLength || data.size() != plan.ready->blockLength) {
    return makeError(CacheCode::kInvalidResponse, "cache hit length mismatch");
  }
  auto checksumType = static_cast<storage::ChecksumType>(plan.ready->checksumType);
  if (checksumType != storage::ChecksumType::CRC32C && checksumType != storage::ChecksumType::CRC32) {
    return makeError(CacheCode::kInvalidResponse, "cache hit checksum type is unsupported");
  }
  auto checksum = storage::ChecksumInfo::create(checksumType, data.data(), data.size());
  if (checksum.value != plan.ready->checksumValue) {
    return makeError(StorageClientCode::kChecksumMismatch, "cache hit checksum mismatch");
  }
  return Void{};
}

CoTryTask<std::vector<uint8_t>> StorageCacheHitReader::readFullBlock(const meta::ReadBlockPlan &plan,
                                                                     const flat::UserInfo &user) {
  if (plan.actualBlockLength > std::numeric_limits<uint32_t>::max()) {
    co_return makeError(StatusCode::kInvalidArg, "cache block is too large for Storage read");
  }
  std::vector<uint8_t> data(plan.actualBlockLength);
  auto buffer = client_.registerIOBuffer(data.data(), data.size());
  CO_RETURN_ON_ERROR(buffer);
  auto io = client_.createReadIO(plan.chainId,
                                 storage::ChunkId{plan.chunkId.pack()},
                                 0,
                                 static_cast<uint32_t>(data.size()),
                                 data.data(),
                                 &*buffer);
  storage::client::ReadOptions options;
  options.set_enableChecksum(true);
  CO_RETURN_ON_ERROR(co_await client_.read(io, user, options));
  CO_RETURN_ON_ERROR(io.result.lengthInfo);
  if (io.resultLen() != data.size()) co_return makeError(CacheCode::kInvalidResponse, "short cache block read");
  CO_RETURN_ON_ERROR(validate(plan, data, io.result.cacheGeneration));
  co_return data;
}

}  // namespace hf3fs::client::cache
