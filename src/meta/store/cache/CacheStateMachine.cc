#include "meta/store/cache/CacheStateMachine.h"

#include <algorithm>

#include "common/utils/Result.h"
#include "meta/store/Inode.h"
#include "meta/store/Utils.h"

namespace hf3fs::meta::server {

CoTryTask<CacheBlockLayout> resolveCacheBlockLayout(kv::IReadWriteTransaction &txn,
                                                    const cache::CacheBlockKey &key,
                                                    std::optional<uint64_t> expectedLength,
                                                    const client::RoutingInfo &routing) {
  CO_RETURN_ON_ERROR(key.valid());
  auto inode = (co_await Inode::load(txn, InodeId{key.inode})).then(checkMetaFound<Inode>);
  CO_RETURN_ON_ERROR(inode);
  if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile, "cache block inode is not an OriginFile");
  if (inode->asOriginFile().superseded || inode->asOriginFile().cacheAdmissionDisabled) {
    co_return makeError(CacheCode::kStateConflict, "cache admission is disabled for superseded OriginFile");
  }
  if (!routing.raw()) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  auto table = routing.raw()->getChainTable(inode->fileLayout().tableId);
  if (!table || !table->isCacheData()) {
    co_return makeError(CacheCode::kStateConflict, "OriginFile cache chain table is unavailable");
  }
  CO_RETURN_ON_ERROR(table->valid());

  const auto blockSize = uint64_t{inode->fileLayout().chunkSize};
  const auto blockOffset = uint64_t{key.block.toUnderType()} * blockSize;
  if (blockOffset >= inode->fileLength()) {
    co_return makeError(StatusCode::kInvalidArg, "cache block is outside the OriginFile");
  }
  const auto blockLength = std::min(blockSize, inode->fileLength() - blockOffset);
  if (expectedLength.has_value() && *expectedLength != blockLength) {
    co_return makeError(CacheCode::kStateConflict, "cache block length does not match the OriginFile");
  }
  auto chainId = inode->getChainId(*inode, blockOffset, *routing.raw());
  CO_RETURN_ON_ERROR(chainId);
  co_return CacheBlockLayout{*chainId, blockLength, table->checksumType};
}

}  // namespace hf3fs::meta::server
