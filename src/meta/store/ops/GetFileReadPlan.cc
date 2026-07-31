#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "meta/store/FileSession.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {
namespace {

uint64_t blockCount(uint64_t begin, uint64_t end, uint64_t blockSize) {
  if (begin >= end) return 0;
  return (end - 1) / blockSize - begin / blockSize + 1;
}

}  // namespace

class GetFileReadPlanOp : public ReadOnlyOperation<GetFileReadPlanRsp> {
 public:
  GetFileReadPlanOp(MetaStore &meta, const GetFileReadPlanReq &req)
      : ReadOnlyOperation<GetFileReadPlanRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<GetFileReadPlanRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);

    auto inode = (co_await Inode::snapshotLoad(txn, req_.inode)).then(checkMetaFound<Inode>);
    CO_RETURN_ON_ERROR(inode);
    if (!inode->isOriginFile()) co_return makeError(MetaCode::kNotFile, "inode is not an OriginFile");
    CO_RETURN_ON_ERROR(inode->acl.checkPermission(req_.user, AccessType::READ));
    CO_RETURN_ON_ERROR(inode->asOriginFile().object.valid());

    auto session = co_await FileSession::load(txn, inode->id, req_.openSessionId);
    CO_RETURN_ON_ERROR(session);
    if (!session->has_value() || (*session)->clientId != req_.client) {
      co_return makeError(MetaCode::kNoPermission, "origin read session is missing or belongs to another client");
    }

    GetFileReadPlanRsp response;
    response.inode = inode->id;
    response.object = inode->asOriginFile().object;
    const auto fileLength = inode->fileLength();
    if (req_.length == 0 || req_.offset >= fileLength) co_return response;
    const auto requestEnd = std::min(fileLength, req_.offset + req_.length);
    const auto blockSize = uint64_t{inode->fileLayout().chunkSize};
    const auto count = blockCount(req_.offset, requestEnd, blockSize);
    if (count > kMaxCacheBatchItems) {
      co_return makeError(CacheCode::kRequestTooLarge, "read plan exceeds the block limit");
    }

    const auto firstBlock = req_.offset / blockSize;
    std::vector<cache::CacheBlockKey> keys;
    keys.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
      auto block = firstBlock + i;
      if (block > std::numeric_limits<uint32_t>::max()) {
        co_return makeError(CacheCode::kRequestTooLarge, "cache block index overflow");
      }
      keys.push_back({inode->id.u64(), cache::CacheBlockIndex{static_cast<uint32_t>(block)}});
    }
    auto records = co_await CacheBlockStore::snapshotLoadBatch(txn, keys);
    CO_RETURN_ON_ERROR(records);

    auto routing = chainAlloc().getRoutingInfo();
    if (!routing || !routing->raw()) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
    response.blocks.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      const auto blockOffset = (firstBlock + i) * blockSize;
      const auto actualLength = std::min(blockSize, fileLength - blockOffset);
      auto chunkId = inode->getChunkId(inode->id, blockOffset);
      CO_RETURN_ON_ERROR(chunkId);
      auto chainId = inode->getChainId(*inode, blockOffset, *routing->raw());
      CO_RETURN_ON_ERROR(chainId);

      ReadBlockPlan plan;
      plan.key = keys[i];
      plan.fileRange = {blockOffset, actualLength};
      plan.originRange = plan.fileRange;
      plan.chunkId = *chunkId;
      plan.chainId = *chainId;
      plan.actualBlockLength = actualLength;
      if ((*records)[i].has_value()) {
        const auto &record = *(*records)[i];
        if (record.chainId != plan.chainId || record.blockLength != actualLength) {
          co_return makeError(StatusCode::kDataCorruption, "cache block record does not match inode layout");
        }
        if (record.state == cache::CacheBlockState::READY && !record.ready.has_value()) {
          co_return makeError(StatusCode::kDataCorruption, "READY cache block has no ready identity");
        }
        plan.state = record.state;
        plan.loadEpoch = record.loadEpoch;
        if (record.state == cache::CacheBlockState::READY) plan.ready = record.ready;
      }
      response.blocks.push_back(std::move(plan));
    }
    co_return response;
  }

 private:
  const GetFileReadPlanReq &req_;
};

MetaStore::OpPtr<GetFileReadPlanRsp> MetaStore::getFileReadPlan(const GetFileReadPlanReq &req) {
  return std::make_unique<GetFileReadPlanOp>(*this, req);
}

}  // namespace hf3fs::meta::server
