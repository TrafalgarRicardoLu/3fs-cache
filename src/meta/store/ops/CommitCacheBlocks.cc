#include <memory>

#include "cache/metrics/CacheMetrics.h"
#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheStateMachine.h"

namespace hf3fs::meta::server {

class CommitCacheBlocksOp : public Operation<CommitCacheBlocksRsp> {
 public:
  CommitCacheBlocksOp(MetaStore &meta, const CommitCacheBlocksReq &req)
      : Operation<CommitCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<CommitCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    CommitCacheBlocksRsp response;
    response.results.reserve(req_.items.size());
    auto routing = chainAlloc().getRoutingInfo();
    if (!routing) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
    for (const auto &item : req_.items) {
      if (auto valid = item.valid(); valid.hasError()) {
        response.results.emplace_back(makeError(valid.error()));
        continue;
      }
      auto layout = co_await resolveCacheBlockLayout(txn, item.key, item.blockLength, *routing);
      if (layout.hasError()) {
        response.results.emplace_back(makeError(layout.error()));
        continue;
      }
      auto result = co_await commit(txn, item, *layout);
      response.results.emplace_back(std::move(result));
    }
    co_return response;
  }

 private:
  CoTryTask<CacheBlockMutationResult> commit(IReadWriteTransaction &txn,
                                             const CommitCacheBlockItem &item,
                                             const CacheBlockLayout &layout) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kStateConflict, "cache block not found");
    auto record = std::move(**loaded);
    if (record.chainId != layout.chainId || record.blockLength != layout.blockLength) {
      co_return makeError(CacheCode::kStateConflict, "cache block layout changed");
    }
    if (item.checksumType != static_cast<uint8_t>(layout.checksumType)) {
      co_return makeError(CacheCode::kStateConflict, "cache block checksum type does not match the chain table");
    }
    cache::ReadyIdentity ready{item.loadEpoch,
                               item.cacheGeneration,
                               item.checksumType,
                               item.checksumValue,
                               item.blockLength};
    CO_RETURN_ON_ERROR(ready.valid());
    if (record.state == cache::CacheBlockState::READY && record.ready == ready) {
      if (record.placement != item.placement || record.committedPermit != item.permit) {
        co_return makeError(CacheCode::kPlacementMismatch, "repeated commit identity changed");
      }
      co_return CacheBlockMutationResult{record.key,
                                         record.state,
                                         cache::CacheEnqueueOutcome::INVALID,
                                         record.placement};
    }
    if (record.state != cache::CacheBlockState::LOADING || record.loaderId != item.loaderId ||
        record.loadEpoch != item.loadEpoch || record.cacheGeneration != item.cacheGeneration) {
      cache::metrics::recordCount(
          cache::metrics::Event::META_EPOCH_CONFLICT,
          1,
          {.inode = item.key.inode, .block = item.key.block.toUnderType(), .reason = "stale_commit"});
      co_return makeError(CacheCode::kStateConflict, "stale cache block commit");
    }
    if (record.permit.has_value()) {
      if (!item.permit.has_value() || !item.placement.has_value() || record.permit != item.permit ||
          record.permit->placement != *item.placement) {
        co_return makeError(CacheCode::kPlacementMismatch, "commit permit or placement changed");
      }
    } else if (item.permit.has_value() || item.placement.has_value()) {
      co_return makeError(CacheCode::kPlacementMismatch, "legacy admission cannot accept phase two placement");
    }

    record.state = cache::CacheBlockState::READY;
    record.ready = ready;
    record.placement = item.placement;
    record.committedPermit = item.permit;
    record.permit.reset();
    record.loaderId = Uuid::zero();
    record.leaseExpiresAt = UtcTime{};
    record.readyAt = UtcClock::now();
    record.lastAccessAt = record.readyAt;
    auto committed = co_await CacheBlockStore::commitCharge(txn, record);
    CO_RETURN_ON_ERROR(committed);
    co_return CacheBlockMutationResult{committed->key,
                                       committed->state,
                                       cache::CacheEnqueueOutcome::INVALID,
                                       committed->placement};
  }

  const CommitCacheBlocksReq &req_;
};

MetaStore::OpPtr<CommitCacheBlocksRsp> MetaStore::commitCacheBlocks(const CommitCacheBlocksReq &req) {
  return std::make_unique<CommitCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
