#include <memory>

#include "cache/metrics/CacheMetrics.h"
#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {

class FinishCleanCacheBlocksOp : public Operation<FinishCleanCacheBlocksRsp> {
 public:
  FinishCleanCacheBlocksOp(MetaStore &meta, const FinishCleanCacheBlocksReq &req)
      : Operation<FinishCleanCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<FinishCleanCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    FinishCleanCacheBlocksRsp response;
    response.results.reserve(req_.items.size());
    for (const auto &item : req_.items) {
      if (auto valid = item.valid(); valid.hasError()) {
        response.results.emplace_back(makeError(valid.error()));
        continue;
      }
      response.results.emplace_back(co_await finishOne(txn, item));
    }
    co_return response;
  }

 private:
  CoTryTask<CacheBlockMutationResult> finishOne(IReadWriteTransaction &txn, const FinishCleanCacheBlockItem &item) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return CacheBlockMutationResult{item.key, cache::CacheBlockState::NONE};
    const auto &record = **loaded;
    if (record.cleanupEpoch != item.cleanupEpoch) {
      cache::metrics::recordCount(
          cache::metrics::Event::META_EPOCH_CONFLICT,
          1,
          {.inode = item.key.inode, .block = item.key.block.toUnderType(), .reason = "stale_cleanup"});
      co_return makeError(CacheCode::kStateConflict, "stale cache cleanup epoch");
    }
    if (record.state == cache::CacheBlockState::FAILED && record.terminalState == cache::CleanupTerminalState::FAILED) {
      co_return CacheBlockMutationResult{record.key, record.state};
    }
    if (record.state == cache::CacheBlockState::QUEUED &&
        record.terminalState == cache::CleanupTerminalState::REENQUEUE) {
      co_return CacheBlockMutationResult{record.key, record.state};
    }
    if (record.state != cache::CacheBlockState::CLEANING) {
      co_return makeError(CacheCode::kStateConflict, "cache block is not cleaning");
    }
    if (record.deleteGeneration != cache::CacheGeneration{} &&
        (!item.retiredGeneration.has_value() || *item.retiredGeneration < record.deleteGeneration)) {
      co_return makeError(CacheCode::kStaleGeneration, "Storage tombstone does not cover the delete generation");
    }

    auto terminalState = record.terminalState;
    CO_RETURN_ON_ERROR(co_await CacheBlockStore::finishClean(txn, item.key, terminalState));
    auto state = terminalState == cache::CleanupTerminalState::REENQUEUE ? cache::CacheBlockState::QUEUED
                 : terminalState == cache::CleanupTerminalState::FAILED  ? cache::CacheBlockState::FAILED
                                                                         : cache::CacheBlockState::NONE;
    co_return CacheBlockMutationResult{item.key, state};
  }

  const FinishCleanCacheBlocksReq &req_;
};

MetaStore::OpPtr<FinishCleanCacheBlocksRsp> MetaStore::finishCleanCacheBlocks(const FinishCleanCacheBlocksReq &req) {
  return std::make_unique<FinishCleanCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
