#include <limits>
#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheStateMachine.h"

namespace hf3fs::meta::server {

class FailCacheBlocksOp : public Operation<FailCacheBlocksRsp> {
 public:
  FailCacheBlocksOp(MetaStore &meta, const FailCacheBlocksReq &req)
      : Operation<FailCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<FailCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    FailCacheBlocksRsp response;
    response.results.reserve(req_.items.size());
    auto routing = chainAlloc().getRoutingInfo();
    if (!routing) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
    for (const auto &item : req_.items) {
      if (auto valid = item.valid(); valid.hasError()) {
        response.results.emplace_back(makeError(valid.error()));
        continue;
      }
      auto layout = co_await resolveCacheBlockLayout(txn, item.key, std::nullopt, *routing);
      if (layout.hasError()) {
        response.results.emplace_back(makeError(layout.error()));
        continue;
      }
      auto result = co_await fail(txn, item, *layout);
      response.results.emplace_back(std::move(result));
    }
    co_return response;
  }

 private:
  CoTryTask<CacheBlockMutationResult> fail(IReadWriteTransaction &txn,
                                           const FailCacheBlockItem &item,
                                           const CacheBlockLayout &layout) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kStateConflict, "cache block not found");
    auto record = std::move(**loaded);
    if (record.chainId != layout.chainId || record.blockLength != layout.blockLength) {
      co_return makeError(CacheCode::kStateConflict, "cache block layout changed");
    }
    if (record.state == cache::CacheBlockState::CLEANING &&
        record.terminalState == cache::CleanupTerminalState::FAILED && record.loaderId == item.loaderId &&
        item.loadEpoch != std::numeric_limits<uint64_t>::max() && record.loadEpoch == item.loadEpoch + 1) {
      co_return CacheBlockMutationResult{record.key, record.state};
    }
    if (record.state != cache::CacheBlockState::LOADING || record.loaderId != item.loaderId ||
        record.loadEpoch != item.loadEpoch) {
      co_return makeError(CacheCode::kStateConflict, "stale cache block failure");
    }
    if (record.cleanupEpoch.toUnderType() == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "cache cleanup epoch exhausted");
    }
    if (record.loadEpoch == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "cache load epoch exhausted");
    }
    record.state = cache::CacheBlockState::CLEANING;
    ++record.loadEpoch;
    ++record.cleanupEpoch;
    record.terminalState = cache::CleanupTerminalState::FAILED;
    record.deleteGeneration = record.cacheGeneration;
    record.leaseExpiresAt = UtcTime{};
    record.ready.reset();
    CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
    co_return CacheBlockMutationResult{record.key, record.state};
  }

  const FailCacheBlocksReq &req_;
};

MetaStore::OpPtr<FailCacheBlocksRsp> MetaStore::failCacheBlocks(const FailCacheBlocksReq &req) {
  return std::make_unique<FailCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
