#include <algorithm>
#include <limits>
#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {

class BeginCleanCacheBlocksOp : public Operation<BeginCleanCacheBlocksRsp> {
 public:
  BeginCleanCacheBlocksOp(MetaStore &meta, const BeginCleanCacheBlocksReq &req)
      : Operation<BeginCleanCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<BeginCleanCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    BeginCleanCacheBlocksRsp response;
    response.results.reserve(req_.items.size());
    for (const auto &item : req_.items) {
      if (auto valid = item.valid(); valid.hasError()) {
        response.results.emplace_back(makeError(valid.error()));
        continue;
      }
      response.results.emplace_back(co_await begin(txn, item));
    }
    co_return response;
  }

 private:
  CoTryTask<BeginCleanCacheBlockResult> begin(IReadWriteTransaction &txn, const BeginCleanCacheBlockItem &item) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "cache block not found");
    auto record = std::move(**loaded);

    if (item.expectedReady.has_value() && record.ready != item.expectedReady) {
      co_return makeError(CacheCode::kStateConflict, "cache READY identity changed");
    }
    if (record.state == cache::CacheBlockState::CLEANING) {
      if (record.terminalState != item.terminalState) {
        co_return makeError(CacheCode::kStateConflict, "cache cleanup terminal state changed");
      }
      if (item.observedGeneration.has_value() && *item.observedGeneration > record.deleteGeneration) {
        record.deleteGeneration = *item.observedGeneration;
        CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
      }
      co_return BeginCleanCacheBlockResult{record.key, record.cleanupEpoch, record.deleteGeneration};
    }
    if (record.state != cache::CacheBlockState::QUEUED && record.state != cache::CacheBlockState::LOADING &&
        record.state != cache::CacheBlockState::READY && record.state != cache::CacheBlockState::FAILED) {
      co_return makeError(CacheCode::kStateConflict, "cache block cannot enter cleanup");
    }
    if (record.loadEpoch == std::numeric_limits<uint64_t>::max() ||
        record.cleanupEpoch.toUnderType() == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "cache cleanup epoch exhausted");
    }

    ++record.loadEpoch;
    ++record.cleanupEpoch;
    record.state = cache::CacheBlockState::CLEANING;
    record.terminalState = item.terminalState;
    record.deleteGeneration = record.cacheGeneration;
    if (item.observedGeneration.has_value()) {
      record.deleteGeneration = std::max(record.deleteGeneration, *item.observedGeneration);
    }
    record.loaderId = Uuid::zero();
    record.leaseExpiresAt = UtcTime{};
    CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
    co_return BeginCleanCacheBlockResult{record.key, record.cleanupEpoch, record.deleteGeneration};
  }

  const BeginCleanCacheBlocksReq &req_;
};

MetaStore::OpPtr<BeginCleanCacheBlocksRsp> MetaStore::beginCleanCacheBlocks(const BeginCleanCacheBlocksReq &req) {
  return std::make_unique<BeginCleanCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
