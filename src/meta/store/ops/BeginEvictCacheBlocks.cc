#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/PinStore.h"

namespace hf3fs::meta::server {
namespace {

CacheEvictionIdentity identity(const CacheBlockRecord &record) {
  return {record.key,
          *record.ready,
          *record.placement,
          record.evictionEpoch,
          record.retireOperationId,
          record.evictionReason};
}

}  // namespace

class BeginEvictCacheBlocksOp : public Operation<BeginEvictCacheBlocksRsp> {
 public:
  BeginEvictCacheBlocksOp(MetaStore &meta, const BeginEvictCacheBlocksReq &req)
      : Operation<BeginEvictCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<BeginEvictCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    BeginEvictCacheBlocksRsp response;
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
  CoTryTask<CacheEvictionIdentity> begin(IReadWriteTransaction &txn, const BeginEvictCacheBlockItem &item) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "cache block not found");
    auto record = std::move(**loaded);
    if (record.state == cache::CacheBlockState::EVICTING) {
      if (record.ready != item.expectedReady) {
        co_return makeError(CacheCode::kStateConflict, "cache READY identity changed");
      }
      co_return identity(record);
    }
    if (record.state != cache::CacheBlockState::READY || record.ready != item.expectedReady) {
      co_return makeError(CacheCode::kStateConflict, "cache block is not the expected READY generation");
    }
    auto nowMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000);
    auto pins = co_await PinStore::queryActive(txn, item.key, nowMs);
    CO_RETURN_ON_ERROR(pins);
    if (!pins->empty()) co_return makeError(CacheCode::kStateConflict, "cache block is pinned");
    if (!record.placement.has_value() || !record.committedPermit.has_value() ||
        record.committedPermit->placement != *record.placement) {
      co_return makeError(CacheCode::kPlacementMismatch, "cache READY block has no immutable placement");
    }

    auto epoch = co_await CacheBlockStore::allocateEvictionEpoch(txn, item.key);
    CO_RETURN_ON_ERROR(epoch);
    record.state = cache::CacheBlockState::EVICTING;
    record.evictionEpoch = *epoch;
    record.retireOperationId = Uuid::random();
    record.evictionReason = item.reason;
    CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
    co_return identity(record);
  }

  const BeginEvictCacheBlocksReq &req_;
};

MetaStore::OpPtr<BeginEvictCacheBlocksRsp> MetaStore::beginEvictCacheBlocks(const BeginEvictCacheBlocksReq &req) {
  return std::make_unique<BeginEvictCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
