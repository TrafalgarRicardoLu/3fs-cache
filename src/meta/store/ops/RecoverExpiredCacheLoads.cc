#include <limits>
#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {

class RecoverExpiredCacheLoadsOp : public Operation<RecoverExpiredCacheLoadsRsp> {
 public:
  RecoverExpiredCacheLoadsOp(MetaStore &meta, const RecoverExpiredCacheLoadsReq &req)
      : Operation<RecoverExpiredCacheLoadsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<RecoverExpiredCacheLoadsRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    RecoverExpiredCacheLoadsRsp response;
    response.results.reserve(req_.items.size());
    for (const auto &item : req_.items) {
      if (auto valid = item.valid(); valid.hasError()) {
        response.results.emplace_back(makeError(valid.error()));
        continue;
      }
      response.results.emplace_back(co_await recover(txn, item));
    }
    co_return response;
  }

 private:
  CoTryTask<CacheBlockMutationResult> recover(IReadWriteTransaction &txn, const RecoverExpiredCacheLoadItem &item) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kStateConflict, "cache block not found");
    auto record = std::move(**loaded);

    if (record.state == cache::CacheBlockState::CLEANING && record.terminalState == item.terminalState &&
        record.loaderId == item.loaderId && item.loadEpoch != std::numeric_limits<uint64_t>::max() &&
        record.loadEpoch == item.loadEpoch + 1 && record.deleteGeneration == item.expectedGeneration &&
        record.placement == std::optional<storage::PlacementIdentity>{item.expectedPlacement}) {
      co_return CacheBlockMutationResult{record.key, record.state};
    }
    if (record.state != cache::CacheBlockState::LOADING || record.loaderId != item.loaderId ||
        record.loadEpoch != item.loadEpoch || record.leaseExpiresAt != item.expectedLeaseExpiresAt ||
        record.cacheGeneration != item.expectedGeneration ||
        record.permit != std::optional<storage::PermitIdentity>{item.expectedPermit} ||
        item.expectedPermit.placement != item.expectedPlacement) {
      co_return makeError(CacheCode::kStateConflict, "expired cache load fence changed");
    }
    if (record.leaseExpiresAt > UtcClock::now()) {
      co_return makeError(CacheCode::kStateConflict, "cache load lease has not expired");
    }
    if (record.loadEpoch == std::numeric_limits<uint64_t>::max() ||
        record.cleanupEpoch.toUnderType() == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "cache recovery epoch exhausted");
    }

    record.state = cache::CacheBlockState::CLEANING;
    ++record.loadEpoch;
    ++record.cleanupEpoch;
    record.terminalState = item.terminalState;
    record.deleteGeneration = record.cacheGeneration;
    record.leaseExpiresAt = UtcTime{};
    record.ready.reset();
    record.readyAt = UtcTime{};
    record.lastAccessAt = UtcTime{};
    record.placement = item.expectedPlacement;
    record.permit.reset();
    CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
    co_return CacheBlockMutationResult{record.key, record.state};
  }

  const RecoverExpiredCacheLoadsReq &req_;
};

MetaStore::OpPtr<RecoverExpiredCacheLoadsRsp> MetaStore::recoverExpiredCacheLoads(
    const RecoverExpiredCacheLoadsReq &req) {
  return std::make_unique<RecoverExpiredCacheLoadsOp>(*this, req);
}

}  // namespace hf3fs::meta::server
