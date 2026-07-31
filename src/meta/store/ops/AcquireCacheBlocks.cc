#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheStateMachine.h"

namespace hf3fs::meta::server {

class AcquireCacheBlocksOp : public Operation<AcquireCacheBlocksRsp> {
 public:
  AcquireCacheBlocksOp(MetaStore &meta, const AcquireCacheBlocksReq &req)
      : Operation<AcquireCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<AcquireCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    AcquireCacheBlocksRsp response;
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
      auto result = co_await acquire(txn, item, *layout);
      response.results.emplace_back(std::move(result));
    }
    co_return response;
  }

 private:
  Uuid loaderIdFor(const cache::CacheBlockKey &key) {
    for (const auto &[existingKey, loaderId] : loaderIds_) {
      if (existingKey == key) return loaderId;
    }
    auto loaderId = Uuid::random();
    loaderIds_.emplace_back(key, loaderId);
    return loaderId;
  }

  CoTryTask<AcquireCacheBlockResult> acquire(IReadWriteTransaction &txn,
                                             const CacheBlockRequestBase &item,
                                             const CacheBlockLayout &layout) {
    auto loaded = co_await CacheBlockStore::load(txn, item.key);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kStateConflict, "cache block is not queued");
    auto record = std::move(**loaded);
    if (record.chainId != layout.chainId || record.blockLength != layout.blockLength) {
      co_return makeError(CacheCode::kStateConflict, "cache block layout changed");
    }

    const auto loaderId = loaderIdFor(item.key);
    if (record.state == cache::CacheBlockState::LOADING && record.loaderId == loaderId) {
      co_return AcquireCacheBlockResult{
          record.key,
          CacheBlockLease{record.loaderId, record.loadEpoch, record.cacheGeneration, record.permit}};
    }
    const auto now = UtcClock::now();
    if (record.state != cache::CacheBlockState::QUEUED &&
        (record.state != cache::CacheBlockState::LOADING || record.leaseExpiresAt > now)) {
      co_return makeError(CacheCode::kStateConflict, "cache block is not acquirable");
    }
    if (record.chargeKind != cache::ChargeKind::RESERVED || record.chargedBytes != record.blockLength) {
      co_return makeError(CacheCode::kStateConflict, "cache block has no reserved charge");
    }
    if (record.loadEpoch == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "cache load epoch exhausted");
    }
    auto generation = co_await CacheBlockStore::allocateGeneration(txn, record.key);
    CO_RETURN_ON_ERROR(generation);
    record.state = cache::CacheBlockState::LOADING;
    record.loaderId = loaderId;
    ++record.loadEpoch;
    record.cacheGeneration = *generation;
    record.leaseExpiresAt = now + config().cache_load_lease().asUs();
    record.ready.reset();
    record.readyAt = UtcTime{};
    record.lastAccessAt = UtcTime{};
    record.terminalState = cache::CleanupTerminalState::NONE;
    CO_RETURN_ON_ERROR(co_await CacheBlockStore::store(txn, record));
    co_return AcquireCacheBlockResult{
        record.key,
        CacheBlockLease{record.loaderId, record.loadEpoch, record.cacheGeneration, record.permit}};
  }

  const AcquireCacheBlocksReq &req_;
  std::vector<std::pair<cache::CacheBlockKey, Uuid>> loaderIds_;
};

MetaStore::OpPtr<AcquireCacheBlocksRsp> MetaStore::acquireCacheBlocks(const AcquireCacheBlocksReq &req) {
  return std::make_unique<AcquireCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
