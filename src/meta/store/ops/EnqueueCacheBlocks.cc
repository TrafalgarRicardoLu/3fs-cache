#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheStateMachine.h"

namespace hf3fs::meta::server {

class EnqueueCacheBlocksOp : public Operation<EnqueueCacheBlocksRsp> {
 public:
  EnqueueCacheBlocksOp(MetaStore &meta, const EnqueueCacheBlocksReq &req)
      : Operation<EnqueueCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<EnqueueCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    EnqueueCacheBlocksRsp response;
    response.results.reserve(req_.items.size());
    response.permits.reserve(req_.items.size());
    auto routing = chainAlloc().getRoutingInfo();
    if (!routing) co_return makeError(CacheCode::kUnavailable, "routing info is unavailable");
    for (const auto &item : req_.items) {
      if (auto valid = item.valid(); valid.hasError()) {
        response.results.emplace_back(makeError(valid.error()));
        response.permits.emplace_back(std::nullopt);
        continue;
      }
      auto layout = co_await resolveCacheBlockLayout(txn, item.key, item.blockLength, *routing);
      if (layout.hasError()) {
        response.results.emplace_back(makeError(layout.error()));
        response.permits.emplace_back(std::nullopt);
        continue;
      }
      if (item.permit.has_value()) {
        const auto &placement = item.permit->placement;
        if (placement.versionedChain != storage::VersionedChainId{layout->chainId, layout->chainVersion} ||
            placement.expectedReplicaTargets != layout->replicaTargets) {
          response.results.emplace_back(makeError(CacheCode::kPlacementMismatch, "permit placement changed"));
          response.permits.emplace_back(std::nullopt);
          continue;
        }
      }
      auto before = co_await CacheBlockStore::load(txn, item.key);
      if (before.hasError()) {
        response.results.emplace_back(makeError(before.error()));
        response.permits.emplace_back(std::nullopt);
        continue;
      }
      auto record = co_await CacheBlockStore::enqueue(txn,
                                                      item.key,
                                                      layout->chainId,
                                                      layout->blockLength,
                                                      item.permit,
                                                      item.expectedPermit,
                                                      item.expectedState,
                                                      item.expectedLoaderId,
                                                      item.expectedLoadEpoch);
      if (record.hasError()) {
        response.results.emplace_back(makeError(record.error()));
        response.permits.emplace_back(std::nullopt);
        continue;
      }
      auto outcome = cache::CacheEnqueueOutcome::CREATED;
      if (before->has_value() && (*before)->state != cache::CacheBlockState::FAILED) {
        switch ((*before)->state) {
          case cache::CacheBlockState::QUEUED:
            outcome = cache::CacheEnqueueOutcome::QUEUED;
            break;
          case cache::CacheBlockState::LOADING:
            outcome = cache::CacheEnqueueOutcome::LOADING;
            break;
          case cache::CacheBlockState::READY:
            outcome = cache::CacheEnqueueOutcome::READY;
            break;
          default:
            outcome = cache::CacheEnqueueOutcome::INVALID;
            break;
        }
      }
      response.results.emplace_back(CacheBlockMutationResult{record->key, record->state, outcome, record->placement});
      response.permits.emplace_back(record->permit);
    }
    co_return response;
  }

 private:
  const EnqueueCacheBlocksReq &req_;
};

MetaStore::OpPtr<EnqueueCacheBlocksRsp> MetaStore::enqueueCacheBlocks(const EnqueueCacheBlocksReq &req) {
  return std::make_unique<EnqueueCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
