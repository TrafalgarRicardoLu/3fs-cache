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
      auto record = co_await CacheBlockStore::enqueue(txn, item.key, layout->chainId, layout->blockLength);
      if (record.hasError()) {
        response.results.emplace_back(makeError(record.error()));
        continue;
      }
      response.results.emplace_back(CacheBlockMutationResult{record->key, record->state});
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
