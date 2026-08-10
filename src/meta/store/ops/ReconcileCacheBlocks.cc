#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {
namespace {

Result<ReconcileCacheBlockStatus> reconcileStatus(const cache::CacheBlockKey &key,
                                                  const std::optional<CacheBlockRecord> &record) {
  if (!record) return ReconcileCacheBlockStatus{key};
  ReconcileCacheBlockStatus status;
  status.key = key;
  status.state = record->state;
  status.blockLength = record->blockLength;
  status.cacheGeneration = record->cacheGeneration;
  status.ready = record->ready;
  status.placement = record->placement;
  status.permit = record->permit ? record->permit : record->committedPermit;
  status.cleanupEpoch = record->cleanupEpoch;
  status.terminalState = record->terminalState;
  status.deleteGeneration = record->deleteGeneration;
  status.evictionEpoch = record->evictionEpoch;
  status.retireOperationId = record->retireOperationId;
  status.evictionReason = record->evictionReason;
  auto valid = status.valid();
  if (valid.hasError()) return makeError(StatusCode::kDataCorruption, valid.error().message());
  return status;
}

class ReconcileCacheBlocksOp : public ReadOnlyOperation<ReconcileCacheBlocksRsp> {
 public:
  ReconcileCacheBlocksOp(MetaStore &meta, const ReconcileCacheBlocksReq &req)
      : ReadOnlyOperation<ReconcileCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ReconcileCacheBlocksRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto records = co_await CacheBlockStore::snapshotLoadBatch(txn, req_.keys);
    CO_RETURN_ON_ERROR(records);
    if (records->size() != req_.keys.size()) {
      co_return makeError(CacheCode::kInvalidResponse, "cache reconcile snapshot result count mismatch");
    }
    ReconcileCacheBlocksRsp response;
    response.results.reserve(req_.keys.size());
    for (size_t index = 0; index < req_.keys.size(); ++index) {
      response.results.push_back(reconcileStatus(req_.keys[index], records->at(index)));
    }
    co_return response;
  }

 private:
  const ReconcileCacheBlocksReq &req_;
};

class ListReconcileCacheBlocksOp : public Operation<ListReconcileCacheBlocksRsp> {
 public:
  ListReconcileCacheBlocksOp(MetaStore &meta, const ListReconcileCacheBlocksReq &req)
      : Operation<ListReconcileCacheBlocksRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListReconcileCacheBlocksRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await CacheBlockStore::snapshotListReconcile(txn, req_.after, req_.limit);
    CO_RETURN_ON_ERROR(page);
    ListReconcileCacheBlocksRsp response;
    response.more = page->more;
    response.items.reserve(page->records.size());
    for (const auto &record : page->records) {
      auto status = reconcileStatus(record.key, record);
      CO_RETURN_ON_ERROR(status);
      response.items.push_back(std::move(*status));
    }
    co_return response;
  }

 private:
  const ListReconcileCacheBlocksReq &req_;
};

}  // namespace

MetaStore::OpPtr<ReconcileCacheBlocksRsp> MetaStore::reconcileCacheBlocks(const ReconcileCacheBlocksReq &req) {
  return std::make_unique<ReconcileCacheBlocksOp>(*this, req);
}

MetaStore::OpPtr<ListReconcileCacheBlocksRsp> MetaStore::listReconcileCacheBlocks(
    const ListReconcileCacheBlocksReq &req) {
  return std::make_unique<ListReconcileCacheBlocksOp>(*this, req);
}

}  // namespace hf3fs::meta::server
