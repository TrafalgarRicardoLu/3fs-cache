#include <limits>
#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"

namespace hf3fs::meta::server {

class UpdateCacheBlockAccessOp : public Operation<UpdateCacheBlockAccessRsp> {
 public:
  UpdateCacheBlockAccessOp(MetaStore &meta, const UpdateCacheBlockAccessReq &req)
      : Operation<UpdateCacheBlockAccessRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<UpdateCacheBlockAccessRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    UpdateCacheBlockAccessRsp response;
    response.results.reserve(req_.items.size());
    for (const auto &item : req_.items) {
      auto valid = item.valid();
      if (valid.hasError()) {
        response.results.push_back(makeError(valid.error()));
        continue;
      }
      const auto receiveUs = item.managerReceiveTimeNs / 1000;
      if (receiveUs > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        response.results.push_back(makeError(StatusCode::kInvalidArg, "cache access timestamp is out of range"));
        continue;
      }
      auto updated = co_await CacheBlockStore::updateAccess(txn,
                                                            item.key,
                                                            item.generation,
                                                            UtcTime::fromMicroseconds(static_cast<int64_t>(receiveUs)));
      if (updated.hasError()) {
        response.results.push_back(makeError(updated.error()));
      } else {
        response.results.push_back(UpdateCacheBlockAccessResult{item.key, *updated});
      }
    }
    co_return response;
  }

 private:
  const UpdateCacheBlockAccessReq &req_;
};

MetaStore::OpPtr<UpdateCacheBlockAccessRsp> MetaStore::updateCacheBlockAccess(const UpdateCacheBlockAccessReq &req) {
  return std::make_unique<UpdateCacheBlockAccessOp>(*this, req);
}

}  // namespace hf3fs::meta::server
