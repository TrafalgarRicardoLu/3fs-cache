#include <memory>

#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/UploadJobStore.h"

namespace hf3fs::meta::server {

class ListUploadJobsOp : public ReadOnlyOperation<ListUploadJobsRsp> {
 public:
  ListUploadJobsOp(MetaStore &meta, const ListUploadJobsReq &req)
      : ReadOnlyOperation<ListUploadJobsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListUploadJobsRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await UploadJobStore::snapshotList(txn, req_.ownerUid, req_.after, req_.limit, req_.includeTerminal);
    CO_RETURN_ON_ERROR(page);
    ListUploadJobsRsp response;
    response.jobs = std::move(page->jobs);
    response.more = page->more;
    co_return response;
  }

 private:
  const ListUploadJobsReq &req_;
};

MetaStore::OpPtr<ListUploadJobsRsp> MetaStore::listUploadJobs(const ListUploadJobsReq &req) {
  return std::make_unique<ListUploadJobsOp>(*this, req);
}

}  // namespace hf3fs::meta::server
