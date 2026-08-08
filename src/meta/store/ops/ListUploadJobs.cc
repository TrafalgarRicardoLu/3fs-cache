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

class GetUploadJobOp : public ReadOnlyOperation<GetUploadJobRsp> {
 public:
  GetUploadJobOp(MetaStore &meta, const GetUploadJobReq &req)
      : ReadOnlyOperation<GetUploadJobRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<GetUploadJobRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto job = co_await UploadJobStore::snapshotLoad(txn, req_.jobId);
    CO_RETURN_ON_ERROR(job);
    if (!job->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    if (!req_.user.isRoot() && (**job).ownerUid != req_.user.uid) {
      co_return makeError(MetaCode::kNoPermission, "upload job belongs to another user");
    }
    GetUploadJobRsp response;
    response.job = std::move(**job);
    co_return response;
  }

 private:
  const GetUploadJobReq &req_;
};

MetaStore::OpPtr<GetUploadJobRsp> MetaStore::getUploadJob(const GetUploadJobReq &req) {
  return std::make_unique<GetUploadJobOp>(*this, req);
}

}  // namespace hf3fs::meta::server
