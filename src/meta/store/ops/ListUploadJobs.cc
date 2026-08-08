#include <folly/experimental/coro/Collect.h>
#include <memory>

#include "meta/store/FileSession.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/PrefetchJobStore.h"
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

class AdminListUploadJobsOp : public ReadOnlyOperation<ListUploadJobsRsp> {
 public:
  AdminListUploadJobsOp(MetaStore &meta, const AdminListUploadJobsReq &req)
      : ReadOnlyOperation<ListUploadJobsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListUploadJobsRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    if (req_.jobId) {
      auto job = co_await UploadJobStore::snapshotLoad(txn, *req_.jobId);
      CO_RETURN_ON_ERROR(job);
      if (!job->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
      ListUploadJobsRsp response;
      response.jobs.push_back(std::move(**job));
      co_return response;
    }
    auto page = co_await UploadJobStore::snapshotList(txn, req_.ownerUid, req_.after, req_.limit, req_.includeTerminal);
    CO_RETURN_ON_ERROR(page);
    ListUploadJobsRsp response;
    response.jobs = std::move(page->jobs);
    response.more = page->more;
    co_return response;
  }

 private:
  const AdminListUploadJobsReq &req_;
};

MetaStore::OpPtr<ListUploadJobsRsp> MetaStore::adminListUploadJobs(const AdminListUploadJobsReq &req) {
  return std::make_unique<AdminListUploadJobsOp>(*this, req);
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
    auto [inode, session] =
        co_await folly::coro::collectAll(Inode::snapshotLoad(txn, InodeId{response.job.stagingInode}),
                                         FileSession::snapshotCheckExists(txn, InodeId{response.job.stagingInode}));
    CO_RETURN_ON_ERROR(inode);
    CO_RETURN_ON_ERROR(session);
    if (!inode->has_value()) {
      response.stagingCleanupState = cache::StagingCleanupState::COMPLETE;
    } else if (session->has_value()) {
      response.stagingCleanupState = cache::StagingCleanupState::WAITING_FOR_HANDLES;
    } else if (response.job.state == cache::UploadJobState::PUBLISHED && (**inode).nlink == 0) {
      response.stagingCleanupState = cache::StagingCleanupState::QUEUED;
    } else {
      response.stagingCleanupState = cache::StagingCleanupState::RETAINED;
    }
    if (response.job.state == cache::UploadJobState::PUBLISHED) {
      response.cleanupPolicy = cache::UploadCleanupPolicy::DELETE_STAGING_AFTER_LAST_HANDLE;
    } else if (response.job.completedObject) {
      response.cleanupPolicy = cache::UploadCleanupPolicy::RETAIN_ORPHAN_FOR_OPERATOR;
    } else {
      response.cleanupPolicy = cache::UploadCleanupPolicy::RETAIN_UNTIL_TERMINAL;
    }
    if (response.job.state == cache::UploadJobState::PUBLISHED) {
      auto prefetchId = cache::publishedPrefetchJobId(response.job.jobId);
      auto prefetch = co_await PrefetchJobStore::snapshotLoad(txn, prefetchId);
      CO_RETURN_ON_ERROR(prefetch);
      if (prefetch->has_value()) {
        response.prefetchJobId = prefetchId;
        response.warmState = cache::UploadWarmState::SUBMITTED;
      }
    }
    co_return response;
  }

 private:
  const GetUploadJobReq &req_;
};

MetaStore::OpPtr<GetUploadJobRsp> MetaStore::getUploadJob(const GetUploadJobReq &req) {
  return std::make_unique<GetUploadJobOp>(*this, req);
}

}  // namespace hf3fs::meta::server
