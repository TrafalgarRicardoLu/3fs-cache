#include <folly/experimental/coro/Collect.h>
#include <memory>

#include "meta/store/FileSession.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/PrefetchJobStore.h"
#include "meta/store/cache/UploadJobStore.h"

namespace hf3fs::meta::server {

class ListUploadJobsOp : public Operation<ListUploadJobsRsp> {
 public:
  ListUploadJobsOp(MetaStore &meta, const ListUploadJobsReq &req)
      : Operation<ListUploadJobsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListUploadJobsRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await UploadJobStore::snapshotList(txn, req_.ownerUid, req_.after, req_.limit, req_.includeTerminal);
    CO_RETURN_ON_ERROR(page);
    ListUploadJobsRsp response;
    response.jobs = std::move(page->jobs);
    response.more = page->more;
    if (req_.includeTerminal) {
      // The startup recovery scan also migrates jobs created before the
      // active-index rollout. Periodic scans can then stay history-bounded.
      for (const auto &listed : response.jobs) {
        auto loaded = co_await UploadJobStore::load(txn, listed.jobId);
        CO_RETURN_ON_ERROR(loaded);
        if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job disappeared during migration");
        const auto &job = **loaded;
        CO_RETURN_ON_ERROR(co_await UploadJobStore::indexActive(txn, job));
        if (job.state == cache::UploadJobState::PUBLISHED) {
          auto prefetch = co_await PrefetchJobStore::snapshotLoad(txn, cache::publishedPrefetchJobId(job.jobId));
          CO_RETURN_ON_ERROR(prefetch);
          if (prefetch->has_value()) {
            CO_RETURN_ON_ERROR(co_await UploadJobStore::removeActive(txn, job.jobId));
            continue;
          }
        }
      }
      if (!response.more) CO_RETURN_ON_ERROR(co_await UploadJobStore::finishStateIndexMigration(txn));
    }
    co_return response;
  }

 private:
  const ListUploadJobsReq &req_;
};

MetaStore::OpPtr<ListUploadJobsRsp> MetaStore::listUploadJobs(const ListUploadJobsReq &req) {
  return std::make_unique<ListUploadJobsOp>(*this, req);
}

class ListExpiredOpenUploadsOp : public ReadOnlyOperation<ListExpiredOpenUploadsRsp> {
 public:
  ListExpiredOpenUploadsOp(MetaStore &meta, const ListExpiredOpenUploadsReq &req)
      : ReadOnlyOperation<ListExpiredOpenUploadsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListExpiredOpenUploadsRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await UploadJobStore::snapshotListExpiredOpen(txn,
                                                                 req_.expiresBeforeMs,
                                                                 req_.after,
                                                                 req_.limit);
    CO_RETURN_ON_ERROR(page);
    ListExpiredOpenUploadsRsp response;
    response.jobs = std::move(page->jobs);
    response.more = page->more;
    co_return response;
  }

 private:
  const ListExpiredOpenUploadsReq &req_;
};

MetaStore::OpPtr<ListExpiredOpenUploadsRsp> MetaStore::listExpiredOpenUploads(
    const ListExpiredOpenUploadsReq &req) {
  return std::make_unique<ListExpiredOpenUploadsOp>(*this, req);
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
    } else if ((response.job.state == cache::UploadJobState::PUBLISHED ||
                response.job.state == cache::UploadJobState::CANCELLED) &&
               (**inode).nlink == 0) {
      response.stagingCleanupState = cache::StagingCleanupState::QUEUED;
    } else {
      response.stagingCleanupState = cache::StagingCleanupState::RETAINED;
    }
    if (response.job.orphanCleanupState == cache::OrphanCleanupState::DELETING) {
      response.cleanupPolicy = cache::UploadCleanupPolicy::DELETE_ORPHAN_IN_PROGRESS;
    } else if (response.job.orphanCleanupState == cache::OrphanCleanupState::COMPLETE) {
      response.cleanupPolicy = cache::UploadCleanupPolicy::DELETE_ORPHAN_COMPLETE;
    } else if ((response.job.state == cache::UploadJobState::FAILED ||
                response.job.state == cache::UploadJobState::CANCELLED) &&
               response.job.completedObject &&
               response.job.orphanCleanupState != cache::OrphanCleanupState::CONFLICT) {
      response.cleanupPolicy = cache::UploadCleanupPolicy::DELETE_ORPHAN_PENDING;
    } else if (response.job.state == cache::UploadJobState::PUBLISHED ||
               response.job.state == cache::UploadJobState::CANCELLED) {
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
