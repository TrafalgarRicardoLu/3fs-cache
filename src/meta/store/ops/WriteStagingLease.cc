#include <algorithm>
#include <limits>
#include <memory>
#include <string_view>
#include <sys/stat.h>

#include "meta/store/Inode.h"
#include "meta/store/DirEntry.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/UploadJobStore.h"

namespace hf3fs::meta::server {
namespace {

uint64_t nowMs() {
  auto nowUs = UtcClock::now().toMicroseconds();
  return nowUs > 0 ? static_cast<uint64_t>(nowUs / 1000) : 0;
}

Result<Void> checkOwner(const cache::UploadJobRecord &job, const UserInfo &user) {
  if (!user.isRoot() && job.ownerUid != user.uid) {
    return makeError(MetaCode::kNoPermission, "upload job belongs to another user");
  }
  return Void{};
}

Result<Void> checkStagingInode(const Inode &inode, const cache::UploadJobRecord &job, ChainAllocator &chainAlloc) {
  if (!inode.isFile() || inode.id.u64() != job.stagingInode) {
    return makeError(CacheCode::kStateConflict, "upload job staging inode changed");
  }
  auto routing = chainAlloc.getRoutingInfo();
  auto table = routing && routing->raw() ? routing->raw()->getChainTable(inode.fileLayout().tableId) : nullptr;
  if (table == nullptr || !table->isWriteStaging()) {
    return makeError(MetaCode::kInvalidFileLayout, "upload job inode is not on WRITE_STAGING");
  }
  return Void{};
}

Result<Void> checkSealRetry(const cache::UploadJobRecord &job,
                            const Inode &inode,
                            uint64_t expectedStateVersion,
                            const VersionedLength &finalLength) {
  if (job.state != cache::UploadJobState::SEALED || job.stateVersion != expectedStateVersion + 1 ||
      job.stagingLength != finalLength.length || inode.asFile().getVersionedLength() != finalLength ||
      !(inode.acl.iflags & FS_IMMUTABLE_FL)) {
    return makeError(CacheCode::kStateConflict, "upload job seal fence changed");
  }
  return Void{};
}

CoTryTask<Void> freeze(IReadWriteTransaction &txn,
                       Inode &inode,
                       cache::UploadJobRecord &job,
                       cache::UploadJobState state,
                       uint64_t updatedAtMs) {
  inode.acl.iflags = IFlags(inode.acl.iflags | FS_IMMUTABLE_FL);
  job.stagingLength = inode.asFile().length;
  job.state = state;
  ++job.stateVersion;
  job.updatedAtMs = std::max(job.updatedAtMs, updatedAtMs);
  job.writerLeaseId = Uuid::zero();
  job.writerLeaseExpiresAtMs = 0;
  CO_RETURN_ON_ERROR(co_await inode.store(txn));
  co_return Void{};
}

}  // namespace

template <typename Rsp>
class WriteStagingOperation : public Operation<Rsp> {
 public:
  using Operation<Rsp>::Operation;

 protected:
  CoTryTask<void> cleanupStaging(IReadWriteTransaction &txn, const cache::UploadJobRecord &job) {
    auto inode = co_await Inode::load(txn, InodeId{job.stagingInode});
    CO_RETURN_ON_ERROR(inode);
    if (!inode->has_value()) co_return Void{};
    CO_RETURN_ON_ERROR(checkStagingInode(**inode, job, this->chainAlloc()));

    auto path = PathAt(job.path);
    auto resolved = co_await this->resolve(txn, UserInfo{}).path(path, AtFlags(AT_SYMLINK_NOFOLLOW));
    CO_RETURN_ON_ERROR(resolved);
    if (!resolved->dirEntry) {
      if ((**inode).nlink == 0) co_return Void{};
      co_return makeError(CacheCode::kStateConflict, "cancelled upload path no longer references staging inode");
    }
    if (resolved->dirEntry->id.u64() != job.stagingInode || !resolved->dirEntry->isFile()) {
      co_return makeError(CacheCode::kStateConflict, "cancelled upload path changed before cleanup");
    }
    auto name = path.path->filename().native();
    (**inode).acl.iflags = IFlags((**inode).acl.iflags & ~FS_IMMUTABLE_FL);
    CO_RETURN_ON_ERROR(
        co_await this->gcManager().removeEntry(txn, *resolved->dirEntry, **inode, GcInfo{job.ownerUid, name}));
    co_return Void{};
  }
};

class RenewWriteStagingLeaseOp : public Operation<RenewWriteStagingLeaseRsp> {
 public:
  RenewWriteStagingLeaseOp(MetaStore &meta, const RenewWriteStagingLeaseReq &req)
      : Operation<RenewWriteStagingLeaseRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<RenewWriteStagingLeaseRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    CO_RETURN_ON_ERROR(checkOwner(job, req_.user));

    if (job.state == cache::UploadJobState::OPEN && job.stateVersion == req_.expectedStateVersion + 1 &&
        job.writerLeaseId == req_.writerLeaseId && job.writerLeaseExpiresAtMs == req_.writerLeaseExpiresAtMs) {
      RenewWriteStagingLeaseRsp response;
      response.job = std::move(job);
      co_return response;
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    if (job.state != cache::UploadJobState::OPEN || job.stateVersion != req_.expectedStateVersion ||
        job.writerLeaseId != req_.writerLeaseId || job.writerLeaseExpiresAtMs <= now ||
        req_.writerLeaseExpiresAtMs <= job.writerLeaseExpiresAtMs || req_.writerLeaseExpiresAtMs <= now) {
      co_return makeError(CacheCode::kStateConflict, "upload writer lease fence changed or expired");
    }
    job.stateVersion++;
    job.updatedAtMs = std::max(job.updatedAtMs, now);
    job.writerLeaseExpiresAtMs = req_.writerLeaseExpiresAtMs;
    auto updated = co_await UploadJobStore::update(txn, req_.expectedStateVersion, job);
    CO_RETURN_ON_ERROR(updated);
    RenewWriteStagingLeaseRsp response;
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  const RenewWriteStagingLeaseReq &req_;
};

class SealWriteStagingOp : public Operation<SealWriteStagingRsp> {
 public:
  SealWriteStagingOp(MetaStore &meta, const SealWriteStagingReq &req)
      : Operation<SealWriteStagingRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<SealWriteStagingRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    CO_RETURN_ON_ERROR(checkOwner(job, req_.user));
    if (job.stagingInode != req_.stagingInode.u64()) {
      co_return makeError(CacheCode::kStateConflict, "upload job staging inode fence changed");
    }
    auto inode = (co_await Inode::load(txn, req_.stagingInode)).then(checkMetaFound<Inode>);
    CO_RETURN_ON_ERROR(inode);
    CO_RETURN_ON_ERROR(checkStagingInode(*inode, job, chainAlloc()));

    if (job.state != cache::UploadJobState::OPEN) {
      CO_RETURN_ON_ERROR(checkSealRetry(job, *inode, req_.expectedStateVersion, req_.finalLength));
      SealWriteStagingRsp response;
      response.inode = std::move(*inode);
      response.job = std::move(job);
      co_return response;
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    if (job.stateVersion != req_.expectedStateVersion || job.writerLeaseId != req_.writerLeaseId ||
        job.writerLeaseExpiresAtMs <= now) {
      co_return makeError(CacheCode::kStateConflict, "upload writer lease fence changed or expired");
    }
    if (inode->asFile().getVersionedLength() != req_.finalLength) {
      co_return makeError(CacheCode::kStateConflict, "staging inode length changed before seal");
    }
    if (inode->asFile().hasHole()) {
      co_return makeError(MetaCode::kFileHasHole, "write staging inode contains a hole");
    }
    CO_RETURN_ON_ERROR(co_await freeze(txn, *inode, job, cache::UploadJobState::SEALED, now));
    auto updated = co_await UploadJobStore::update(txn, req_.expectedStateVersion, job);
    CO_RETURN_ON_ERROR(updated);
    SealWriteStagingRsp response;
    response.inode = std::move(*inode);
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  const SealWriteStagingReq &req_;
};

class RecoverExpiredWriteStagingOp : public WriteStagingOperation<RecoverExpiredWriteStagingRsp> {
 public:
  RecoverExpiredWriteStagingOp(MetaStore &meta, const RecoverExpiredWriteStagingReq &req)
      : WriteStagingOperation<RecoverExpiredWriteStagingRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<RecoverExpiredWriteStagingRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    CO_RETURN_ON_ERROR(checkOwner(job, req_.user));
    auto inodeId = InodeId(job.stagingInode);
    auto inode = (co_await Inode::load(txn, inodeId)).then(checkMetaFound<Inode>);
    CO_RETURN_ON_ERROR(inode);
    CO_RETURN_ON_ERROR(checkStagingInode(*inode, job, chainAlloc()));

    auto recoveredRetry =
        (job.state == cache::UploadJobState::SEALED && (inode->acl.iflags & FS_IMMUTABLE_FL)) ||
        (job.state == cache::UploadJobState::CANCELLED && inode->nlink == 0);
    if (recoveredRetry && job.stateVersion == req_.expectedStateVersion + 1) {
      if (job.state == cache::UploadJobState::CANCELLED) {
        CO_RETURN_ON_ERROR(co_await cleanupStaging(txn, job));
      }
      RecoverExpiredWriteStagingRsp response;
      response.inode = std::move(*inode);
      response.job = std::move(job);
      co_return response;
    }
    auto action = config().write_staging_expired_action();
    auto recoveredState = action == "recover" ? cache::UploadJobState::SEALED : cache::UploadJobState::CANCELLED;
    if (action != "recover" && action != "cancel") {
      co_return makeError(StatusCode::kInvalidConfig, "write_staging_expired_action must be recover or cancel");
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    if (job.state != cache::UploadJobState::OPEN || job.stateVersion != req_.expectedStateVersion ||
        job.writerLeaseId != req_.expectedWriterLeaseId ||
        job.writerLeaseExpiresAtMs != req_.expectedWriterLeaseExpiresAtMs || job.writerLeaseExpiresAtMs > now) {
      co_return makeError(CacheCode::kStateConflict, "expired upload writer lease fence changed");
    }
    if (recoveredState == cache::UploadJobState::SEALED && inode->asFile().hasHole()) {
      co_return makeError(MetaCode::kFileHasHole, "expired write staging inode contains a hole");
    }
    auto expectedVersion = job.stateVersion;
    CO_RETURN_ON_ERROR(co_await freeze(txn, *inode, job, recoveredState, now));
    if (recoveredState == cache::UploadJobState::CANCELLED) {
      CO_RETURN_ON_ERROR(co_await cleanupStaging(txn, job));
      auto cleaned = (co_await Inode::load(txn, inodeId)).then(checkMetaFound<Inode>);
      CO_RETURN_ON_ERROR(cleaned);
      inode = std::move(cleaned);
    }
    auto updated = co_await UploadJobStore::update(txn, expectedVersion, job);
    CO_RETURN_ON_ERROR(updated);
    RecoverExpiredWriteStagingRsp response;
    response.inode = std::move(*inode);
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  const RecoverExpiredWriteStagingReq &req_;
};

class BeginMultipartUploadOp : public Operation<BeginMultipartUploadRsp> {
 public:
  BeginMultipartUploadOp(MetaStore &meta, const BeginMultipartUploadReq &req)
      : Operation<BeginMultipartUploadRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<BeginMultipartUploadRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    if (job.state == cache::UploadJobState::UPLOADING && job.stateVersion == req_.expectedStateVersion + 1 &&
        job.multipartId == req_.multipartId) {
      BeginMultipartUploadRsp response;
      response.job = std::move(job);
      co_return response;
    }
    if (job.state != cache::UploadJobState::SEALED || job.stateVersion != req_.expectedStateVersion ||
        !job.multipartId.empty() || !job.parts.empty() || job.nextPartNumber != 1) {
      co_return makeError(CacheCode::kStateConflict, "upload job start fence changed");
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    job.state = cache::UploadJobState::UPLOADING;
    job.stateVersion++;
    job.updatedAtMs = std::max(job.updatedAtMs, now);
    job.multipartId = req_.multipartId;
    auto updated = co_await UploadJobStore::update(txn, req_.expectedStateVersion, job);
    CO_RETURN_ON_ERROR(updated);
    BeginMultipartUploadRsp response;
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  const BeginMultipartUploadReq &req_;
};

class CheckpointUploadPartOp : public Operation<CheckpointUploadPartRsp> {
 public:
  CheckpointUploadPartOp(MetaStore &meta, const CheckpointUploadPartReq &req)
      : Operation<CheckpointUploadPartRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<CheckpointUploadPartRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    if (job.state == cache::UploadJobState::UPLOADING && job.stateVersion == req_.expectedStateVersion + 1 &&
        job.multipartId == req_.multipartId && req_.part.partNumber <= job.parts.size() &&
        job.parts[req_.part.partNumber - 1] == req_.part) {
      CheckpointUploadPartRsp response;
      response.job = std::move(job);
      co_return response;
    }
    if (job.state != cache::UploadJobState::UPLOADING || job.stateVersion != req_.expectedStateVersion ||
        job.multipartId != req_.multipartId || req_.part.partNumber != job.nextPartNumber) {
      co_return makeError(CacheCode::kStateConflict, "upload part checkpoint fence changed");
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    job.parts.push_back(req_.part);
    job.nextPartNumber++;
    job.stateVersion++;
    job.updatedAtMs = std::max(job.updatedAtMs, now);
    auto updated = co_await UploadJobStore::update(txn, req_.expectedStateVersion, job);
    CO_RETURN_ON_ERROR(updated);
    CheckpointUploadPartRsp response;
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  const CheckpointUploadPartReq &req_;
};

class MutateMultipartUploadOp : public WriteStagingOperation<MutateMultipartUploadRsp> {
 public:
  MutateMultipartUploadOp(MetaStore &meta, const MutateMultipartUploadReq &req)
      : WriteStagingOperation<MutateMultipartUploadRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<MutateMultipartUploadRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    if (job.multipartId != req_.multipartId) {
      co_return makeError(CacheCode::kStateConflict, "multipart upload identity changed");
    }
    if (job.stateVersion == req_.expectedStateVersion + 1 && retryMatches(job)) {
      if (req_.mutation == MultipartUploadMutation::FINISH_ABORT) {
        CO_RETURN_ON_ERROR(co_await cleanupStaging(txn, job));
      }
      MutateMultipartUploadRsp response;
      response.job = std::move(job);
      co_return response;
    }
    if (job.stateVersion != req_.expectedStateVersion) {
      co_return makeError(CacheCode::kStateConflict, "multipart upload mutation fence changed");
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    CO_RETURN_ON_ERROR(apply(job, now));
    if (req_.mutation == MultipartUploadMutation::FINISH_ABORT) {
      CO_RETURN_ON_ERROR(co_await cleanupStaging(txn, job));
    }
    auto expectedVersion = job.stateVersion;
    job.stateVersion++;
    job.updatedAtMs = std::max(job.updatedAtMs, now);
    auto updated = co_await UploadJobStore::update(txn, expectedVersion, job);
    CO_RETURN_ON_ERROR(updated);
    if (req_.mutation == MultipartUploadMutation::MARK_PREFETCH_SUBMITTED) {
      CO_RETURN_ON_ERROR(co_await UploadJobStore::removeActive(txn, job.jobId));
    }
    MutateMultipartUploadRsp response;
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  bool retryMatches(const cache::UploadJobRecord &job) const {
    switch (req_.mutation) {
      case MultipartUploadMutation::PREPARE_COMPLETE:
        return job.state == cache::UploadJobState::COMPLETING;
      case MultipartUploadMutation::SAVE_COMPLETED:
        return job.state == cache::UploadJobState::PUBLISHING && job.completedObject == req_.completedObject;
      case MultipartUploadMutation::BEGIN_ABORT:
        return job.state == cache::UploadJobState::ABORTING && job.error == req_.error;
      case MultipartUploadMutation::FINISH_ABORT:
        return job.state == cache::UploadJobState::CANCELLED;
      case MultipartUploadMutation::MARK_PREFETCH_SUBMITTED:
        return job.state == cache::UploadJobState::PUBLISHED;
      case MultipartUploadMutation::FAIL_PUBLISH:
        return job.state == cache::UploadJobState::FAILED && job.error == req_.error &&
               job.orphanCleanupState == cache::OrphanCleanupState::PENDING &&
               job.orphanCleanupEligibleAtMs == req_.orphanCleanupEligibleAtMs;
      case MultipartUploadMutation::BEGIN_ORPHAN_DELETE:
        return (job.orphanCleanupState == cache::OrphanCleanupState::PENDING &&
                job.orphanCleanupEligibleAtMs == req_.orphanCleanupEligibleAtMs) ||
               (job.orphanCleanupState == cache::OrphanCleanupState::DELETING &&
                job.orphanCleanupOperationId == req_.orphanCleanupOperationId);
      case MultipartUploadMutation::FINISH_ORPHAN_DELETE:
        return job.orphanCleanupState == cache::OrphanCleanupState::COMPLETE &&
               job.orphanCleanupOperationId == req_.orphanCleanupOperationId;
      case MultipartUploadMutation::FAIL_ORPHAN_DELETE:
        return job.orphanCleanupState == cache::OrphanCleanupState::CONFLICT &&
               job.orphanCleanupOperationId == req_.orphanCleanupOperationId;
      default:
        return false;
    }
  }

  Result<Void> apply(cache::UploadJobRecord &job, uint64_t now) const {
    uint64_t uploaded = 0;
    for (const auto &part : job.parts) uploaded += part.size;
    switch (req_.mutation) {
      case MultipartUploadMutation::PREPARE_COMPLETE:
        if (job.state != cache::UploadJobState::UPLOADING || uploaded != job.stagingLength) {
          return makeError(CacheCode::kStateConflict, "multipart upload is not fully checkpointed");
        }
        job.state = cache::UploadJobState::COMPLETING;
        return Void{};
      case MultipartUploadMutation::SAVE_COMPLETED:
        if (job.state != cache::UploadJobState::COMPLETING || !req_.completedObject ||
            req_.completedObject->originId != job.destination.originId ||
            req_.completedObject->bucket != job.destination.bucket ||
            req_.completedObject->key != job.destination.key || uploaded != job.stagingLength) {
          return makeError(CacheCode::kStateConflict, "completed object does not match staged upload");
        }
        job.completedObject = req_.completedObject;
        job.state = cache::UploadJobState::PUBLISHING;
        return Void{};
      case MultipartUploadMutation::BEGIN_ABORT:
        if (job.state != cache::UploadJobState::UPLOADING && job.state != cache::UploadJobState::COMPLETING) {
          return makeError(CacheCode::kStateConflict, "multipart upload cannot enter aborting");
        }
        job.error = req_.error;
        job.state = cache::UploadJobState::ABORTING;
        return Void{};
      case MultipartUploadMutation::FINISH_ABORT:
        if (job.state != cache::UploadJobState::ABORTING) {
          return makeError(CacheCode::kStateConflict, "multipart upload is not aborting");
        }
        job.state = cache::UploadJobState::CANCELLED;
        return Void{};
      case MultipartUploadMutation::MARK_PREFETCH_SUBMITTED:
        if (job.state != cache::UploadJobState::PUBLISHED) {
          return makeError(CacheCode::kStateConflict, "upload is not published");
        }
        return Void{};
      case MultipartUploadMutation::FAIL_PUBLISH:
        if (job.state != cache::UploadJobState::PUBLISHING || !job.completedObject || job.publishedInode != 0) {
          return makeError(CacheCode::kStateConflict, "only an unpublished completed object can fail publishing");
        }
        job.state = cache::UploadJobState::FAILED;
        job.error = req_.error;
        job.orphanCleanupState = cache::OrphanCleanupState::PENDING;
        job.orphanCleanupEligibleAtMs = req_.orphanCleanupEligibleAtMs;
        return Void{};
      case MultipartUploadMutation::BEGIN_ORPHAN_DELETE:
        if ((job.state != cache::UploadJobState::FAILED && job.state != cache::UploadJobState::CANCELLED) ||
            !job.completedObject || job.publishedInode != 0 ||
            (job.orphanCleanupState != cache::OrphanCleanupState::NONE &&
             job.orphanCleanupState != cache::OrphanCleanupState::PENDING)) {
          return makeError(CacheCode::kStateConflict, "upload object is not eligible for orphan cleanup");
        }
        if (job.orphanCleanupState == cache::OrphanCleanupState::NONE) {
          job.orphanCleanupState = cache::OrphanCleanupState::PENDING;
          job.orphanCleanupEligibleAtMs = req_.orphanCleanupEligibleAtMs;
        } else if (job.orphanCleanupEligibleAtMs != req_.orphanCleanupEligibleAtMs) {
          return makeError(CacheCode::kStateConflict, "orphan cleanup eligibility changed");
        }
        if (now >= job.orphanCleanupEligibleAtMs) {
          if (job.orphanCleanupAttempts == std::numeric_limits<uint32_t>::max()) {
            return makeError(CacheCode::kStateConflict, "orphan cleanup attempt counter exhausted");
          }
          job.orphanCleanupState = cache::OrphanCleanupState::DELETING;
          job.orphanCleanupOperationId = req_.orphanCleanupOperationId;
          ++job.orphanCleanupAttempts;
        }
        return Void{};
      case MultipartUploadMutation::FINISH_ORPHAN_DELETE:
        if ((job.state != cache::UploadJobState::FAILED && job.state != cache::UploadJobState::CANCELLED) ||
            job.orphanCleanupState != cache::OrphanCleanupState::DELETING ||
            job.orphanCleanupOperationId != req_.orphanCleanupOperationId || !job.completedObject ||
            job.publishedInode != 0) {
          return makeError(CacheCode::kStateConflict, "orphan cleanup completion fence changed");
        }
        job.orphanCleanupState = cache::OrphanCleanupState::COMPLETE;
        return Void{};
      case MultipartUploadMutation::FAIL_ORPHAN_DELETE:
        if ((job.state != cache::UploadJobState::FAILED && job.state != cache::UploadJobState::CANCELLED) ||
            job.orphanCleanupState != cache::OrphanCleanupState::DELETING ||
            job.orphanCleanupOperationId != req_.orphanCleanupOperationId) {
          return makeError(CacheCode::kStateConflict, "orphan cleanup failure fence changed");
        }
        job.orphanCleanupState = cache::OrphanCleanupState::CONFLICT;
        job.error = req_.error;
        return Void{};
      default:
        return makeError(StatusCode::kInvalidArg, "invalid multipart upload mutation");
    }
  }

  const MutateMultipartUploadReq &req_;
};

class AdminMutateUploadJobOp : public WriteStagingOperation<AdminMutateUploadJobRsp> {
 public:
  AdminMutateUploadJobOp(MetaStore &meta, const AdminMutateUploadJobReq &req)
      : WriteStagingOperation<AdminMutateUploadJobRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<AdminMutateUploadJobRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    if (job.stateVersion != req_.expectedStateVersion) {
      co_return makeError(CacheCode::kStateConflict, "admin upload mutation fence changed");
    }
    CO_RETURN_ON_ERROR(apply(job));
    if (job.state == cache::UploadJobState::CANCELLED) {
      CO_RETURN_ON_ERROR(co_await cleanupStaging(txn, job));
    }
    auto now = nowMs();
    if (now == 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock");
    auto expectedVersion = job.stateVersion++;
    job.updatedAtMs = std::max(job.updatedAtMs, now);
    auto updated = co_await UploadJobStore::update(txn, expectedVersion, job);
    CO_RETURN_ON_ERROR(updated);
    AdminMutateUploadJobRsp response;
    response.job = std::move(*updated);
    co_return response;
  }

 private:
  Result<Void> apply(cache::UploadJobRecord &job) const {
    if (req_.mutation == AdminUploadMutation::CANCEL) {
      if (job.state == cache::UploadJobState::PUBLISHED || job.state == cache::UploadJobState::PUBLISHING) {
        return makeError(CacheCode::kStateConflict, "published upload cannot be cancelled");
      }
      if (job.state == cache::UploadJobState::CANCELLED || job.state == cache::UploadJobState::ABORTING) {
        return makeError(CacheCode::kStateConflict, "upload cancellation is already terminal or in progress");
      }
      job.writerLeaseId = Uuid::zero();
      job.writerLeaseExpiresAtMs = 0;
      job.error = "cancelled by cache administrator";
      job.state = job.multipartId.empty() ? cache::UploadJobState::CANCELLED : cache::UploadJobState::ABORTING;
      return Void{};
    }
    if (req_.mutation == AdminUploadMutation::RETRY) {
      if (job.state != cache::UploadJobState::FAILED) {
        return makeError(CacheCode::kStateConflict, "only failed uploads can be retried");
      }
      if (job.orphanCleanupState == cache::OrphanCleanupState::DELETING ||
          job.orphanCleanupState == cache::OrphanCleanupState::COMPLETE ||
          job.orphanCleanupState == cache::OrphanCleanupState::CONFLICT) {
        return makeError(CacheCode::kStateConflict, "failed upload object cleanup has already started");
      }
      uint64_t uploaded = 0;
      for (const auto &part : job.parts) uploaded += part.size;
      job.error.clear();
      job.orphanCleanupState = cache::OrphanCleanupState::NONE;
      job.orphanCleanupOperationId = Uuid::zero();
      job.orphanCleanupEligibleAtMs = 0;
      job.orphanCleanupAttempts = 0;
      if (job.completedObject) {
        job.state = cache::UploadJobState::PUBLISHING;
      } else if (job.multipartId.empty()) {
        job.state = cache::UploadJobState::SEALED;
      } else if (uploaded == job.stagingLength) {
        job.state = cache::UploadJobState::COMPLETING;
      } else {
        job.state = cache::UploadJobState::UPLOADING;
      }
      return Void{};
    }
    return makeError(StatusCode::kInvalidArg, "invalid admin upload mutation");
  }

  const AdminMutateUploadJobReq &req_;
};

class FinalizeCancelledUploadOp : public WriteStagingOperation<FinalizeCancelledUploadRsp> {
 public:
  FinalizeCancelledUploadOp(MetaStore &meta, const FinalizeCancelledUploadReq &req)
      : WriteStagingOperation<FinalizeCancelledUploadRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<FinalizeCancelledUploadRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);
    if (job.state != cache::UploadJobState::CANCELLED || job.stateVersion != req_.expectedStateVersion) {
      co_return makeError(CacheCode::kStateConflict, "cancelled upload cleanup fence changed");
    }
    CO_RETURN_ON_ERROR(co_await cleanupStaging(txn, job));
    FinalizeCancelledUploadRsp response;
    response.job = std::move(job);
    co_return response;
  }

 private:
  const FinalizeCancelledUploadReq &req_;
};

MetaStore::OpPtr<RenewWriteStagingLeaseRsp> MetaStore::renewWriteStagingLease(const RenewWriteStagingLeaseReq &req) {
  return std::make_unique<RenewWriteStagingLeaseOp>(*this, req);
}

MetaStore::OpPtr<SealWriteStagingRsp> MetaStore::sealWriteStaging(const SealWriteStagingReq &req) {
  return std::make_unique<SealWriteStagingOp>(*this, req);
}

MetaStore::OpPtr<RecoverExpiredWriteStagingRsp> MetaStore::recoverExpiredWriteStaging(
    const RecoverExpiredWriteStagingReq &req) {
  return std::make_unique<RecoverExpiredWriteStagingOp>(*this, req);
}

MetaStore::OpPtr<BeginMultipartUploadRsp> MetaStore::beginMultipartUpload(const BeginMultipartUploadReq &req) {
  return std::make_unique<BeginMultipartUploadOp>(*this, req);
}

MetaStore::OpPtr<CheckpointUploadPartRsp> MetaStore::checkpointUploadPart(const CheckpointUploadPartReq &req) {
  return std::make_unique<CheckpointUploadPartOp>(*this, req);
}

MetaStore::OpPtr<MutateMultipartUploadRsp> MetaStore::mutateMultipartUpload(const MutateMultipartUploadReq &req) {
  return std::make_unique<MutateMultipartUploadOp>(*this, req);
}

MetaStore::OpPtr<AdminMutateUploadJobRsp> MetaStore::adminMutateUploadJob(const AdminMutateUploadJobReq &req) {
  return std::make_unique<AdminMutateUploadJobOp>(*this, req);
}

MetaStore::OpPtr<FinalizeCancelledUploadRsp> MetaStore::finalizeCancelledUpload(
    const FinalizeCancelledUploadReq &req) {
  return std::make_unique<FinalizeCancelledUploadOp>(*this, req);
}

}  // namespace hf3fs::meta::server
