#include <algorithm>
#include <memory>
#include <string_view>
#include <sys/stat.h>

#include "meta/store/FileSession.h"
#include "meta/store/Inode.h"
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
  CO_RETURN_ON_ERROR(co_await FileSession::removeAll(txn, inode.id));
  CO_RETURN_ON_ERROR(co_await inode.store(txn));
  co_return Void{};
}

}  // namespace

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

class RecoverExpiredWriteStagingOp : public Operation<RecoverExpiredWriteStagingRsp> {
 public:
  RecoverExpiredWriteStagingOp(MetaStore &meta, const RecoverExpiredWriteStagingReq &req)
      : Operation<RecoverExpiredWriteStagingRsp>(meta),
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

    if ((job.state == cache::UploadJobState::SEALED || job.state == cache::UploadJobState::CANCELLED) &&
        job.stateVersion == req_.expectedStateVersion + 1 && (inode->acl.iflags & FS_IMMUTABLE_FL)) {
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

}  // namespace hf3fs::meta::server
