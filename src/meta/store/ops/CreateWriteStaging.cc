#include <memory>
#include <sys/stat.h>

#include "meta/store/DirEntry.h"
#include "meta/store/FileSession.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/UploadJobStore.h"

namespace hf3fs::meta::server {
namespace {

Acl makeStagingAcl(const Inode &parent, const UserInfo &user, Permission permission) {
  Acl acl(user.uid,
          user.gid,
          Permission(permission.toUnderType() & ALLPERMS),
          IFlags(parent.acl.iflags & FS_FL_INHERITABLE));
  if (parent.acl.perm & S_ISGID) acl.gid = parent.acl.gid;
  return acl;
}

bool matchesRetry(const Inode &inode,
                  const cache::UploadJobRecord &job,
                  const CreateWriteStagingReq &req,
                  std::string_view normalizedPath) {
  if (!inode.isFile() || job.jobId != req.jobId || job.ownerUid != req.user.uid || job.path != normalizedPath ||
      job.stagingInode != inode.id.u64() || job.destination != req.destination) {
    return false;
  }
  const auto &layout = inode.asFile().layout;
  if (layout.tableId != req.tableId || layout.chunkSize.u32() != req.chunkSize || layout.stripeSize != req.stripeSize ||
      inode.acl.perm != Permission(req.permission.toUnderType() & ALLPERMS)) {
    return false;
  }
  return job.state != cache::UploadJobState::OPEN || job.writerLeaseId == req.writerLeaseId;
}

}  // namespace

class CreateWriteStagingOp : public Operation<CreateWriteStagingRsp> {
 public:
  CreateWriteStagingOp(MetaStore &meta, const CreateWriteStagingReq &req)
      : Operation<CreateWriteStagingRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<CreateWriteStagingRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto normalizedPath = req_.path.path->lexically_normal().generic_string();

    auto resolved = co_await resolve(txn, req_.user).path(req_.path, AtFlags(AT_SYMLINK_NOFOLLOW));
    CO_RETURN_ON_ERROR(resolved);
    if (resolved->dirEntry.has_value()) {
      auto inode = co_await resolved->dirEntry->snapshotLoadInode(txn);
      CO_RETURN_ON_ERROR(inode);
      auto job = co_await UploadJobStore::load(txn, req_.jobId);
      CO_RETURN_ON_ERROR(job);
      if (!job->has_value() || !matchesRetry(*inode, **job, req_, normalizedPath)) {
        co_return makeError(MetaCode::kExists, "write staging path or job identity conflicts");
      }
      CreateWriteStagingRsp rsp;
      rsp.inode = std::move(*inode);
      rsp.job = std::move(**job);
      rsp.created = false;
      co_return rsp;
    }

    auto parent = co_await resolved->getParentInode(txn);
    CO_RETURN_ON_ERROR(parent);
    CO_RETURN_ON_ERROR(parent->acl.checkPermission(req_.user, AccessType::WRITE));
    CO_RETURN_ON_ERROR(parent->asDirectory().checkLock(req_.client));

    auto routing = chainAlloc().getRoutingInfo();
    auto table = routing && routing->raw() ? routing->raw()->getChainTable(req_.tableId) : nullptr;
    if (table == nullptr || !table->isWriteStaging()) {
      co_return makeError(MetaCode::kInvalidFileLayout, "write staging requires a WRITE_STAGING chain table");
    }

    auto layout = Layout::newEmpty(req_.tableId, req_.chunkSize, req_.stripeSize);
    CO_RETURN_ON_ERROR(co_await chainAlloc().allocateChainsForLayout(layout));
    auto newChunkEngine = config().enable_new_chunk_engine() || (parent->acl.iflags & FS_NEW_CHUNK_ENGINE);
    auto inodeId = co_await allocateInodeId(txn, newChunkEngine);
    CO_RETURN_ON_ERROR(inodeId);

    auto parentId = resolved->getParentId();
    auto name = req_.path.path->filename().native();
    auto inode =
        Inode::newFile(*inodeId, makeStagingAcl(*parent, req_.user, req_.permission), std::move(layout), now());
    auto entry = DirEntry::newFile(parentId, std::move(name), *inodeId);
    entry.uuid = req_.jobId.toUnderType();

    auto nowUs = now().toMicroseconds();
    if (nowUs <= 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock for upload job");
    cache::UploadJobRecord job;
    job.jobId = req_.jobId;
    job.ownerUid = req_.user.uid;
    job.path = std::move(normalizedPath);
    job.stagingInode = inodeId->u64();
    job.destination = req_.destination;
    job.state = cache::UploadJobState::OPEN;
    job.stateVersion = 1;
    job.createdAtMs = job.updatedAtMs = static_cast<uint64_t>(nowUs / 1000);
    job.writerLeaseId = req_.writerLeaseId;
    job.writerLeaseExpiresAtMs = req_.writerLeaseExpiresAtMs;

    CO_RETURN_ON_ERROR(co_await Inode(parentId).addIntoReadConflict(txn));
    CO_RETURN_ON_ERROR(co_await entry.addIntoReadConflict(txn));
    CO_RETURN_ON_ERROR(co_await inode.store(txn));
    CO_RETURN_ON_ERROR(co_await entry.store(txn));
    CO_RETURN_ON_ERROR(co_await FileSession::create(inode.id, req_.session).store(txn));
    auto created = co_await UploadJobStore::create(txn, job);
    CO_RETURN_ON_ERROR(created);
    if (!created->created) co_return makeError(CacheCode::kStateConflict, "write upload job already exists");

    CreateWriteStagingRsp rsp;
    rsp.inode = std::move(inode);
    rsp.job = std::move(job);
    rsp.created = true;
    co_return rsp;
  }

 private:
  const CreateWriteStagingReq &req_;
};

MetaStore::OpPtr<CreateWriteStagingRsp> MetaStore::createWriteStaging(const CreateWriteStagingReq &req) {
  return std::make_unique<CreateWriteStagingOp>(*this, req);
}

}  // namespace hf3fs::meta::server
