#include <algorithm>
#include <memory>
#include <sys/stat.h>

#include "meta/store/DirEntry.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/UploadJobStore.h"

namespace hf3fs::meta::server {
namespace {

bool matchesPublished(const Inode &inode, const OriginFileMetadata &metadata) {
  if (!inode.isOriginFile()) return false;
  const auto &origin = inode.asOriginFile();
  return origin.object == metadata.object && origin.length == metadata.objectSize &&
         origin.layout.tableId == metadata.tableId && origin.layout.chunkSize == metadata.blockSize &&
         origin.layout.stripeSize == metadata.stripeSize &&
         inode.acl.perm == Permission(metadata.permission.toUnderType() & ALLPERMS);
}

Acl makeOriginAcl(const Inode &parent, const UserInfo &user, Permission permission) {
  Acl acl(user.uid,
          user.gid,
          Permission(permission.toUnderType() & ALLPERMS),
          IFlags(parent.acl.iflags & FS_FL_INHERITABLE));
  if (parent.acl.perm & S_ISGID) acl.gid = parent.acl.gid;
  return acl;
}

}  // namespace

class PublishOriginFileFromStagingOp : public Operation<PublishOriginFileFromStagingRsp> {
 public:
  PublishOriginFileFromStagingOp(MetaStore &meta, const PublishOriginFileFromStagingReq &req)
      : Operation<PublishOriginFileFromStagingRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<PublishOriginFileFromStagingRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto loaded = co_await UploadJobStore::load(txn, req_.jobId);
    CO_RETURN_ON_ERROR(loaded);
    if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
    auto job = std::move(**loaded);

    if (job.ownerUid != req_.user.uid || job.stagingInode != req_.expectedStagingInode.u64() || !job.completedObject ||
        *job.completedObject != req_.metadata.object || job.stagingLength != req_.metadata.objectSize) {
      co_return makeError(CacheCode::kStateConflict, "staged publish identity changed");
    }

    auto path = PathAt(job.path);
    auto resolved = co_await resolve(txn, req_.user).path(path, AtFlags(AT_SYMLINK_NOFOLLOW));
    CO_RETURN_ON_ERROR(resolved);
    if (!resolved->dirEntry.has_value()) {
      co_return makeError(CacheCode::kStateConflict, "staged publish path no longer exists");
    }

    if (job.state == cache::UploadJobState::PUBLISHED && job.stateVersion == req_.expectedStateVersion + 1) {
      if (resolved->dirEntry->id.u64() != job.publishedInode) {
        co_return makeError(CacheCode::kStateConflict, "published path changed after commit");
      }
      auto inode = co_await resolved->dirEntry->snapshotLoadInode(txn);
      CO_RETURN_ON_ERROR(inode);
      if (!matchesPublished(*inode, req_.metadata)) {
        co_return makeError(CacheCode::kStateConflict, "published inode identity changed after commit");
      }
      PublishOriginFileFromStagingRsp response;
      response.inode = std::move(*inode);
      response.outcome = PublishOriginFileOutcome::ALREADY_PUBLISHED;
      co_return response;
    }

    if (job.state != cache::UploadJobState::PUBLISHING || job.stateVersion != req_.expectedStateVersion ||
        resolved->dirEntry->id != req_.expectedStagingInode || !resolved->dirEntry->isFile()) {
      co_return makeError(CacheCode::kStateConflict, "staged publish fence changed");
    }

    auto staging = (co_await Inode::load(txn, req_.expectedStagingInode)).then(checkMetaFound<Inode>);
    CO_RETURN_ON_ERROR(staging);
    if (!staging->isFile() || staging->asFile().length != job.stagingLength ||
        !(staging->acl.iflags & FS_IMMUTABLE_FL) || staging->acl.uid != job.ownerUid ||
        staging->acl.perm != Permission(req_.metadata.permission.toUnderType() & ALLPERMS)) {
      co_return makeError(CacheCode::kStateConflict, "staging inode is not the sealed upload snapshot");
    }
    auto routing = chainAlloc().getRoutingInfo();
    auto stagingTable =
        routing && routing->raw() ? routing->raw()->getChainTable(staging->fileLayout().tableId) : nullptr;
    if (stagingTable == nullptr || !stagingTable->isWriteStaging()) {
      co_return makeError(MetaCode::kInvalidFileLayout, "staging inode is not on WRITE_STAGING");
    }

    auto parent = co_await resolved->getParentInode(txn);
    CO_RETURN_ON_ERROR(parent);
    CO_RETURN_ON_ERROR(parent->acl.checkPermission(req_.user, AccessType::WRITE));
    CO_RETURN_ON_ERROR(parent->asDirectory().checkLock(req_.client));

    auto layout = Layout::newEmpty(req_.metadata.tableId, req_.metadata.blockSize, req_.metadata.stripeSize);
    CO_RETURN_ON_ERROR(co_await chainAlloc().allocateChainsForLayout(layout));
    auto inodeId = co_await allocateInodeId(txn, false);
    CO_RETURN_ON_ERROR(inodeId);
    auto owner = req_.user;
    owner.gid = staging->acl.gid;
    auto inode = Inode::newOriginFile(*inodeId,
                                      makeOriginAcl(*parent, owner, req_.metadata.permission),
                                      req_.metadata.objectSize,
                                      std::move(layout),
                                      req_.metadata.object,
                                      now());
    auto parentId = resolved->getParentId();
    auto name = path.path->filename().native();
    auto entry = DirEntry::newOriginFile(parentId, name, *inodeId);
    entry.uuid = req_.jobId.toUnderType();

    staging->acl.iflags = IFlags(staging->acl.iflags & ~FS_IMMUTABLE_FL);
    CO_RETURN_ON_ERROR(
        co_await gcManager().removeEntry(txn, *resolved->dirEntry, *staging, GcInfo{job.ownerUid, name}));
    CO_RETURN_ON_ERROR(co_await inode.store(txn));
    CO_RETURN_ON_ERROR(co_await entry.store(txn));

    job.state = cache::UploadJobState::PUBLISHED;
    ++job.stateVersion;
    job.publishedInode = inodeId->u64();
    auto nowUs = now().toMicroseconds();
    if (nowUs <= 0) co_return makeError(StatusCode::kDataCorruption, "invalid metadata clock for staged publish");
    job.updatedAtMs = std::max(job.updatedAtMs, static_cast<uint64_t>(nowUs / 1000));
    auto updated = co_await UploadJobStore::update(txn, req_.expectedStateVersion, job);
    CO_RETURN_ON_ERROR(updated);

    PublishOriginFileFromStagingRsp response;
    response.inode = std::move(inode);
    response.outcome = PublishOriginFileOutcome::PUBLISHED;
    co_return response;
  }

 private:
  const PublishOriginFileFromStagingReq &req_;
};

MetaStore::OpPtr<PublishOriginFileFromStagingRsp> MetaStore::publishOriginFileFromStaging(
    const PublishOriginFileFromStagingReq &req) {
  return std::make_unique<PublishOriginFileFromStagingOp>(*this, req);
}

}  // namespace hf3fs::meta::server
