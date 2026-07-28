#include <memory>
#include <sys/stat.h>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "meta/components/OriginNamespaceManager.h"
#include "meta/store/DirEntry.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"

namespace hf3fs::meta::server {
namespace {

uint64_t blockCount(uint64_t length, uint64_t blockSize) { return length / blockSize + (length % blockSize != 0); }

Acl makeOriginAcl(const Inode &parent, const UserInfo &user, Permission permission) {
  Acl acl(user.uid,
          user.gid,
          Permission(permission.toUnderType() & ALLPERMS),
          IFlags(parent.acl.iflags & FS_FL_INHERITABLE));
  if (parent.acl.perm & S_ISGID) acl.gid = parent.acl.gid;
  return acl;
}

}  // namespace

class RefreshOriginFileOp : public Operation<RefreshOriginFileRsp> {
 public:
  RefreshOriginFileOp(MetaStore &meta, const RefreshOriginFileReq &req)
      : Operation<RefreshOriginFileRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<RefreshOriginFileRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);

    auto previous = co_await OriginNamespaceManager::loadRefresh(txn, req_.requestId);
    CO_RETURN_ON_ERROR(previous);
    if (previous->has_value()) {
      if (!(**previous).matches(req_)) {
        co_return makeError(CacheCode::kStateConflict, "refresh request id was reused with different parameters");
      }
      co_return (**previous).response();
    }

    auto resolved = co_await resolve(txn, req_.user).path(req_.path, AtFlags(AT_SYMLINK_NOFOLLOW));
    CO_RETURN_ON_ERROR(resolved);
    if (!resolved->dirEntry.has_value()) co_return makeError(MetaCode::kNotFound);
    if (resolved->dirEntry->id != req_.expectedInode) {
      co_return makeError(CacheCode::kStateConflict, "refresh expected inode is stale");
    }

    auto oldInode = (co_await Inode::load(txn, req_.expectedInode)).then(checkMetaFound<Inode>);
    CO_RETURN_ON_ERROR(oldInode);
    if (!oldInode->isOriginFile() || oldInode->asOriginFile().object != req_.oldObject) {
      co_return makeError(CacheCode::kStateConflict, "refresh old object identity is stale");
    }

    auto parent = co_await resolved->getParentInode(txn);
    CO_RETURN_ON_ERROR(parent);
    CO_RETURN_ON_ERROR(parent->acl.checkPermission(req_.user, AccessType::WRITE));
    CO_RETURN_ON_ERROR(parent->asDirectory().checkLock(req_.client));

    auto layout = Layout::newEmpty(req_.newMetadata.tableId, req_.newMetadata.blockSize, req_.newMetadata.stripeSize);
    CO_RETURN_ON_ERROR(co_await chainAlloc().allocateChainsForLayout(layout));
    auto newInodeId = co_await allocateInodeId(txn, false);
    CO_RETURN_ON_ERROR(newInodeId);
    auto cleanupJobId = Uuid::random();

    auto newInode = Inode::newOriginFile(*newInodeId,
                                         makeOriginAcl(*parent, req_.user, req_.newMetadata.permission),
                                         req_.newMetadata.objectSize,
                                         std::move(layout),
                                         req_.newMetadata.object,
                                         now());
    auto parentId = resolved->getParentId();
    auto name = req_.path.path->filename().native();
    auto newEntry = DirEntry::newOriginFile(parentId, name, *newInodeId);
    newEntry.uuid = req_.requestId;

    oldInode->asOriginFile().superseded = true;
    oldInode->asOriginFile().cacheAdmissionDisabled = true;
    oldInode->asOriginFile().cleanupJobId = cleanupJobId;
    CO_RETURN_ON_ERROR(
        co_await gcManager().removeEntry(txn, *resolved->dirEntry, *oldInode, GcInfo{req_.user.uid, name}));
    CO_RETURN_ON_ERROR(co_await newInode.store(txn));
    CO_RETURN_ON_ERROR(co_await newEntry.store(txn));

    OriginCleanupJobRecord cleanup;
    cleanup.jobId = cleanupJobId;
    cleanup.inode = oldInode->id;
    cleanup.beginBlock = 0;
    cleanup.endBlock = blockCount(oldInode->fileLength(), oldInode->fileLayout().chunkSize);
    cleanup.cursor = 0;
    CO_RETURN_ON_ERROR(co_await OriginNamespaceManager::storeCleanup(txn, cleanup));

    RefreshOriginFileRecord record;
    record.requestId = req_.requestId;
    record.path = req_.path;
    record.expectedInode = req_.expectedInode;
    record.oldObject = req_.oldObject;
    record.newMetadata = req_.newMetadata;
    record.newInode = newInode;
    record.cleanupJobId = cleanupJobId;
    CO_RETURN_ON_ERROR(co_await OriginNamespaceManager::storeRefresh(txn, record));
    co_return record.response();
  }

 private:
  const RefreshOriginFileReq &req_;
};

MetaStore::OpPtr<RefreshOriginFileRsp> MetaStore::refreshOriginFile(const RefreshOriginFileReq &req) {
  return std::make_unique<RefreshOriginFileOp>(*this, req);
}

}  // namespace hf3fs::meta::server
