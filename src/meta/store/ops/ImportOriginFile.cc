#include <memory>
#include <sys/stat.h>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "meta/store/DirEntry.h"
#include "meta/store/Inode.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"

namespace hf3fs::meta::server {
namespace {

bool matchesImport(const Inode &inode, const OriginFileMetadata &metadata) {
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

class ImportOriginFileOp : public Operation<ImportOriginFileRsp> {
 public:
  ImportOriginFileOp(MetaStore &meta, const ImportOriginFileReq &req)
      : Operation<ImportOriginFileRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ImportOriginFileRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);

    auto resolved = co_await resolve(txn, req_.user).path(req_.entry.path, AtFlags(AT_SYMLINK_NOFOLLOW));
    CO_RETURN_ON_ERROR(resolved);
    if (resolved->dirEntry.has_value()) {
      auto inode = co_await resolved->dirEntry->snapshotLoadInode(txn);
      CO_RETURN_ON_ERROR(inode);
      if (!matchesImport(*inode, req_.entry.metadata)) co_return makeError(MetaCode::kExists);
      ImportOriginFileRsp rsp;
      rsp.inode = std::move(*inode);
      rsp.outcome = ImportOriginFileOutcome::ALREADY_EXISTS;
      co_return rsp;
    }

    auto parent = co_await resolved->getParentInode(txn);
    CO_RETURN_ON_ERROR(parent);
    CO_RETURN_ON_ERROR(parent->acl.checkPermission(req_.user, AccessType::WRITE));
    CO_RETURN_ON_ERROR(parent->asDirectory().checkLock(req_.client));

    auto layout =
        Layout::newEmpty(req_.entry.metadata.tableId, req_.entry.metadata.blockSize, req_.entry.metadata.stripeSize);
    CO_RETURN_ON_ERROR(co_await chainAlloc().allocateChainsForLayout(layout));
    auto inodeId = co_await allocateInodeId(txn, false);
    CO_RETURN_ON_ERROR(inodeId);

    auto parentId = resolved->getParentId();
    auto name = req_.entry.path.path->filename().native();
    auto acl = makeOriginAcl(*parent, req_.user, req_.entry.metadata.permission);
    auto inode = Inode::newOriginFile(*inodeId,
                                      acl,
                                      req_.entry.metadata.objectSize,
                                      std::move(layout),
                                      req_.entry.metadata.object,
                                      now());
    auto entry = DirEntry::newOriginFile(parentId, std::move(name), *inodeId);

    CO_RETURN_ON_ERROR(co_await Inode(parentId).addIntoReadConflict(txn));
    CO_RETURN_ON_ERROR(co_await entry.addIntoReadConflict(txn));
    CO_RETURN_ON_ERROR(co_await inode.store(txn));
    CO_RETURN_ON_ERROR(co_await entry.store(txn));

    ImportOriginFileRsp rsp;
    rsp.inode = std::move(inode);
    rsp.outcome = ImportOriginFileOutcome::CREATED;
    co_return rsp;
  }

 private:
  const ImportOriginFileReq &req_;
};

MetaStore::OpPtr<ImportOriginFileRsp> MetaStore::importOriginFile(const ImportOriginFileReq &req) {
  return std::make_unique<ImportOriginFileOp>(*this, req);
}

}  // namespace hf3fs::meta::server
