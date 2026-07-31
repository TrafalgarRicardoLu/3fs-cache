#pragma once

#include <fcntl.h>
#include <folly/logging/xlog.h>
#include <functional>
#include <limits>
#include <optional>
#include <type_traits>
#include <variant>
#include <vector>

#include "common/app/ClientId.h"
#include "common/app/NodeId.h"
#include "common/serde/Serde.h"
#include "common/serde/Service.h"
#include "common/utils/Result.h"
#include "common/utils/Status.h"
#include "common/utils/StrongType.h"
#include "common/utils/Uuid.h"
#include "fbs/core/user/User.h"
#include "fbs/meta/Common.h"
#include "fbs/meta/Schema.h"
#include "fbs/mgmtd/MgmtdTypes.h"
#include "fbs/storage/Cache.h"

#define META_SERVICE_VERSION 1

#define CHECK_SESSION(session)             \
  do {                                     \
    if (!session.has_value()) {            \
      return INVALID(#session " not set"); \
    }                                      \
    if (!session->valid()) {               \
      return INVALID(#session " invalid"); \
    }                                      \
  } while (0)

namespace hf3fs::meta {

struct ReqBase {
  SERDE_STRUCT_FIELD(user, UserInfo{});
  SERDE_STRUCT_FIELD(client, ClientId{Uuid::zero()});
  SERDE_STRUCT_FIELD(forward, flat::NodeId(0));
  SERDE_STRUCT_FIELD(uuid, Uuid::zero());

 public:
  // set client id in unittest
  static std::optional<ClientId> &currentClientId() {
    static std::optional<ClientId> clientId;
    return clientId;
  }

  ReqBase(UserInfo user = {}, Uuid uuid = Uuid::zero())
      : user(std::move(user)),
        uuid(uuid) {
    if (currentClientId()) {
      client = *currentClientId();
    }
  }

  Result<Void> checkUuid() const {
    if (client.uuid == Uuid::zero()) return INVALID("Invalid client uuid");
    if (uuid == Uuid::zero()) return INVALID("Invalid request uuid");
    return VALID;
  }
};
struct RspBase {
  SERDE_STRUCT_FIELD(dummy, Void{});
};

// authentication
struct AuthReq : ReqBase {
  SERDE_STRUCT_FIELD(dummy, Void{});

 public:
  AuthReq() = default;
  AuthReq(UserInfo user)
      : ReqBase(std::move(user)) {}
  Result<Void> valid() const { return VALID; }
};

struct AuthRsp : RspBase {
  SERDE_STRUCT_FIELD(user, UserInfo{});

 public:
  AuthRsp() = default;
  AuthRsp(UserInfo user)
      : user(std::move(user)) {}
};

// statFs
struct StatFsReq : ReqBase {
 public:
  StatFsReq() = default;
  StatFsReq(UserInfo user)
      : ReqBase(std::move(user)) {}
  Result<Void> valid() const { return VALID; }
};
struct StatFsRsp : RspBase {
  SERDE_STRUCT_FIELD(capacity, uint64_t(0));
  SERDE_STRUCT_FIELD(used, uint64_t(0));
  SERDE_STRUCT_FIELD(free, uint64_t(0));

 public:
  StatFsRsp() = default;
  StatFsRsp(uint64_t capacity, uint64_t used, uint64_t free)
      : capacity(capacity),
        used(used),
        free(free) {}
};

// stat
struct StatReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(flags, AtFlags());

 public:
  StatReq() = default;
  StatReq(UserInfo user, PathAt path, AtFlags flags)
      : ReqBase(std::move(user)),
        path(std::move(path)),
        flags(flags) {}
  Result<Void> valid() const { return flags.valid(); }
};
struct StatRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  StatRsp() = default;
  StatRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// batchStat & batchStatByPath
struct BatchStatReq : ReqBase {
  SERDE_STRUCT_FIELD(inodeIds, std::vector<InodeId>());

 public:
  BatchStatReq() = default;
  BatchStatReq(UserInfo user, std::vector<InodeId> inodeIds)
      : ReqBase(std::move(user)),
        inodeIds(std::move(inodeIds)) {}

  Result<Void> valid() const { return VALID; }
};

struct BatchStatRsp : RspBase {
  SERDE_STRUCT_FIELD(inodes, std::vector<std::optional<Inode>>());

 public:
  BatchStatRsp() = default;
  BatchStatRsp(std::vector<std::optional<Inode>> inodes)
      : inodes(std::move(inodes)) {}
};

struct BatchStatByPathReq : ReqBase {
  SERDE_STRUCT_FIELD(paths, std::vector<PathAt>());
  SERDE_STRUCT_FIELD(flags, AtFlags());

 public:
  BatchStatByPathReq() = default;
  BatchStatByPathReq(UserInfo user, std::vector<PathAt> paths, AtFlags flags)
      : ReqBase(std::move(user)),
        paths(std::move(paths)),
        flags(flags) {}

  Result<Void> valid() const { return flags.valid(); }
};

struct BatchStatByPathRsp : RspBase {
  SERDE_STRUCT_FIELD(inodes, std::vector<Result<Inode>>());

 public:
  BatchStatByPathRsp() = default;
  BatchStatByPathRsp(std::vector<Result<Inode>> inodes)
      : inodes(std::move(inodes)) {}
};

// create
struct CreateReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(session, std::optional<SessionInfo>());
  SERDE_STRUCT_FIELD(flags, OpenFlags());
  SERDE_STRUCT_FIELD(perm, Permission());
  SERDE_STRUCT_FIELD(layout, std::optional<Layout>());
  SERDE_STRUCT_FIELD(removeChunksBatchSize, uint32_t(0));
  SERDE_STRUCT_FIELD(dynStripe, false);

 public:
  CreateReq() = default;
  CreateReq(UserInfo user,
            PathAt path,
            std::optional<SessionInfo> session,
            OpenFlags flags,
            Permission perm,
            std::optional<Layout> layout = std::nullopt,
            bool dynStripe = false)
      : ReqBase(std::move(user), Uuid::random()),
        path(std::move(path)),
        session(session),
        flags(flags),
        perm(perm),
        layout(std::move(layout)),
        removeChunksBatchSize(32),
        dynStripe(dynStripe) {}

  Result<Void> valid() const {
    RETURN_ON_ERROR(path.validForCreate());
    RETURN_ON_ERROR(flags.valid());
    if (flags.accessType() != AccessType::READ) CHECK_SESSION(session);
    if (layout.has_value()) RETURN_ON_ERROR(layout->valid(true));
    if (flags.contains(O_TRUNC) && removeChunksBatchSize == 0) return INVALID("removeChunksBatchSize == 0");
    return VALID;
  }
};
struct CreateRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());
  SERDE_STRUCT_FIELD(needTruncate, false);

 public:
  CreateRsp() = default;
  CreateRsp(Inode stat, bool needTruncate)
      : stat(std::move(stat)),
        needTruncate(needTruncate) {}
};

// mkdirs
struct MkdirsReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(perm, Permission());
  SERDE_STRUCT_FIELD(recursive, false);
  SERDE_STRUCT_FIELD(layout, std::optional<Layout>());

 public:
  MkdirsReq() = default;
  MkdirsReq(UserInfo user, PathAt path, Permission perm, bool recursive, std::optional<Layout> layout = std::nullopt)
      : ReqBase(std::move(user), Uuid::random()),
        path(std::move(path)),
        perm(perm),
        recursive(recursive),
        layout(std::move(layout)) {}
  Result<Void> valid() const {
    if (!path.path.has_value() || path.path->empty()) return INVALID("path not set");
    if (layout.has_value()) RETURN_ON_ERROR(layout->valid(true));
    return Void{};
  }
};

struct MkdirsRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  MkdirsRsp() = default;
  MkdirsRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// symlink
struct SymlinkReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(target, Path());

 public:
  SymlinkReq() = default;
  SymlinkReq(UserInfo user, PathAt path, Path target)
      : ReqBase(std::move(user), Uuid::random()),
        path(std::move(path)),
        target(std::move(target)) {}
  Result<Void> valid() const { return path.validForCreate(); }
};
struct SymlinkRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  SymlinkRsp() = default;
  SymlinkRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// hardlink
struct HardLinkReq : ReqBase {
  SERDE_STRUCT_FIELD(oldPath, PathAt());
  SERDE_STRUCT_FIELD(newPath, PathAt());
  SERDE_STRUCT_FIELD(flags, AtFlags());

 public:
  HardLinkReq() = default;
  HardLinkReq(UserInfo user, PathAt oldPath, PathAt newPath, AtFlags flags)
      : ReqBase(std::move(user), Uuid::random()),
        oldPath(std::move(oldPath)),
        newPath(std::move(newPath)),
        flags(flags) {}
  Result<Void> valid() const {
    RETURN_ON_ERROR(newPath.validForCreate());
    RETURN_ON_ERROR(flags.valid());
    return VALID;
  }
};
struct HardLinkRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  HardLinkRsp() = default;
  HardLinkRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// remove
struct RemoveReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(atFlags, AtFlags());
  SERDE_STRUCT_FIELD(recursive, false);
  SERDE_STRUCT_FIELD(checkType, false);
  SERDE_STRUCT_FIELD(inodeId, std::optional<InodeId>());

 public:
  RemoveReq() = default;
  RemoveReq(UserInfo user,
            PathAt path,
            AtFlags flags,
            bool recursive,
            bool checkType = false,
            std::optional<InodeId> inodeId = std::nullopt)
      : ReqBase(std::move(user), Uuid::random()),
        path(std::move(path)),
        atFlags(flags),
        recursive(recursive),
        checkType(checkType),
        inodeId(inodeId) {}
  Result<Void> valid() const {
    if (path.path.has_value()) RETURN_ON_ERROR(path.validForCreate());
    if (recursive) RETURN_ON_ERROR(checkUuid());
    return VALID;
  }
};

struct RemoveRsp : RspBase {
  SERDE_STRUCT_FIELD(dummy, Void{});
};

// open
struct OpenReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(session, std::optional<SessionInfo>());
  SERDE_STRUCT_FIELD(flags, OpenFlags());
  SERDE_STRUCT_FIELD(removeChunksBatchSize, uint32_t(0));
  SERDE_STRUCT_FIELD(dynStripe, false);

 public:
  OpenReq() = default;
  OpenReq(UserInfo user, PathAt path, std::optional<SessionInfo> session, OpenFlags flags, bool dynStripe = false)
      : ReqBase(std::move(user)),
        path(std::move(path)),
        session(session),
        flags(flags),
        removeChunksBatchSize(32),
        dynStripe(dynStripe) {}
  Result<Void> valid() const {
    RETURN_ON_ERROR(flags.valid());
    if (flags.accessType() != AccessType::READ) CHECK_SESSION(session);
    if (session.has_value() && !session->valid()) return INVALID("session invalid");
    if (flags.accessType() == AccessType::READ && (flags.contains(O_TRUNC) || flags.contains(O_APPEND)))
      return INVALID("O_RDONLY with O_TRUNC or O_APPEND");
    if (flags.contains(O_TRUNC) && removeChunksBatchSize == 0) return INVALID("removeChunksBatchSize == 0");
    return VALID;
  }
};
struct OpenRsp : RspBase {
  SERDE_STRUCT_FIELD(_unused, (uint32_t)0);
  SERDE_STRUCT_FIELD(stat, Inode());
  SERDE_STRUCT_FIELD(needTruncate, false);

 public:
  OpenRsp() = default;
  OpenRsp(Inode stat, bool needTruncate)
      : stat(std::move(stat)),
        needTruncate(needTruncate) {}
};

// sync
struct SyncReq : ReqBase {
  SERDE_STRUCT_FIELD(inode, InodeId());
  SERDE_STRUCT_FIELD(updateLength, false);
  SERDE_STRUCT_FIELD(atime, std::optional<UtcTime>());
  SERDE_STRUCT_FIELD(mtime, std::optional<UtcTime>());
  SERDE_STRUCT_FIELD(truncated, false);
  SERDE_STRUCT_FIELD(lengthHint, std::optional<VersionedLength>());

 public:
  SyncReq() = default;
  SyncReq(UserInfo user,
          InodeId inode,
          bool updateLength,
          std::optional<UtcTime> atime,
          std::optional<UtcTime> mtime,
          bool truncated = false,
          std::optional<VersionedLength> hint = std::nullopt)
      : ReqBase(std::move(user)),
        inode(inode),
        updateLength(updateLength),
        atime(atime),
        mtime(mtime),
        truncated(truncated),
        lengthHint(hint) {}
  Result<Void> valid() const {
    if (truncated && !updateLength) return INVALID("truncate but not updateLength");
    return VALID;
  }
};
struct SyncRsp : RspBase {
  SERDE_STRUCT_FIELD(_unused, (uint32_t)0);
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  SyncRsp() = default;
  SyncRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// close
struct CloseReq : ReqBase {
  SERDE_STRUCT_FIELD(inode, InodeId());
  SERDE_STRUCT_FIELD(session, std::optional<SessionInfo>());
  SERDE_STRUCT_FIELD(updateLength, false);
  SERDE_STRUCT_FIELD(atime, std::optional<UtcTime>());
  SERDE_STRUCT_FIELD(mtime, std::optional<UtcTime>());
  SERDE_STRUCT_FIELD(lengthHint, std::optional<VersionedLength>());

 public:
  bool pruneSession = false;

  CloseReq() = default;
  CloseReq(UserInfo user,
           InodeId inode,
           std::optional<SessionInfo> session,
           bool updateLength,
           std::optional<UtcTime> atime,
           std::optional<UtcTime> mtime)
      : ReqBase(std::move(user)),
        inode(inode),
        session(session),
        updateLength(updateLength),
        atime(atime),
        mtime(mtime) {}
  Result<Void> valid() const {
    if (updateLength || mtime.has_value()) CHECK_SESSION(session);
    return VALID;
  }
};
struct CloseRsp : RspBase {
  SERDE_STRUCT_FIELD(_unused, (uint32_t)0);
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  CloseRsp() = default;
  CloseRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// rename
struct RenameReq : ReqBase {
  SERDE_STRUCT_FIELD(src, PathAt());
  SERDE_STRUCT_FIELD(dest, PathAt());
  SERDE_STRUCT_FIELD(moveToTrash, false);
  SERDE_STRUCT_FIELD(inodeId, std::optional<InodeId>());

 public:
  RenameReq() = default;
  RenameReq(UserInfo user,
            PathAt src,
            PathAt dest,
            bool moveToTrash = false,
            std::optional<InodeId> inodeId = std::nullopt)
      : ReqBase(std::move(user), Uuid::random()),
        src(std::move(src)),
        dest(std::move(dest)),
        moveToTrash(moveToTrash),
        inodeId(inodeId) {}
  Result<Void> valid() const {
    RETURN_ON_ERROR(src.validForCreate());
    RETURN_ON_ERROR(dest.validForCreate());
    if (moveToTrash) RETURN_ON_ERROR(checkUuid());
    return VALID;
  }
};
struct RenameRsp : RspBase {
  SERDE_STRUCT_FIELD(dummy, Void{});
  SERDE_STRUCT_FIELD(stat, std::optional<Inode>());

 public:
  RenameRsp() = default;
  RenameRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// list
struct ListReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(prev, std::string());
  SERDE_STRUCT_FIELD(limit, (int32_t)-1);
  SERDE_STRUCT_FIELD(status, false);

 public:
  ListReq() = default;
  ListReq(UserInfo user, PathAt path, std::string prev = {}, int32_t limit = -1, bool status = false)
      : ReqBase(std::move(user)),
        path(std::move(path)),
        prev(std::move(prev)),
        limit(limit),
        status(status) {}
  Result<Void> valid() const { return VALID; }
};
struct ListRsp : RspBase {
  SERDE_STRUCT_FIELD(entries, std::vector<DirEntry>());
  SERDE_STRUCT_FIELD(inodes, std::vector<Inode>());
  SERDE_STRUCT_FIELD(more, false);

 public:
  ListRsp() = default;
  ListRsp(std::vector<DirEntry> entries, std::vector<Inode> inodes, bool more)
      : entries(std::move(entries)),
        inodes(std::move(inodes)),
        more(more) {}
};

// truncate
struct TruncateReq : ReqBase {
  SERDE_STRUCT_FIELD(inode, InodeId(0));
  SERDE_STRUCT_FIELD(length, uint64_t(0));
  SERDE_STRUCT_FIELD(removeChunksBatchSize, uint32_t(0));

 public:
  TruncateReq() = default;
  TruncateReq(UserInfo user, InodeId inode, uint64_t length, uint32_t removeChunksBatchSize)
      : ReqBase(std::move(user)),
        inode(inode),
        length(length),
        removeChunksBatchSize(removeChunksBatchSize) {}
  Result<Void> valid() const {
    if (removeChunksBatchSize == 0) return INVALID("removeChunksBatchSize == 0");
    return VALID;
  }
};
struct TruncateRsp : RspBase {
  SERDE_STRUCT_FIELD(chunksRemoved, uint32_t(0));
  SERDE_STRUCT_FIELD(stat, Inode());
  SERDE_STRUCT_FIELD(finished, true);

 public:
  TruncateRsp() = default;
  TruncateRsp(Inode stat)
      : TruncateRsp(std::move(stat), 0, true) {}
  TruncateRsp(Inode stat, uint32_t chunksRemoved, bool finished)
      : chunksRemoved(chunksRemoved),
        stat(std::move(stat)),
        finished(finished) {}
};

// getRealPath
struct GetRealPathReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(absolute, false);

 public:
  GetRealPathReq() = default;
  GetRealPathReq(UserInfo user, PathAt path, bool absolute)
      : ReqBase(std::move(user)),
        path(std::move(path)),
        absolute(absolute) {}
  Result<Void> valid() const { return VALID; }
};
struct GetRealPathRsp : RspBase {
  SERDE_STRUCT_FIELD(path, Path());

 public:
  GetRealPathRsp() = default;
  GetRealPathRsp(Path path)
      : path(std::move(path)) {}
};

// setAttr
struct SetAttrReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(flags, AtFlags());
  SERDE_STRUCT_FIELD(uid, std::optional<Uid>());
  SERDE_STRUCT_FIELD(gid, std::optional<Gid>());
  SERDE_STRUCT_FIELD(perm, std::optional<Permission>());
  SERDE_STRUCT_FIELD(atime, std::optional<UtcTime>());
  SERDE_STRUCT_FIELD(mtime, std::optional<UtcTime>());
  SERDE_STRUCT_FIELD(layout, std::optional<Layout>());
  SERDE_STRUCT_FIELD(iflags, std::optional<IFlags>());
  SERDE_STRUCT_FIELD(dynStripe, uint32_t(0));

 public:
  static SetAttrReq setLayout(UserInfo user, PathAt path, AtFlags flags, Layout layout) {
    return {std::move(user), std::move(path), flags, {}, {}, {}, {}, {}, std::move(layout)};
  }
  static SetAttrReq setPermission(UserInfo user,
                                  PathAt path,
                                  AtFlags flags,
                                  std::optional<Uid> uid,
                                  std::optional<Gid> gid,
                                  std::optional<Permission> perm,
                                  std::optional<IFlags> iflags = std::nullopt) {
    return {std::move(user), std::move(path), flags, uid, gid, perm, {}, {}, {}, iflags};
  }
  static SetAttrReq setIFlags(UserInfo user, PathAt path, IFlags iflags) {
    return {std::move(user), std::move(path), AtFlags(), {}, {}, {}, {}, {}, {}, iflags};
  }
  static SetAttrReq utimes(UserInfo user,
                           PathAt path,
                           AtFlags flags,
                           std::optional<UtcTime> atime,
                           std::optional<UtcTime> mtime) {
    return {std::move(user), std::move(path), flags, {}, {}, {}, atime, mtime, {}};
  }
  static SetAttrReq extendStripe(UserInfo user, InodeId inode, uint32_t stripe) {
    return {std::move(user), PathAt(inode), AtFlags{}, {}, {}, {}, {}, {}, {}, {}, stripe};
  }
  Result<Void> valid() const {
    if (layout.has_value()) RETURN_ON_ERROR(layout->valid(true));
    if (iflags && (*iflags & ~FS_FL_SUPPORTED)) {
      return MAKE_ERROR_F(StatusCode::kInvalidArg, "only support {:x}", (uint32_t)FS_FL_SUPPORTED);
    }
    return VALID;
  }
};

struct SetAttrRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());

 public:
  SetAttrRsp() = default;
  SetAttrRsp(Inode stat)
      : stat(std::move(stat)) {}
};

// pruneSession
struct PruneSessionReq : ReqBase {
  SERDE_STRUCT_FIELD(client, ClientId::zero());
  SERDE_STRUCT_FIELD(sessions, std::vector<Uuid>());
  SERDE_STRUCT_FIELD(needSync, std::vector<bool>());  // deperated

 public:
  PruneSessionReq() = default;
  PruneSessionReq(ClientId client, std::vector<Uuid> sessions)
      : ReqBase(),
        client(client),
        sessions(std::move(sessions)) {}
  Result<Void> valid() const { return VALID; }
};
struct PruneSessionRsp : RspBase {
  SERDE_STRUCT_FIELD(dummy, Void{});
};

// dropUserCache
struct DropUserCacheReq : ReqBase {
  SERDE_STRUCT_FIELD(uid, std::optional<Uid>());
  SERDE_STRUCT_FIELD(dropAll, false);

 public:
  DropUserCacheReq() = default;
  DropUserCacheReq(std::optional<Uid> u, bool d)
      : uid(std::move(u)),
        dropAll(d) {}
  Result<Void> valid() const { return VALID; }
};
struct DropUserCacheRsp : RspBase {
  SERDE_STRUCT_FIELD(dummy, Void{});
};

// lockDirectory
struct LockDirectoryReq : ReqBase {
  enum class LockAction : uint8_t {
    TryLock,
    PreemptLock,
    UnLock,
    Clear,
  };
  SERDE_STRUCT_FIELD(inode, InodeId());
  SERDE_STRUCT_FIELD(action, LockAction::TryLock);

 public:
  LockDirectoryReq() = default;
  LockDirectoryReq(UserInfo user, InodeId inode, LockAction action)
      : ReqBase(user),
        inode(inode),
        action(action) {}
  Result<Void> valid() const { return VALID; }
};

struct LockDirectoryRsp : RspBase {
  SERDE_STRUCT_FIELD(dummy, Void{});

 public:
  LockDirectoryRsp() = default;
};

inline constexpr size_t kMaxCacheBatchItems = 1000;

enum class ImportOriginFileOutcome : uint8_t {
  CREATED,
  ALREADY_EXISTS,
};

struct OriginFileMetadata {
  SERDE_STRUCT_FIELD(object, cache::ImmutableObjectIdentity{});
  SERDE_STRUCT_FIELD(objectSize, uint64_t{0});
  SERDE_STRUCT_FIELD(tableId, flat::ChainTableId{});
  SERDE_STRUCT_FIELD(blockSize, uint32_t{0});
  SERDE_STRUCT_FIELD(stripeSize, uint32_t{0});
  SERDE_STRUCT_FIELD(permission, Permission{});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(object.valid());
    if (!tableId || blockSize == 0 || stripeSize == 0) return INVALID("invalid OriginFile layout");
    auto blocks = objectSize / blockSize + (objectSize % blockSize != 0);
    if (blocks > std::numeric_limits<uint32_t>::max()) return INVALID("object has too many cache blocks");
    return VALID;
  }
};

struct ImportOriginFileEntry {
  SERDE_STRUCT_FIELD(path, PathAt{});
  SERDE_STRUCT_FIELD(metadata, OriginFileMetadata{});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(path.validForCreate());
    return metadata.valid();
  }
};

struct ImportOriginFileReq : ReqBase {
  SERDE_STRUCT_FIELD(entry, ImportOriginFileEntry{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const { return entry.valid(); }
};

struct ImportOriginFileRsp : RspBase {
  SERDE_STRUCT_FIELD(inode, Inode{});
  SERDE_STRUCT_FIELD(outcome, ImportOriginFileOutcome::CREATED);
};

struct BatchImportOriginFilesReq : ReqBase {
  SERDE_STRUCT_FIELD(entries, std::vector<ImportOriginFileEntry>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (entries.size() > kMaxCacheBatchItems) return makeError(CacheCode::kRequestTooLarge, "too many import entries");
    for (const auto &entry : entries) RETURN_ON_ERROR(entry.valid());
    return VALID;
  }
};

struct BatchImportOriginFilesRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<ImportOriginFileRsp>>{});
};

struct RefreshOriginFileReq : ReqBase {
  SERDE_STRUCT_FIELD(requestId, Uuid::zero());
  SERDE_STRUCT_FIELD(path, PathAt{});
  SERDE_STRUCT_FIELD(expectedInode, InodeId{});
  SERDE_STRUCT_FIELD(oldObject, cache::ImmutableObjectIdentity{});
  SERDE_STRUCT_FIELD(newMetadata, OriginFileMetadata{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (requestId == Uuid::zero()) return INVALID("requestId not set");
    if (expectedInode == InodeId{}) return INVALID("expectedInode not set");
    RETURN_ON_ERROR(path.validForCreate());
    RETURN_ON_ERROR(oldObject.valid());
    if (oldObject == newMetadata.object) return INVALID("refresh object identity did not change");
    return newMetadata.valid();
  }
};

struct RefreshOriginFileRsp : RspBase {
  SERDE_STRUCT_FIELD(newInode, Inode{});
  SERDE_STRUCT_FIELD(cleanupJobId, Uuid::zero());
};

struct ReadBlockPlan {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(fileRange, cache::ByteRange{});
  SERDE_STRUCT_FIELD(originRange, cache::ByteRange{});
  SERDE_STRUCT_FIELD(state, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(chunkId, (ChunkId(InodeId{}, 0, 0)));
  SERDE_STRUCT_FIELD(chainId, flat::ChainId{});
  SERDE_STRUCT_FIELD(actualBlockLength, uint64_t{0});
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{0});
  SERDE_STRUCT_FIELD(ready, std::optional<cache::ReadyIdentity>{});
};

struct GetFileReadPlanReq : ReqBase {
  SERDE_STRUCT_FIELD(openSessionId, Uuid::zero());
  SERDE_STRUCT_FIELD(inode, InodeId{});
  SERDE_STRUCT_FIELD(offset, uint64_t{0});
  SERDE_STRUCT_FIELD(length, uint64_t{0});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (openSessionId == Uuid::zero()) return INVALID("openSessionId not set");
    if (inode == InodeId{}) return INVALID("inode not set");
    if (length && offset > std::numeric_limits<uint64_t>::max() - length) return INVALID("read range overflow");
    return VALID;
  }
};

struct GetFileReadPlanRsp : RspBase {
  SERDE_STRUCT_FIELD(inode, InodeId{});
  SERDE_STRUCT_FIELD(object, cache::ImmutableObjectIdentity{});
  SERDE_STRUCT_FIELD(blocks, std::vector<ReadBlockPlan>{});
};

struct CacheServiceIdentity {
  SERDE_STRUCT_FIELD(name, String{});
  SERDE_STRUCT_FIELD(token, String{});

 public:
  Result<Void> valid() const {
    if (name.empty() || token.empty()) return INVALID("invalid service identity");
    return VALID;
  }
  std::string serdeToReadable() const { return std::string{name} + "@SECRET TOKEN"; }
};

struct CacheBlockRequestBase {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(blockLength, uint64_t{0});
  SERDE_STRUCT_FIELD(permit, std::optional<storage::PermitIdentity>{});
  SERDE_STRUCT_FIELD(expectedPermit, std::optional<storage::PermitIdentity>{});
  SERDE_STRUCT_FIELD(expectedState, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(expectedLoaderId, Uuid::zero());
  SERDE_STRUCT_FIELD(expectedLoadEpoch, uint64_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (blockLength == 0) return INVALID("blockLength not set");
    if (permit.has_value()) RETURN_ON_ERROR(permit->valid());
    if (expectedPermit.has_value()) {
      RETURN_ON_ERROR(expectedPermit->valid());
      if (!permit.has_value()) return INVALID("expectedPermit requires a replacement permit");
    }
    if (expectedState != cache::CacheBlockState::NONE && expectedState != cache::CacheBlockState::QUEUED &&
        expectedState != cache::CacheBlockState::LOADING) {
      return INVALID("invalid expected cache admission state");
    }
    if ((expectedLoaderId == Uuid::zero()) != (expectedLoadEpoch == 0)) return INVALID("incomplete loader fence");
    const bool hasReplacementFence = expectedPermit.has_value() || expectedState != cache::CacheBlockState::NONE ||
                                     expectedLoaderId != Uuid::zero() || expectedLoadEpoch != 0;
    if (hasReplacementFence &&
        (!permit.has_value() || !expectedPermit.has_value() || expectedState == cache::CacheBlockState::NONE)) {
      return INVALID("incomplete permit replacement fence");
    }
    if (expectedState == cache::CacheBlockState::QUEUED && expectedLoaderId != Uuid::zero()) {
      return INVALID("queued permit replacement cannot carry loader fence");
    }
    if (expectedState == cache::CacheBlockState::LOADING && expectedLoaderId == Uuid::zero()) {
      return INVALID("loading permit replacement requires loader fence");
    }
    return VALID;
  }
};

struct CacheBlockLease {
  SERDE_STRUCT_FIELD(loaderId, Uuid::zero());
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{0});
  SERDE_STRUCT_FIELD(cacheGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(permit, std::optional<storage::PermitIdentity>{});
};

struct CacheBlockMutationResult {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(state, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(enqueueOutcome, cache::CacheEnqueueOutcome::INVALID);
  SERDE_STRUCT_FIELD(placement, std::optional<storage::PlacementIdentity>{});
};

#define CACHE_BOUNDED_REQ(NAME, ITEM)                                                \
  struct NAME##Req : ReqBase {                                                       \
    SERDE_STRUCT_FIELD(service, CacheServiceIdentity{});                             \
    SERDE_STRUCT_FIELD(items, std::vector<ITEM>{});                                  \
                                                                                     \
   public:                                                                           \
    Result<Void> valid() const {                                                     \
      RETURN_ON_ERROR(service.valid());                                              \
      if (items.size() > kMaxCacheBatchItems)                                        \
        return makeError(CacheCode::kRequestTooLarge, "too many cache block items"); \
      return VALID;                                                                  \
    }                                                                                \
  }

CACHE_BOUNDED_REQ(EnqueueCacheBlocks, CacheBlockRequestBase);
struct EnqueueCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheBlockMutationResult>>{});
  SERDE_STRUCT_FIELD(permits, std::vector<std::optional<storage::PermitIdentity>>{});
};

CACHE_BOUNDED_REQ(AcquireCacheBlocks, CacheBlockRequestBase);
struct AcquireCacheBlockResult {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(lease, CacheBlockLease{});
};
struct AcquireCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<AcquireCacheBlockResult>>{});
};

struct CommitCacheBlockItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(loaderId, Uuid::zero());
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{0});
  SERDE_STRUCT_FIELD(cacheGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(blockLength, uint64_t{0});
  SERDE_STRUCT_FIELD(checksumType, uint8_t{0});
  SERDE_STRUCT_FIELD(checksumValue, uint32_t{0});
  SERDE_STRUCT_FIELD(permit, std::optional<storage::PermitIdentity>{});
  SERDE_STRUCT_FIELD(placement, std::optional<storage::PlacementIdentity>{});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (loaderId == Uuid::zero() || loadEpoch == 0 || cacheGeneration == cache::CacheGeneration{} || blockLength == 0) {
      return INVALID("invalid cache commit fence");
    }
    if (permit.has_value() != placement.has_value()) return INVALID("permit and placement must be committed together");
    if (permit.has_value()) {
      RETURN_ON_ERROR(permit->valid());
      RETURN_ON_ERROR(placement->valid());
      if (permit->placement != *placement) return INVALID("commit placement differs from permit placement");
    }
    return VALID;
  }
};
CACHE_BOUNDED_REQ(CommitCacheBlocks, CommitCacheBlockItem);
struct CommitCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheBlockMutationResult>>{});
};

struct FailCacheBlockItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(loaderId, Uuid::zero());
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (loaderId == Uuid::zero() || loadEpoch == 0) return INVALID("invalid cache failure fence");
    return VALID;
  }
};
CACHE_BOUNDED_REQ(FailCacheBlocks, FailCacheBlockItem);
struct FailCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheBlockMutationResult>>{});
};

struct BeginCleanCacheBlockItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(expectedReady, std::optional<cache::ReadyIdentity>{});
  SERDE_STRUCT_FIELD(observedGeneration, std::optional<cache::CacheGeneration>{});
  SERDE_STRUCT_FIELD(terminalState, cache::CleanupTerminalState::NONE);

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (terminalState != cache::CleanupTerminalState::NONE && terminalState != cache::CleanupTerminalState::FAILED &&
        terminalState != cache::CleanupTerminalState::REENQUEUE) {
      return INVALID("invalid cleanup terminal state");
    }
    if (expectedReady.has_value()) RETURN_ON_ERROR(expectedReady->valid());
    if (observedGeneration.has_value() && *observedGeneration == cache::CacheGeneration{}) {
      return INVALID("invalid observed generation");
    }
    return VALID;
  }
};
CACHE_BOUNDED_REQ(BeginCleanCacheBlocks, BeginCleanCacheBlockItem);
struct BeginCleanCacheBlockResult {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(cleanupEpoch, cache::CleanupEpoch{});
  SERDE_STRUCT_FIELD(deleteGeneration, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(placement, std::optional<storage::PlacementIdentity>{});
};
struct BeginCleanCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<BeginCleanCacheBlockResult>>{});
};

struct FinishCleanCacheBlockItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(cleanupEpoch, cache::CleanupEpoch{});
  SERDE_STRUCT_FIELD(retiredGeneration, std::optional<cache::CacheGeneration>{});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (cleanupEpoch == cache::CleanupEpoch{}) return INVALID("cleanupEpoch not set");
    if (retiredGeneration.has_value() && *retiredGeneration == cache::CacheGeneration{}) {
      return INVALID("invalid retired generation");
    }
    return VALID;
  }
};
CACHE_BOUNDED_REQ(FinishCleanCacheBlocks, FinishCleanCacheBlockItem);
struct FinishCleanCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheBlockMutationResult>>{});
};

#undef CACHE_BOUNDED_REQ

struct GetCacheStatusReq : ReqBase {
  SERDE_STRUCT_FIELD(inode, std::optional<InodeId>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const { return VALID; }
};
struct CacheStateCount {
  SERDE_STRUCT_FIELD(state, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(count, uint64_t{0});
};
struct CacheChargeCount {
  SERDE_STRUCT_FIELD(kind, cache::ChargeKind::NONE);
  SERDE_STRUCT_FIELD(count, uint64_t{0});
  SERDE_STRUCT_FIELD(bytes, uint64_t{0});
};
struct GetCacheStatusRsp : RspBase {
  SERDE_STRUCT_FIELD(logicalCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(usedCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(reservedCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(committedCapacity, uint64_t{0});
  SERDE_STRUCT_FIELD(stateCounts, std::vector<CacheStateCount>{});
  SERDE_STRUCT_FIELD(chargeCounts, std::vector<CacheChargeCount>{});
};

struct ListCacheBlocksReq : ReqBase {
  SERDE_STRUCT_FIELD(inode, InodeId{});
  SERDE_STRUCT_FIELD(beginBlock, cache::CacheBlockIndex{});
  SERDE_STRUCT_FIELD(limit, uint32_t{1000});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (limit > kMaxCacheBatchItems) return makeError(CacheCode::kRequestTooLarge, "limit too large");
    return VALID;
  }
};
struct CacheBlockStatus {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(state, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(ready, std::optional<cache::ReadyIdentity>{});
  SERDE_STRUCT_FIELD(chargeKind, cache::ChargeKind::NONE);
  SERDE_STRUCT_FIELD(chargedBytes, uint64_t{0});
  SERDE_STRUCT_FIELD(readyAt, UtcTime{});
  SERDE_STRUCT_FIELD(lastAccessAt, UtcTime{});
};
struct ListCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(blocks, std::vector<CacheBlockStatus>{});
  SERDE_STRUCT_FIELD(more, false);
};

struct RecoverableCachePermit {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(state, cache::CacheBlockState::NONE);
  SERDE_STRUCT_FIELD(blockLength, uint64_t{0});
  SERDE_STRUCT_FIELD(permit, storage::PermitIdentity{});
  SERDE_STRUCT_FIELD(loaderId, Uuid::zero());
  SERDE_STRUCT_FIELD(loadEpoch, uint64_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    RETURN_ON_ERROR(permit.valid());
    if (blockLength == 0 || (state != cache::CacheBlockState::QUEUED && state != cache::CacheBlockState::LOADING)) {
      return INVALID("invalid recoverable cache permit");
    }
    if (state == cache::CacheBlockState::QUEUED && (loaderId != Uuid::zero() || loadEpoch != 0)) {
      return INVALID("queued recovery item has a loader fence");
    }
    if (state == cache::CacheBlockState::LOADING && (loaderId == Uuid::zero() || loadEpoch == 0)) {
      return INVALID("loading recovery item is missing its loader fence");
    }
    return VALID;
  }
};
struct ListRecoverableCachePermitsReq : ReqBase {
  SERDE_STRUCT_FIELD(service, CacheServiceIdentity{});
  SERDE_STRUCT_FIELD(after, std::optional<cache::CacheBlockKey>{});
  SERDE_STRUCT_FIELD(limit, uint32_t{1000});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(service.valid());
    if (after) RETURN_ON_ERROR(after->valid());
    if (limit == 0 || limit > kMaxCacheBatchItems)
      return makeError(CacheCode::kRequestTooLarge, "invalid recovery page limit");
    return VALID;
  }
};
struct ListRecoverableCachePermitsRsp : RspBase {
  SERDE_STRUCT_FIELD(items, std::vector<RecoverableCachePermit>{});
  SERDE_STRUCT_FIELD(more, false);
};

struct CancelQueuedAdmissionItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(expectedPermit, storage::PermitIdentity{});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    return expectedPermit.valid();
  }
};
struct CancelQueuedAdmissionsReq : ReqBase {
  SERDE_STRUCT_FIELD(service, CacheServiceIdentity{});
  SERDE_STRUCT_FIELD(items, std::vector<CancelQueuedAdmissionItem>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(service.valid());
    if (items.size() > kMaxCacheBatchItems) return makeError(CacheCode::kRequestTooLarge, "too many cache block items");
    return VALID;
  }
};
struct CancelQueuedAdmissionsRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheBlockMutationResult>>{});
};

struct UpdateCacheBlockAccessItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(generation, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(managerReceiveTimeNs, uint64_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    if (generation == cache::CacheGeneration{} || managerReceiveTimeNs == 0)
      return makeError(StatusCode::kInvalidArg, "invalid access update identity");
    return Void{};
  }
};
struct UpdateCacheBlockAccessReq : ReqBase {
  SERDE_STRUCT_FIELD(service, CacheServiceIdentity{});
  SERDE_STRUCT_FIELD(items, std::vector<UpdateCacheBlockAccessItem>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(service.valid());
    if (items.size() > cache::kMaxPhase2BatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many access updates");
    return Void{};
  }
};
struct UpdateCacheBlockAccessResult {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(updated, false);
};
struct UpdateCacheBlockAccessRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<UpdateCacheBlockAccessResult>>{});
};

struct BeginEvictCacheBlockItem {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(expectedReady, cache::ReadyIdentity{});
  SERDE_STRUCT_FIELD(reason, cache::EvictionReason::INVALID);

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    RETURN_ON_ERROR(expectedReady.valid());
    if (reason == cache::EvictionReason::INVALID) return makeError(StatusCode::kInvalidArg, "reason not set");
    return Void{};
  }
};
struct BeginEvictCacheBlocksReq : ReqBase {
  SERDE_STRUCT_FIELD(service, CacheServiceIdentity{});
  SERDE_STRUCT_FIELD(items, std::vector<BeginEvictCacheBlockItem>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(service.valid());
    if (items.size() > cache::kMaxPhase2BatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many eviction items");
    for (const auto &item : items) RETURN_ON_ERROR(item.valid());
    return Void{};
  }
};
struct CacheEvictionIdentity {
  SERDE_STRUCT_FIELD(key, cache::CacheBlockKey{});
  SERDE_STRUCT_FIELD(ready, cache::ReadyIdentity{});
  SERDE_STRUCT_FIELD(placement, storage::PlacementIdentity{});
  SERDE_STRUCT_FIELD(evictionEpoch, cache::EvictionEpoch{});
  SERDE_STRUCT_FIELD(retireOperationId, Uuid::zero());
  SERDE_STRUCT_FIELD(reason, cache::EvictionReason::INVALID);

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(key.valid());
    RETURN_ON_ERROR(ready.valid());
    RETURN_ON_ERROR(placement.valid());
    if (evictionEpoch == cache::EvictionEpoch{} || retireOperationId == Uuid::zero() ||
        reason == cache::EvictionReason::INVALID) {
      return makeError(StatusCode::kInvalidArg, "invalid cache eviction identity");
    }
    return Void{};
  }
  bool operator==(const CacheEvictionIdentity &) const = default;
};
struct BeginEvictCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheEvictionIdentity>>{});
};
struct ListEvictingCacheBlocksReq : ReqBase {
  SERDE_STRUCT_FIELD(beginInode, uint64_t{0});
  SERDE_STRUCT_FIELD(beginBlock, cache::CacheBlockIndex{});
  SERDE_STRUCT_FIELD(limit, uint32_t{1000});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (limit == 0) return makeError(StatusCode::kInvalidArg, "limit is zero");
    if (limit > cache::kMaxPhase2BatchItems) return makeError(CacheCode::kRequestTooLarge, "limit too large");
    return Void{};
  }
};
struct ListEvictingCacheBlocksRsp : RspBase {
  SERDE_STRUCT_FIELD(items, std::vector<CacheEvictionIdentity>{});
  SERDE_STRUCT_FIELD(more, false);
};

struct CacheStorageEvent {
  SERDE_STRUCT_FIELD(sourceId, storage::PhysicalDiskId{});
  SERDE_STRUCT_FIELD(sequence, uint64_t{0});
  SERDE_STRUCT_FIELD(type, cache::CacheStorageEventType::DELETED);
  SERDE_STRUCT_FIELD(key, storage::CacheChunkKey{});
  SERDE_STRUCT_FIELD(generation, cache::CacheGeneration{});
  SERDE_STRUCT_FIELD(placement, storage::PlacementIdentity{});
  SERDE_STRUCT_FIELD(evictionEpoch, cache::EvictionEpoch{});
  SERDE_STRUCT_FIELD(operationId, Uuid::zero());

 public:
  Result<Void> valid() const {
    RETURN_ON_ERROR(sourceId.valid());
    RETURN_ON_ERROR(key.valid());
    RETURN_ON_ERROR(placement.valid());
    if (sequence == 0 || generation == cache::CacheGeneration{} || operationId == Uuid::zero())
      return makeError(StatusCode::kInvalidArg, "invalid storage event identity");
    return Void{};
  }
};
struct ReportCacheStorageEventsReq : ReqBase {
  SERDE_STRUCT_FIELD(events, std::vector<CacheStorageEvent>{});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (events.size() > cache::kMaxPhase2BatchItems)
      return makeError(CacheCode::kRequestTooLarge, "too many storage events");
    for (const auto &event : events) RETURN_ON_ERROR(event.valid());
    return Void{};
  }
};
struct CacheStorageEventAck {
  SERDE_STRUCT_FIELD(sourceId, storage::PhysicalDiskId{});
  SERDE_STRUCT_FIELD(sequence, uint64_t{0});
  SERDE_STRUCT_FIELD(deadLettered, false);
};
struct ReportCacheStorageEventsRsp : RspBase {
  SERDE_STRUCT_FIELD(results, std::vector<Result<CacheStorageEventAck>>{});
};

struct ListCacheEventDeadLettersReq : ReqBase {
  SERDE_STRUCT_FIELD(sourceId, std::optional<storage::PhysicalDiskId>{});
  SERDE_STRUCT_FIELD(beginSequence, uint64_t{0});
  SERDE_STRUCT_FIELD(limit, uint32_t{1000});
  SERDE_STRUCT_FIELD(cacheProtocolVersion, uint32_t{0});

 public:
  Result<Void> valid() const {
    if (limit > cache::kMaxPhase2BatchItems) return makeError(CacheCode::kRequestTooLarge, "limit too large");
    return Void{};
  }
};
struct CacheEventDeadLetter {
  SERDE_STRUCT_FIELD(event, CacheStorageEvent{});
  SERDE_STRUCT_FIELD(errorCode, uint32_t{0});
  SERDE_STRUCT_FIELD(reason, String{});
};
struct ListCacheEventDeadLettersRsp : RspBase {
  SERDE_STRUCT_FIELD(items, std::vector<CacheEventDeadLetter>{});
  SERDE_STRUCT_FIELD(more, false);
};

// testRpc
struct TestRpcReq : ReqBase {
  SERDE_STRUCT_FIELD(path, PathAt());
  SERDE_STRUCT_FIELD(flags, uint32_t(0));

 public:
  Result<Void> valid() const { return VALID; }
};
struct TestRpcRsp : RspBase {
  SERDE_STRUCT_FIELD(stat, Inode());
};

/* MetaSerde service */
SERDE_SERVICE(MetaSerde, 4) {
#define META_SERVICE_METHOD(NAME, CODE, REQ, RSP)                                   \
  SERDE_SERVICE_METHOD(NAME, CODE, REQ, RSP);                                       \
                                                                                    \
 public:                                                                            \
  static constexpr std::string_view getRpcName(const REQ &) { return #NAME; }       \
  static_assert(serde::SerializableToBytes<REQ> && serde::SerializableToJson<REQ>); \
  static_assert(serde::SerializableToBytes<RSP> && serde::SerializableToJson<RSP>)

  META_SERVICE_METHOD(statFs, 1, StatFsReq, StatFsRsp);
  META_SERVICE_METHOD(stat, 2, StatReq, StatRsp);
  META_SERVICE_METHOD(create, 3, CreateReq, CreateRsp);
  META_SERVICE_METHOD(mkdirs, 4, MkdirsReq, MkdirsRsp);
  META_SERVICE_METHOD(symlink, 5, SymlinkReq, SymlinkRsp);
  META_SERVICE_METHOD(hardLink, 6, HardLinkReq, HardLinkRsp);
  META_SERVICE_METHOD(remove, 7, RemoveReq, RemoveRsp);
  META_SERVICE_METHOD(open, 8, OpenReq, OpenRsp);
  META_SERVICE_METHOD(sync, 9, SyncReq, SyncRsp);
  META_SERVICE_METHOD(close, 10, CloseReq, CloseRsp);
  META_SERVICE_METHOD(rename, 11, RenameReq, RenameRsp);
  META_SERVICE_METHOD(list, 12, ListReq, ListRsp);
  // deperated:
  META_SERVICE_METHOD(truncate, 13, TruncateReq, TruncateRsp);
  META_SERVICE_METHOD(getRealPath, 14, GetRealPathReq, GetRealPathRsp);
  META_SERVICE_METHOD(setAttr, 15, SetAttrReq, SetAttrRsp);
  META_SERVICE_METHOD(pruneSession, 16, PruneSessionReq, PruneSessionRsp);
  META_SERVICE_METHOD(dropUserCache, 17, DropUserCacheReq, DropUserCacheRsp);
  META_SERVICE_METHOD(authenticate, 18, AuthReq, AuthRsp);
  META_SERVICE_METHOD(lockDirectory, 19, LockDirectoryReq, LockDirectoryRsp);
  META_SERVICE_METHOD(batchStat, 20, BatchStatReq, BatchStatRsp);
  META_SERVICE_METHOD(batchStatByPath, 21, BatchStatByPathReq, BatchStatByPathRsp);
  META_SERVICE_METHOD(importOriginFile, 22, ImportOriginFileReq, ImportOriginFileRsp);
  META_SERVICE_METHOD(batchImportOriginFiles, 23, BatchImportOriginFilesReq, BatchImportOriginFilesRsp);
  META_SERVICE_METHOD(refreshOriginFile, 24, RefreshOriginFileReq, RefreshOriginFileRsp);
  META_SERVICE_METHOD(getFileReadPlan, 25, GetFileReadPlanReq, GetFileReadPlanRsp);
  META_SERVICE_METHOD(enqueueCacheBlocks, 26, EnqueueCacheBlocksReq, EnqueueCacheBlocksRsp);
  META_SERVICE_METHOD(acquireCacheBlocks, 27, AcquireCacheBlocksReq, AcquireCacheBlocksRsp);
  META_SERVICE_METHOD(commitCacheBlocks, 28, CommitCacheBlocksReq, CommitCacheBlocksRsp);
  META_SERVICE_METHOD(failCacheBlocks, 29, FailCacheBlocksReq, FailCacheBlocksRsp);
  META_SERVICE_METHOD(beginCleanCacheBlocks, 30, BeginCleanCacheBlocksReq, BeginCleanCacheBlocksRsp);
  META_SERVICE_METHOD(finishCleanCacheBlocks, 31, FinishCleanCacheBlocksReq, FinishCleanCacheBlocksRsp);
  META_SERVICE_METHOD(getCacheStatus, 32, GetCacheStatusReq, GetCacheStatusRsp);
  META_SERVICE_METHOD(listCacheBlocks, 33, ListCacheBlocksReq, ListCacheBlocksRsp);
  META_SERVICE_METHOD(updateCacheBlockAccess, 34, UpdateCacheBlockAccessReq, UpdateCacheBlockAccessRsp);
  META_SERVICE_METHOD(beginEvictCacheBlocks, 35, BeginEvictCacheBlocksReq, BeginEvictCacheBlocksRsp);
  META_SERVICE_METHOD(listEvictingCacheBlocks, 36, ListEvictingCacheBlocksReq, ListEvictingCacheBlocksRsp);
  META_SERVICE_METHOD(reportCacheStorageEvents, 37, ReportCacheStorageEventsReq, ReportCacheStorageEventsRsp);
  META_SERVICE_METHOD(listCacheEventDeadLetters, 38, ListCacheEventDeadLettersReq, ListCacheEventDeadLettersRsp);
  META_SERVICE_METHOD(listRecoverableCachePermits, 39, ListRecoverableCachePermitsReq, ListRecoverableCachePermitsRsp);
  META_SERVICE_METHOD(cancelQueuedAdmissions, 40, CancelQueuedAdmissionsReq, CancelQueuedAdmissionsRsp);

  META_SERVICE_METHOD(testRpc, 50, TestRpcReq, TestRpcRsp);

#undef META_SERVICE_METHOD
};

}  // namespace hf3fs::meta
