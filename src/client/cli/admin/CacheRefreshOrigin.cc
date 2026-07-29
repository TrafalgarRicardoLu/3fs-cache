#include "CacheRefreshOrigin.h"

#include "AdminEnv.h"
#include "CacheOriginCli.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/kv/WithTransaction.h"
#include "common/utils/MagicEnum.hpp"
#include "fdb/FDBRetryStrategy.h"
#include "meta/components/OriginNamespaceManager.h"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-refresh-origin");
  parser.add_argument("path");
  parser.add_argument("object-key");
  parser.add_argument("--request-id");
  parser.add_argument("--permission");
  cache_admin::addOriginArguments(parser);
  return parser;
}

kv::FDBRetryStrategy retryStrategy() { return kv::FDBRetryStrategy({1_s, 10, true}); }

CoTryTask<std::optional<meta::server::RefreshOriginFileRecord>> loadRefresh(AdminEnv &env, Uuid requestId) {
  auto handler = [requestId](kv::IReadWriteTransaction &transaction) {
    return meta::server::OriginNamespaceManager::loadRefresh(transaction, requestId);
  };
  co_return co_await kv::WithTransaction(retryStrategy())
      .run(env.kvEngineGetter()->createReadWriteTransaction(), std::move(handler));
}

CoTryTask<std::optional<meta::server::OriginCleanupJobRecord>> loadCleanup(AdminEnv &env, Uuid jobId) {
  auto handler = [jobId](kv::IReadOnlyTransaction &transaction) {
    return meta::server::OriginNamespaceManager::snapshotLoadCleanup(transaction, jobId);
  };
  co_return co_await kv::WithTransaction(retryStrategy())
      .run(env.kvEngineGetter()->createReadonlyTransaction(), std::move(handler));
}

CoTryTask<Dispatcher::OutputTable> outputRefresh(AdminEnv &env,
                                                 Uuid requestId,
                                                 meta::InodeId oldInode,
                                                 const meta::RefreshOriginFileRsp &response) {
  auto cleanup = co_await loadCleanup(env, response.cleanupJobId);
  CO_RETURN_ON_ERROR(cleanup);
  auto cleanupState = std::string{"NOT_FOUND"};
  auto cleanupDetail = std::string{"run cache-cleanup --job-id <CleanupJobId>"};
  if (cleanup->has_value()) {
    cleanupState = std::string(magic_enum::enum_name((**cleanup).state));
    cleanupDetail = fmt::format("cursor={}/{} remaining_blocks={} remaining_bytes={}",
                                (**cleanup).cursor,
                                (**cleanup).endBlock,
                                (**cleanup).remainingNonTerminalBlocks,
                                (**cleanup).remainingChargedBytes);
  }
  co_return Dispatcher::OutputTable{{"RequestId", requestId.toHexString()},
                                    {"CleanupJobId", response.cleanupJobId.toHexString()},
                                    {"OldInode", oldInode.toHexString()},
                                    {"NewInode", response.newInode.id.toHexString()},
                                    {"ObjectVersion", response.newInode.asOriginFile().object.version.value},
                                    {"CleanupState", std::move(cleanupState)},
                                    {"CleanupDetail", std::move(cleanupDetail)}};
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  auto path = parser.get<std::string>("path");
  auto key = parser.get<std::string>("object-key");
  auto pathAt = meta::PathAt{env.currentDirId, Path{path}};
  std::optional<Uuid> requestedId;
  if (auto value = parser.present<std::string>("--request-id")) {
    auto parsed = Uuid::fromHexString(*value);
    CO_RETURN_ON_ERROR(parsed);
    requestedId = *parsed;
  }

  auto origin = cache_admin::parseOriginOptions(parser);
  CO_RETURN_ON_ERROR(origin);
  auto store = cache_admin::makeObjectStore(*origin);
  CO_RETURN_ON_ERROR(store);
  auto object = co_await cache_admin::headObject(*origin, **store, key);
  CO_RETURN_ON_ERROR(object);

  if (requestedId) {
    auto previous = co_await loadRefresh(env, *requestedId);
    CO_RETURN_ON_ERROR(previous);
    if (previous->has_value()) {
      if ((**previous).path != pathAt || (**previous).newMetadata.object != object->identity) {
        co_return makeError(CacheCode::kStateConflict,
                            "refresh request id was already used for a different path or object");
      }
      co_return co_await outputRefresh(env, *requestedId, (**previous).expectedInode, (**previous).response());
    }
  }

  auto oldInode = co_await env.metaClientGetter()->stat(env.userInfo, env.currentDirId, Path{path}, false);
  CO_RETURN_ON_ERROR(oldInode);
  if (!oldInode->isOriginFile()) co_return makeError(MetaCode::kNotFile, "path is not an OriginFile");
  if (object->identity == oldInode->asOriginFile().object) {
    co_return makeError(CacheCode::kStateConflict, "origin object identity has not changed");
  }

  auto permission = cache_admin::parsePermission(parser, oldInode->acl.perm);
  CO_RETURN_ON_ERROR(permission);
  meta::OriginFileMetadata metadata;
  metadata.object = object->identity;
  metadata.objectSize = object->size;
  metadata.tableId = oldInode->fileLayout().tableId;
  metadata.blockSize = oldInode->fileLayout().chunkSize;
  metadata.stripeSize = oldInode->fileLayout().stripeSize;
  metadata.permission = *permission;

  auto requestId = requestedId.value_or(cache_admin::stableRefreshRequestId(pathAt, oldInode->id, metadata.object));

  meta::RefreshOriginFileReq request;
  request.user = env.userInfo;
  request.requestId = requestId;
  request.path = pathAt;
  request.expectedInode = oldInode->id;
  request.oldObject = oldInode->asOriginFile().object;
  request.newMetadata = std::move(metadata);
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await env.metaClientGetter()->refreshOriginFile(std::move(request));
  CO_RETURN_ON_ERROR(result);

  co_return co_await outputRefresh(env, requestId, oldInode->id, *result);
}

}  // namespace

CoTryTask<void> registerCacheRefreshOriginHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
