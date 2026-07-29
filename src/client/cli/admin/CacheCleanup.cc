#include "CacheCleanup.h"

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/kv/WithTransaction.h"
#include "common/utils/MagicEnum.hpp"
#include "fdb/FDBRetryStrategy.h"
#include "meta/store/cache/CleanupJobStore.h"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-cleanup");
  parser.add_argument("--inode").scan<'u', uint64_t>();
  parser.add_argument("--begin").default_value(uint32_t{0}).scan<'u', uint32_t>();
  parser.add_argument("--count").scan<'u', uint32_t>();
  parser.add_argument("--job-id");
  parser.add_argument("--max-batches").default_value(uint32_t{1024}).scan<'u', uint32_t>();
  return parser;
}

kv::FDBRetryStrategy retryStrategy() { return kv::FDBRetryStrategy({1_s, 10, true}); }

CoTryTask<std::optional<meta::server::OriginCleanupJobRecord>> loadJob(AdminEnv &env, Uuid jobId) {
  auto handler = [jobId](kv::IReadOnlyTransaction &transaction) {
    return meta::server::CleanupJobStore::snapshotLoad(transaction, jobId);
  };
  co_return co_await kv::WithTransaction(retryStrategy())
      .run(env.kvEngineGetter()->createReadonlyTransaction(), std::move(handler));
}

CoTryTask<meta::server::OriginCleanupJobRecord> advanceJob(AdminEnv &env, Uuid jobId) {
  auto handler = [jobId](kv::IReadWriteTransaction &transaction) {
    return meta::server::CleanupJobStore::advance(transaction, jobId);
  };
  co_return co_await kv::WithTransaction(retryStrategy())
      .run(env.kvEngineGetter()->createReadWriteTransaction(), std::move(handler));
}

CoTryTask<cache_manager::AdminCleanupCacheBlocksRsp> cleanup(AdminEnv &env,
                                                             meta::InodeId inode,
                                                             uint32_t begin,
                                                             uint32_t count) {
  cache_manager::AdminCleanupCacheBlocksReq request;
  request.user = env.userInfo;
  request.inode = inode;
  request.beginBlock = cache::CacheBlockIndex{begin};
  request.blockCount = count;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  co_return co_await env.cacheManagerStubGetter()->adminCleanupCacheBlocks(request);
}

void appendResults(Dispatcher::OutputTable &table, const cache_manager::AdminCleanupCacheBlocksRsp &response) {
  for (const auto &result : response.results) {
    if (result.hasError()) {
      table.push_back({"", "ERROR", result.error().describe()});
    } else {
      table.push_back(
          {std::to_string(result->block.toUnderType()), std::string(magic_enum::enum_name(result->status)), ""});
    }
  }
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  auto jobValue = parser.present<std::string>("--job-id");
  auto inodeValue = parser.present<uint64_t>("--inode");
  auto countValue = parser.present<uint32_t>("--count");
  ENSURE_USAGE(jobValue.has_value() != inodeValue.has_value(), "specify exactly one of --job-id or --inode");
  if (inodeValue) ENSURE_USAGE(countValue && *countValue > 0, "--count is required with --inode");

  Dispatcher::OutputTable table{{"Block", "Status", "Detail"}};
  if (inodeValue) {
    auto result = co_await cleanup(env, meta::InodeId{*inodeValue}, parser.get<uint32_t>("--begin"), *countValue);
    CO_RETURN_ON_ERROR(result);
    appendResults(table, *result);
    co_return table;
  }

  auto parsedJob = Uuid::fromHexString(*jobValue);
  CO_RETURN_ON_ERROR(parsedJob);
  auto maxBatches = parser.get<uint32_t>("--max-batches");
  ENSURE_USAGE(maxBatches > 0, "--max-batches must be greater than zero");
  for (uint32_t batch = 0; batch < maxBatches; ++batch) {
    auto job = co_await loadJob(env, *parsedJob);
    CO_RETURN_ON_ERROR(job);
    if (!job->has_value()) co_return makeError(CacheCode::kNotFound, "cleanup job not found");
    if ((**job).complete()) {
      table.push_back({"JOB", "COMPLETE", parsedJob->toHexString()});
      co_return table;
    }
    auto begin = (**job).cursor;
    auto end = begin + std::min((**job).endBlock - begin, uint64_t{meta::kMaxCacheBatchItems});
    if (begin > std::numeric_limits<uint32_t>::max() || end > uint64_t{std::numeric_limits<uint32_t>::max()} + 1) {
      co_return makeError(StatusCode::kInvalidArg, "cleanup job block range is too large");
    }
    if (begin != end) {
      auto result =
          co_await cleanup(env, (**job).inode, static_cast<uint32_t>(begin), static_cast<uint32_t>(end - begin));
      CO_RETURN_ON_ERROR(result);
      appendResults(table, *result);
    }
    auto advanced = co_await advanceJob(env, *parsedJob);
    CO_RETURN_ON_ERROR(advanced);
    if (advanced->complete()) {
      table.push_back({"JOB", "COMPLETE", parsedJob->toHexString()});
      co_return table;
    }
  }
  table.push_back({"JOB", "RUNNING", "max batch limit reached; rerun with the same --job-id"});
  co_return table;
}

}  // namespace

CoTryTask<void> registerCacheCleanupHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
