#include "CacheOrchestration.h"

#include <algorithm>
#include <chrono>

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/serde/Serde.h"
#include "common/utils/FileUtils.h"
#include "common/utils/MagicEnum.hpp"

namespace hf3fs::client::cli {
namespace {

Result<Uuid> parseId(const argparse::ArgumentParser &parser, std::string_view option) {
  auto value = parser.present<std::string>(option);
  if (!value) return makeError(CliCode::kWrongUsage, fmt::format("{} is required", option));
  return Uuid::fromHexString(*value);
}

Result<std::vector<cache::DatasetSource>> parseSources(const argparse::ArgumentParser &parser) {
  std::vector<cache::DatasetSource> sources;
  auto paths = parser.get<std::vector<std::string>>("--path");
  for (auto &path : paths) sources.push_back(cache::DatasetSource{cache::NamespacePathSource{std::move(path), true}});
  if (auto manifest = parser.present<std::string>("--manifest")) {
    sources.push_back(cache::DatasetSource{cache::ManifestPathSource{std::move(*manifest)}});
  }
  auto origin = parser.present<uint32_t>("--origin-id");
  auto bucket = parser.present<std::string>("--bucket");
  auto prefix = parser.present<std::string>("--prefix");
  auto destination = parser.present<std::string>("--destination-root");
  auto s3Fields = static_cast<unsigned>(origin.has_value()) + static_cast<unsigned>(bucket.has_value()) +
                  static_cast<unsigned>(prefix.has_value()) + static_cast<unsigned>(destination.has_value());
  if (s3Fields != 0 && s3Fields != 4) {
    return makeError(CliCode::kWrongUsage,
                     "--origin-id, --bucket, --prefix and --destination-root must be specified together");
  }
  if (s3Fields == 4) {
    sources.push_back(cache::DatasetSource{cache::S3PrefixSource{cache::OriginId{*origin},
                                                                 std::move(*bucket),
                                                                 std::move(*prefix),
                                                                 std::move(*destination)}});
  }
  RETURN_ON_ERROR(cache::validateDatasetSources(sources));
  return sources;
}

Result<cache::PrefetchJobSpec> parseSpec(const argparse::ArgumentParser &parser, const flat::UserInfo &user) {
  cache::PrefetchJobSpec spec;
  if (auto file = parser.present<std::string>("--spec")) {
    auto contents = loadFile(Path{*file});
    RETURN_ON_ERROR(contents);
    auto parsed =
        file->ends_with(".json") ? serde::fromJsonString(spec, *contents) : serde::fromTomlString(spec, *contents);
    RETURN_ON_ERROR(parsed);
    if (spec.ownerUid != user.uid) return makeError(CliCode::kWrongUsage, "spec ownerUid differs from the caller");
    return spec;
  }
  auto id = parseId(parser, "--job-id");
  RETURN_ON_ERROR(id);
  auto sources = parseSources(parser);
  RETURN_ON_ERROR(sources);
  spec.jobId = cache::PrefetchJobId{*id};
  spec.ownerUid = user.uid;
  spec.sources = std::move(*sources);
  spec.priority = parser.get<uint32_t>("--priority");
  spec.maxParallelLoads = parser.get<uint32_t>("--max-parallel");
  spec.bandwidthLimitBytesPerSec = parser.get<uint64_t>("--bandwidth");
  spec.requiredReadyBps = parser.get<uint32_t>("--ready-bps");
  spec.pinTtlMs = parser.get<uint64_t>("--pin-ttl-ms");
  spec.pinAfterReady = spec.pinTtlMs != 0;
  RETURN_ON_ERROR(spec.valid());
  return spec;
}

void addSourceArguments(argparse::ArgumentParser &parser) {
  parser.add_argument("--path").nargs(argparse::nargs_pattern::any);
  parser.add_argument("--manifest");
  parser.add_argument("--origin-id").scan<'u', uint32_t>();
  parser.add_argument("--bucket");
  parser.add_argument("--prefix");
  parser.add_argument("--destination-root");
}

auto prefetchParser() {
  argparse::ArgumentParser parser("cache-prefetch");
  parser.add_argument("action");
  parser.add_argument("--job-id");
  parser.add_argument("--spec");
  addSourceArguments(parser);
  parser.add_argument("--priority").default_value(uint32_t{0}).scan<'u', uint32_t>();
  parser.add_argument("--max-parallel").default_value(uint32_t{1}).scan<'u', uint32_t>();
  parser.add_argument("--bandwidth").default_value(uint64_t{0}).scan<'u', uint64_t>();
  parser.add_argument("--ready-bps").default_value(cache::kReadyRatioScaleBps).scan<'u', uint32_t>();
  parser.add_argument("--pin-ttl-ms").default_value(uint64_t{0}).scan<'u', uint64_t>();
  parser.add_argument("--limit").default_value(uint32_t{100}).scan<'u', uint32_t>();
  return parser;
}

Dispatcher::OutputRow jobRow(const cache::PrefetchJobRecord &job) {
  return {job.spec.jobId.toUnderType().toHexString(),
          std::string(magic_enum::enum_name(job.state)),
          std::to_string(job.plannedBytes),
          std::to_string(job.readyBytes),
          std::to_string(cacheReadyBps(job.readyBytes, job.plannedBytes)),
          std::to_string(job.spec.priority),
          job.error};
}

CoTryTask<Dispatcher::OutputTable> handlePrefetch(IEnv &ienv,
                                                  const argparse::ArgumentParser &parser,
                                                  const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  auto action = parser.get<std::string>("action");
  Dispatcher::OutputTable table{{"JobId", "State", "PlannedBytes", "ReadyBytes", "ReadyBps", "Priority", "Error"}};
  if (action == "create") {
    auto spec = parseSpec(parser, env.userInfo);
    CO_RETURN_ON_ERROR(spec);
    cache_manager::CreatePrefetchJobReq request{env.userInfo, std::move(*spec), cache::kCachePhase3ProtocolVersion};
    auto result = co_await env.cacheManagerStubGetter()->createPrefetchJob(request);
    CO_RETURN_ON_ERROR(result);
    table.push_back(jobRow(result->job));
  } else if (action == "list") {
    cache_manager::ListPrefetchJobsReq request;
    request.user = env.userInfo;
    request.limit = parser.get<uint32_t>("--limit");
    request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto result = co_await env.cacheManagerStubGetter()->listPrefetchJobs(request);
    CO_RETURN_ON_ERROR(result);
    for (const auto &job : result->jobs) table.push_back(jobRow(job));
  } else {
    auto id = parseId(parser, "--job-id");
    CO_RETURN_ON_ERROR(id);
    if (action == "status") {
      cache_manager::GetPrefetchJobReq request{env.userInfo,
                                               cache::PrefetchJobId{*id},
                                               cache::kCachePhase3ProtocolVersion};
      auto result = co_await env.cacheManagerStubGetter()->getPrefetchJob(request);
      CO_RETURN_ON_ERROR(result);
      table.push_back(jobRow(result->job));
    } else if (action == "cancel") {
      cache_manager::CancelPrefetchJobReq request{env.userInfo,
                                                  cache::PrefetchJobId{*id},
                                                  cache::kCachePhase3ProtocolVersion};
      auto result = co_await env.cacheManagerStubGetter()->cancelPrefetchJob(request);
      CO_RETURN_ON_ERROR(result);
      table.push_back(jobRow(result->job));
    } else {
      co_return makeError(CliCode::kWrongUsage, "action must be create, status, list or cancel");
    }
  }
  co_return table;
}

auto pinParser() {
  argparse::ArgumentParser parser("cache-pin");
  parser.add_argument("action");
  parser.add_argument("--pin-id");
  addSourceArguments(parser);
  parser.add_argument("--priority").default_value(uint32_t{0}).scan<'u', uint32_t>();
  parser.add_argument("--ttl-ms").default_value(uint64_t{0}).scan<'u', uint64_t>();
  parser.add_argument("--prefetch-missing").default_value(false).implicit_value(true);
  return parser;
}

auto reconcileParser() {
  argparse::ArgumentParser parser("cache-reconcile");
  parser.add_argument("action");
  parser.add_argument("--confirm").default_value(false).implicit_value(true);
  parser.add_argument("--timeout-ms").default_value(uint64_t{30000}).scan<'u', uint64_t>();
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handleReconcile(IEnv &ienv,
                                                   const argparse::ArgumentParser &parser,
                                                   const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  const auto action = parser.get<std::string>("action");
  ENSURE_USAGE(action == "run" || action == "dry-run" || action == "status", "action must be run, dry-run or status");
  if (action == "status") {
    cache_manager::GetCacheStatusReq request;
    request.user = env.userInfo;
    request.cacheProtocolVersion = cache::kCacheProtocolVersion;
    auto result = co_await env.cacheManagerStubGetter()->getCacheStatus(request);
    CO_RETURN_ON_ERROR(result);
    co_return cacheReconcileTable(result->reconcile, result->reconcileDryRun);
  }
  const bool dryRun = action == "dry-run";
  const bool confirm = parser.get<bool>("--confirm");
  ENSURE_USAGE(dryRun || confirm, "repair reconcile requires --confirm");
  const auto timeoutMs = parser.get<uint64_t>("--timeout-ms");
  ENSURE_USAGE(timeoutMs != 0, "--timeout-ms must be positive");
  cache_manager::RunCacheReconcileReq request{env.userInfo, dryRun, confirm, cache::kCachePhase4ProtocolVersion};
  net::UserRequestOptions options;
  options.timeout = Duration(std::chrono::milliseconds(timeoutMs));
  auto result = co_await env.cacheManagerStubGetter()->runCacheReconcile(request, options);
  CO_RETURN_ON_ERROR(result);
  co_return cacheReconcileTable(result->progress, result->dryRun);
}

auto uploadParser() {
  argparse::ArgumentParser parser("cache-upload");
  parser.add_argument("action");
  parser.add_argument("--job-id");
  parser.add_argument("--owner-uid").scan<'u', uint32_t>();
  parser.add_argument("--after");
  parser.add_argument("--limit").default_value(uint32_t{100}).scan<'u', uint32_t>();
  parser.add_argument("--active-only").default_value(false).implicit_value(true);
  parser.add_argument("--confirm").default_value(false).implicit_value(true);
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handleUpload(IEnv &ienv,
                                                const argparse::ArgumentParser &parser,
                                                const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  const auto action = parser.get<std::string>("action");
  ENSURE_USAGE(action == "status" || action == "cancel" || action == "retry", "action must be status, cancel or retry");
  Dispatcher::OutputTable table{{"JobId",
                                 "OwnerUid",
                                 "Path",
                                 "State",
                                 "StateVersion",
                                 "StagingBytes",
                                 "UploadedParts",
                                 "UpdatedAtMs",
                                 "HasError",
                                 "More"}};
  auto idText = parser.present<std::string>("--job-id");
  if (action == "status" && !idText) {
    meta::AdminListUploadJobsReq request;
    request.user = env.userInfo;
    if (auto owner = parser.present<uint32_t>("--owner-uid")) request.ownerUid = flat::Uid{*owner};
    if (auto after = parser.present<std::string>("--after")) {
      auto parsed = Uuid::fromHexString(*after);
      CO_RETURN_ON_ERROR(parsed);
      request.after = cache::UploadJobId{*parsed};
    }
    request.includeTerminal = !parser.get<bool>("--active-only");
    request.limit = parser.get<uint32_t>("--limit");
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto result = co_await env.metaClientGetter()->adminListUploadJobs(std::move(request));
    CO_RETURN_ON_ERROR(result);
    for (const auto &job : result->jobs) table.push_back(cacheUploadJobRow(job, result->more));
    co_return table;
  }
  ENSURE_USAGE(idText.has_value(), "--job-id is required");
  if (action != "status") {
    ENSURE_USAGE(parser.get<bool>("--confirm"), fmt::format("{} requires --confirm", action));
  }
  auto parsed = Uuid::fromHexString(*idText);
  CO_RETURN_ON_ERROR(parsed);
  meta::AdminListUploadJobsReq get;
  get.user = env.userInfo;
  get.jobId = cache::UploadJobId{*parsed};
  get.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto current = co_await env.metaClientGetter()->adminListUploadJobs(std::move(get));
  CO_RETURN_ON_ERROR(current);
  if (current->jobs.size() != 1) co_return makeError(CacheCode::kInvalidResponse, "invalid upload job response");
  auto &job = current->jobs.front();
  if (action == "status") {
    table.push_back(cacheUploadJobRow(job, false));
    co_return table;
  }
  meta::AdminMutateUploadJobReq mutate;
  mutate.user = env.userInfo;
  mutate.jobId = job.jobId;
  mutate.expectedStateVersion = job.stateVersion;
  mutate.mutation = action == "cancel" ? meta::AdminUploadMutation::CANCEL : meta::AdminUploadMutation::RETRY;
  mutate.confirm = true;
  mutate.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto result = co_await env.metaClientGetter()->adminMutateUploadJob(std::move(mutate));
  CO_RETURN_ON_ERROR(result);
  table.push_back(cacheUploadJobRow(result->job, false));
  co_return table;
}

CoTryTask<Dispatcher::OutputTable> handlePin(IEnv &ienv,
                                             const argparse::ArgumentParser &parser,
                                             const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  auto action = parser.get<std::string>("action");
  auto id = parseId(parser, "--pin-id");
  CO_RETURN_ON_ERROR(id);
  auto pinId = cache::PinOwnerId{*id};
  if (action == "create") {
    auto sources = parseSources(parser);
    CO_RETURN_ON_ERROR(sources);
    cache_manager::PinDatasetReq request;
    request.user = env.userInfo;
    request.pinId = pinId;
    request.sources = std::move(*sources);
    request.prefetchMissing = parser.get<bool>("--prefetch-missing");
    request.priority = parser.get<uint32_t>("--priority");
    request.ttlMs = parser.get<uint64_t>("--ttl-ms");
    request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto result = co_await env.cacheManagerStubGetter()->pinDataset(request);
    CO_RETURN_ON_ERROR(result);
    co_return Dispatcher::OutputTable{
        {"PinId", "PlannedBytes"},
        {result->pinId.toUnderType().toHexString(), std::to_string(result->plannedBytes)}};
  }
  if (action == "remove") {
    cache_manager::UnpinDatasetReq request{env.userInfo, pinId, cache::kCachePhase3ProtocolVersion};
    auto result = co_await env.cacheManagerStubGetter()->unpinDataset(request);
    CO_RETURN_ON_ERROR(result);
    co_return Dispatcher::OutputTable{{"PinId", "RemovedBlocks"},
                                      {id->toHexString(), std::to_string(result->removedBlocks)}};
  }
  if (action == "status") {
    cache_manager::GetPinStatusReq request{env.userInfo, pinId, cache::kCachePhase3ProtocolVersion};
    auto result = co_await env.cacheManagerStubGetter()->getPinStatus(request);
    CO_RETURN_ON_ERROR(result);
    co_return Dispatcher::OutputTable{{"PinId", "PlannedBytes", "PinnedBytes", "ReadyBytes", "ExpiresAtMs"},
                                      {id->toHexString(),
                                       std::to_string(result->plannedBytes),
                                       std::to_string(result->pinnedBytes),
                                       std::to_string(result->readyBytes),
                                       std::to_string(result->expiresAtMs)}};
  }
  co_return makeError(CliCode::kWrongUsage, "action must be create, status or remove");
}

}  // namespace

uint32_t cacheReadyBps(uint64_t readyBytes, uint64_t plannedBytes) {
  if (plannedBytes == 0) return 0;
  auto scaled = (__uint128_t{readyBytes} * cache::kReadyRatioScaleBps) / plannedBytes;
  return static_cast<uint32_t>(std::min<__uint128_t>(scaled, cache::kReadyRatioScaleBps));
}

Dispatcher::OutputTable cacheReconcileTable(const cache::ReconcileProgress &progress, bool dryRun) {
  return {{"RunId",
           "State",
           "DryRun",
           "Scanned",
           "Repaired",
           "Orphaned",
           "Missing",
           "Conflicts",
           "Retryable",
           "StartedAtMs",
           "UpdatedAtMs",
           "HasError"},
          {progress.runId == cache::ReconcileRunId{} ? "" : progress.runId.toUnderType().toHexString(),
           std::string(magic_enum::enum_name(progress.state)),
           dryRun ? "true" : "false",
           std::to_string(progress.scanned),
           std::to_string(progress.repaired),
           std::to_string(progress.orphaned),
           std::to_string(progress.missing),
           std::to_string(progress.conflicts),
           std::to_string(progress.retryable),
           std::to_string(progress.startedAtMs),
           std::to_string(progress.updatedAtMs),
           progress.error.empty() ? "false" : "true"}};
}

Dispatcher::OutputRow cacheUploadJobRow(const cache::UploadJobRecord &job, bool more) {
  return {job.jobId.toUnderType().toHexString(),
          std::to_string(job.ownerUid.toUnderType()),
          job.path,
          std::string(magic_enum::enum_name(job.state)),
          std::to_string(job.stateVersion),
          std::to_string(job.stagingLength),
          std::to_string(job.parts.size()),
          std::to_string(job.updatedAtMs),
          job.error.empty() ? "false" : "true",
          more ? "true" : "false"};
}

CoTryTask<void> registerCachePrefetchHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(prefetchParser, handlePrefetch);
}

CoTryTask<void> registerCachePinHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(pinParser, handlePin);
}

CoTryTask<void> registerCacheReconcileHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(reconcileParser, handleReconcile);
}

CoTryTask<void> registerCacheUploadHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(uploadParser, handleUpload);
}

}  // namespace hf3fs::client::cli
