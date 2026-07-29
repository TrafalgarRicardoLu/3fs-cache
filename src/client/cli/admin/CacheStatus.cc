#include "CacheStatus.h"

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/MagicEnum.hpp"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-status");
  parser.add_argument("--inode").scan<'u', uint64_t>();
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  meta::GetCacheStatusReq request;
  request.user = env.userInfo;
  if (auto inode = parser.present<uint64_t>("--inode")) request.inode = meta::InodeId{*inode};
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await env.metaClientGetter()->getCacheStatus(std::move(request));
  CO_RETURN_ON_ERROR(result);

  cache_manager::GetCacheStatusReq managerRequest;
  managerRequest.user = env.userInfo;
  if (auto inode = parser.present<uint64_t>("--inode")) managerRequest.inode = meta::InodeId{*inode};
  managerRequest.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto managerResult = co_await env.cacheManagerStubGetter()->getCacheStatus(managerRequest);
  CO_RETURN_ON_ERROR(managerResult);

  Dispatcher::OutputTable table{
      {"Metric", "Value"},
      {"logical_capacity", std::to_string(result->logicalCapacity)},
      {"used_capacity", std::to_string(result->usedCapacity)},
      {"free_capacity",
       std::to_string(result->logicalCapacity > result->usedCapacity ? result->logicalCapacity - result->usedCapacity
                                                                     : 0)},
      {"manager.queued", std::to_string(managerResult->queued)},
      {"manager.loading", std::to_string(managerResult->loading)},
      {"manager.ready", std::to_string(managerResult->ready)},
      {"manager.cleaning", std::to_string(managerResult->cleaning)}};
  for (const auto &count : result->stateCounts) {
    table.push_back({fmt::format("state.{}", magic_enum::enum_name(count.state)), std::to_string(count.count)});
  }
  co_return table;
}

}  // namespace

CoTryTask<void> registerCacheStatusHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
