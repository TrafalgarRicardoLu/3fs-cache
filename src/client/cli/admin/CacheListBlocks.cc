#include "CacheListBlocks.h"

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/MagicEnum.hpp"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-list-blocks");
  parser.add_argument("--inode").required().scan<'u', uint64_t>();
  parser.add_argument("--begin").default_value(uint32_t{0}).scan<'u', uint32_t>();
  parser.add_argument("--limit").default_value(uint32_t{1000}).scan<'u', uint32_t>();
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  meta::ListCacheBlocksReq request;
  request.user = env.userInfo;
  request.inode = meta::InodeId{parser.get<uint64_t>("--inode")};
  request.beginBlock = cache::CacheBlockIndex{parser.get<uint32_t>("--begin")};
  request.limit = parser.get<uint32_t>("--limit");
  ENSURE_USAGE(request.limit > 0, "--limit must be greater than zero");
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto result = co_await env.metaClientGetter()->listCacheBlocks(std::move(request));
  CO_RETURN_ON_ERROR(result);

  Dispatcher::OutputTable table{{"Block", "State", "Generation", "Length", "Checksum"}};
  for (const auto &block : result->blocks) {
    table.push_back({std::to_string(block.key.block.toUnderType()),
                     std::string(magic_enum::enum_name(block.state)),
                     block.ready ? std::to_string(block.ready->cacheGeneration.toUnderType()) : "",
                     block.ready ? std::to_string(block.ready->blockLength) : "",
                     block.ready ? fmt::format("{}:{}", block.ready->checksumType, block.ready->checksumValue) : ""});
  }
  if (result->more && !result->blocks.empty()) {
    table.push_back({"NEXT", std::to_string(result->blocks.back().key.block.toUnderType() + 1), "", "", ""});
  }
  co_return table;
}

}  // namespace

CoTryTask<void> registerCacheListBlocksHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
