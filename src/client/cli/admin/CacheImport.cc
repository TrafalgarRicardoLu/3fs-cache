#include "CacheImport.h"

#include <iterator>
#include <set>

#include "AdminEnv.h"
#include "CacheOriginCli.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/MagicEnum.hpp"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("cache-import");
  parser.add_argument("items").remaining();
  parser.add_argument("--table-id").required().scan<'u', uint32_t>();
  parser.add_argument("--block-size").required().scan<'u', uint32_t>();
  parser.add_argument("--stripe-size").default_value(uint32_t{1}).scan<'u', uint32_t>();
  parser.add_argument("--permission");
  cache_admin::addOriginArguments(parser);
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handle(IEnv &ienv,
                                          const argparse::ArgumentParser &parser,
                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  auto items = parser.get<std::vector<std::string>>("items");
  ENSURE_USAGE(!items.empty() && items.size() % 2 == 0,
               "items must be PATH OBJECT_KEY pairs; for example: cache-import /data/a objects/a ...");
  ENSURE_USAGE(items.size() / 2 <= meta::kMaxCacheBatchItems, "too many import items");

  auto origin = cache_admin::parseOriginOptions(parser);
  CO_RETURN_ON_ERROR(origin);
  auto permission = cache_admin::parsePermission(parser, meta::Permission{0444});
  CO_RETURN_ON_ERROR(permission);
  auto store = cache_admin::makeObjectStore(*origin);
  CO_RETURN_ON_ERROR(store);

  meta::BatchImportOriginFilesReq request;
  request.user = env.userInfo;
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  std::vector<size_t> requestRows;
  std::vector<Dispatcher::OutputRow> rows;
  std::set<std::string> paths;
  for (size_t i = 0; i < items.size(); i += 2) {
    const auto &path = items[i];
    const auto &key = items[i + 1];
    auto row = rows.size();
    rows.push_back({path, key, "", "", ""});
    if (!paths.emplace(path).second) {
      rows[row][2] = "FAILED";
      rows[row][4] = "duplicate path in request";
      continue;
    }
    auto object = co_await cache_admin::headObject(*origin, **store, key);
    if (object.hasError()) {
      rows[row][2] = "FAILED";
      rows[row][4] = object.error().describe();
      continue;
    }
    meta::OriginFileMetadata metadata;
    metadata.object = object->identity;
    metadata.objectSize = object->size;
    metadata.tableId = flat::ChainTableId{parser.get<uint32_t>("--table-id")};
    metadata.blockSize = parser.get<uint32_t>("--block-size");
    metadata.stripeSize = parser.get<uint32_t>("--stripe-size");
    metadata.permission = *permission;
    auto valid = metadata.valid();
    if (valid.hasError()) {
      rows[row][2] = "FAILED";
      rows[row][4] = valid.error().describe();
      continue;
    }
    request.entries.push_back({meta::PathAt{env.currentDirId, Path{path}}, std::move(metadata)});
    requestRows.push_back(row);
  }

  if (!request.entries.empty()) {
    auto result = co_await env.metaClientGetter()->batchImportOriginFiles(std::move(request));
    CO_RETURN_ON_ERROR(result);
    if (result->results.size() != requestRows.size()) {
      co_return makeError(CacheCode::kInvalidResponse, "batch import response size mismatch");
    }
    for (size_t i = 0; i < result->results.size(); ++i) {
      const auto &item = result->results[i];
      auto &row = rows[requestRows[i]];
      if (item.hasError()) {
        row[2] = "FAILED";
        row[4] = item.error().describe();
      } else {
        row[1] = item->inode.asOriginFile().object.key;
        row[2] = std::string(magic_enum::enum_name(item->outcome));
        row[3] = item->inode.id.toHexString();
        row[4] = fmt::format("{} bytes", item->inode.fileLength());
      }
    }
  }

  Dispatcher::OutputTable table{{"Path", "Object", "Status", "Inode", "Detail"}};
  table.insert(table.end(), std::make_move_iterator(rows.begin()), std::make_move_iterator(rows.end()));
  co_return table;
}

}  // namespace

CoTryTask<void> registerCacheImportHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handle);
}

}  // namespace hf3fs::client::cli
