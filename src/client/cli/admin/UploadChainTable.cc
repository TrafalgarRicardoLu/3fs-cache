#include "UploadChainTable.h"

#include <folly/Conv.h>

#include "AdminEnv.h"
#include "client/cli/common/Dispatcher.h"
#include "client/cli/common/Utils.h"
#include "common/utils/RapidCsv.h"

namespace hf3fs::client::cli {
namespace {

auto getParser() {
  argparse::ArgumentParser parser("upload-chain-table");
  parser.add_argument("-d", "--dump-template").default_value(false).implicit_value(true);
  parser.add_argument("tableId").scan<'u', uint32_t>();
  parser.add_argument("csv-file-path");
  parser.add_argument("--desc");
  parser.add_argument("--role").default_value(std::string("USER_DATA"));
  parser.add_argument("--logical-capacity").default_value(uint64_t{0}).scan<'u', uint64_t>();
  parser.add_argument("--checksum").default_value(std::string("NONE"));
  return parser;
}

CoTryTask<Dispatcher::OutputTable> handleUploadChainTable(IEnv &ienv,
                                                          const argparse::ArgumentParser &parser,
                                                          const Dispatcher::Args &args) {
  auto &env = dynamic_cast<AdminEnv &>(ienv);
  ENSURE_USAGE(args.empty());
  Dispatcher::OutputTable table;

  auto tableId = flat::ChainTableId(parser.get<uint32_t>("tableId"));
  auto csvFilePath = parser.get<std::string>("csv-file-path");
  auto desc = parser.present<String>("--desc").value_or("");
  auto roleName = parser.get<std::string>("--role");
  if (roleName != "USER_DATA" && roleName != "CACHE_DATA" && roleName != "WRITE_STAGING") {
    co_return makeError(StatusCode::kInvalidArg, "role must be USER_DATA, CACHE_DATA, or WRITE_STAGING");
  }
  auto role = roleName == "CACHE_DATA"      ? flat::ChainTableRole::CACHE_DATA
              : roleName == "WRITE_STAGING" ? flat::ChainTableRole::WRITE_STAGING
                                            : flat::ChainTableRole::USER_DATA;
  auto logicalCapacity = parser.get<uint64_t>("--logical-capacity");
  auto checksumName = parser.get<std::string>("--checksum");
  if (checksumName != "NONE" && checksumName != "CRC32C" && checksumName != "CRC32") {
    co_return makeError(StatusCode::kInvalidArg, "checksum must be NONE, CRC32C, or CRC32");
  }
  auto checksumType = checksumName == "CRC32C"  ? flat::ChainTableChecksumType::CRC32C
                      : checksumName == "CRC32" ? flat::ChainTableChecksumType::CRC32
                                                : flat::ChainTableChecksumType::NONE;

  if (parser.get<bool>("-d")) {
    std::ofstream of(csvFilePath);
    of.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    of << "ChainId\n";
    of << "123\n";
    of << "234\n";
    table.push_back({fmt::format("Dump template to {} succeeded", csvFilePath)});
    co_return table;
  }

  rapidcsv::Document doc(csvFilePath);
  auto columnNames = doc.GetColumnNames();

  if (columnNames.size() != 1) {
    co_return makeError(StatusCode::kInvalidFormat, "expected columns: ChainId");
  }

  if (columnNames[0] != "ChainId") {
    co_return makeError(StatusCode::kInvalidFormat,
                        fmt::format("column[0] expected:'ChainId' actual:'{}'", columnNames[0]));
  }

  if (doc.GetRowCount() == 0) {
    co_return makeError(StatusCode::kInvalidFormat, "empty rows");
  }

  std::vector<flat::ChainId> chainIds;
  for (size_t i = 0; i < doc.GetRowCount(); ++i) {
    auto row = doc.GetRow<int64_t>(i);
    if (row.size() != columnNames.size()) {
      co_return makeError(
          StatusCode::kInvalidFormat,
          fmt::format("unexpected size of row[{}]. expected:{} actual:{}", i, columnNames.size(), row.size()));
    }

    if (row[0] <= 0) {
      co_return makeError(StatusCode::kInvalidFormat, fmt::format("ChainId should be positive: {}", row[0]));
    }
    if (row[0] > static_cast<int64_t>(std::numeric_limits<flat::ChainId::UnderlyingType>::max())) {
      co_return makeError(StatusCode::kInvalidFormat,
                          fmt::format("ChainId overflow. max: {}. now: {}",
                                      std::numeric_limits<flat::ChainId::UnderlyingType>::max(),
                                      row[0]));
    }
    chainIds.emplace_back(row[0]);
  }

  auto rsp = co_await env.mgmtdClientGetter()
                 ->setChainTable(env.userInfo, tableId, chainIds, desc, role, logicalCapacity, checksumType);
  CO_RETURN_ON_ERROR(rsp);

  table.push_back({fmt::format("Upload {} of {} succeeded", tableId, rsp->chainTableVersion)});
  co_return table;
}
}  // namespace
CoTryTask<void> registerUploadChainTableHandler(Dispatcher &dispatcher) {
  co_return co_await dispatcher.registerHandler(getParser, handleUploadChainTable);
}
}  // namespace hf3fs::client::cli
