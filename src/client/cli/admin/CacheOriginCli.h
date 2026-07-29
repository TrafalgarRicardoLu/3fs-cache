#pragma once

#include <memory>

#include "cache/origin/ObjectStore.h"
#include "common/utils/ArgParse.h"
#include "fbs/meta/Service.h"

namespace hf3fs::client::cli::cache_admin {

struct OriginOptions {
  cache::OriginId originId{};
  std::string bucket;
  std::string endpoint;
  std::string region{"us-east-1"};
  bool useTls{true};
  bool pathStyle{false};
  uint32_t maxConcurrentRequests{16};
  uint64_t maxInflightBytes{256_MB};
};

void addOriginArguments(argparse::ArgumentParser &parser);
Result<OriginOptions> parseOriginOptions(const argparse::ArgumentParser &parser);
Result<meta::Permission> parsePermission(const argparse::ArgumentParser &parser, meta::Permission fallback);
Result<std::shared_ptr<cache::origin::ObjectStore>> makeObjectStore(const OriginOptions &options);
CoTryTask<cache::origin::ObjectMetadata> headObject(const OriginOptions &options,
                                                    cache::origin::ObjectStore &store,
                                                    std::string key);
Uuid stableRefreshRequestId(const meta::PathAt &path,
                            meta::InodeId expectedInode,
                            const cache::ImmutableObjectIdentity &object);

}  // namespace hf3fs::client::cli::cache_admin
