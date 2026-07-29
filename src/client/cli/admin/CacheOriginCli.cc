#include "CacheOriginCli.h"

#include <boost/uuid/name_generator_sha1.hpp>
#include <scn/tuple_return/tuple_return.h>
#include <sys/stat.h>

#include "cache/origin/s3/S3ObjectStore.h"
#include "common/serde/Serde.h"

namespace hf3fs::client::cli::cache_admin {

void addOriginArguments(argparse::ArgumentParser &parser) {
  parser.add_argument("--origin-id").required().scan<'u', uint32_t>();
  parser.add_argument("--bucket").required();
  parser.add_argument("--endpoint").default_value(std::string{});
  parser.add_argument("--region").default_value(std::string{"us-east-1"});
  parser.add_argument("--no-tls").default_value(false).implicit_value(true);
  parser.add_argument("--path-style").default_value(false).implicit_value(true);
  parser.add_argument("--origin-concurrency").default_value(uint32_t{16}).scan<'u', uint32_t>();
  parser.add_argument("--origin-inflight-bytes").default_value(uint64_t{256_MB}).scan<'u', uint64_t>();
}

Result<OriginOptions> parseOriginOptions(const argparse::ArgumentParser &parser) {
  OriginOptions options;
  options.originId = cache::OriginId{parser.get<uint32_t>("--origin-id")};
  options.bucket = parser.get<std::string>("--bucket");
  options.endpoint = parser.get<std::string>("--endpoint");
  options.region = parser.get<std::string>("--region");
  options.useTls = !parser.get<bool>("--no-tls");
  options.pathStyle = parser.get<bool>("--path-style");
  options.maxConcurrentRequests = parser.get<uint32_t>("--origin-concurrency");
  options.maxInflightBytes = parser.get<uint64_t>("--origin-inflight-bytes");
  if (options.originId == cache::OriginId{} || options.bucket.empty() || options.maxConcurrentRequests == 0 ||
      options.maxInflightBytes == 0) {
    return makeError(StatusCode::kInvalidArg, "invalid origin options");
  }
  return options;
}

Result<meta::Permission> parsePermission(const argparse::ArgumentParser &parser, meta::Permission fallback) {
  auto value = parser.present<std::string>("--permission");
  if (!value) return fallback;
  auto [result, permission] = scn::scan_tuple<uint32_t>(*value, "{:o}");
  if (!result) return makeError(StatusCode::kInvalidArg, "invalid permission: {}", result.error().msg());
  if ((permission & ~uint32_t{ALLPERMS}) != 0) {
    return makeError(StatusCode::kInvalidArg, "permission contains unsupported bits");
  }
  return meta::Permission{permission};
}

Result<std::shared_ptr<cache::origin::ObjectStore>> makeObjectStore(const OriginOptions &options) {
  cache::origin::s3::S3ObjectStoreConfig storeConfig;
  storeConfig.ioThreads = options.maxConcurrentRequests;
  storeConfig.maxConcurrentRequests = options.maxConcurrentRequests;
  storeConfig.maxInflightBytes = options.maxInflightBytes;
  cache::origin::s3::AwsS3ClientConfig clientConfig;
  clientConfig.endpoint = options.endpoint;
  clientConfig.region = options.region;
  clientConfig.useTls = options.useTls;
  clientConfig.pathStyle = options.pathStyle;
  clientConfig.maxConnections = options.maxConcurrentRequests;
  auto store = cache::origin::s3::S3ObjectStore::createAws(storeConfig, clientConfig);
  RETURN_ON_ERROR(store);
  return std::static_pointer_cast<cache::origin::ObjectStore>(*store);
}

CoTryTask<cache::origin::ObjectMetadata> headObject(const OriginOptions &options,
                                                    cache::origin::ObjectStore &store,
                                                    std::string key) {
  cache::ObjectRef object{options.originId, options.bucket, std::move(key)};
  CO_RETURN_ON_ERROR(object.valid());
  co_return co_await store.head(object);
}

Uuid stableRefreshRequestId(const meta::PathAt &path,
                            meta::InodeId expectedInode,
                            const cache::ImmutableObjectIdentity &object) {
  const auto namespaceId = Uuid::from(0x4846334653434143ULL, 0x5245465245534831ULL);
  boost::uuids::name_generator_sha1 generator(namespaceId);
  auto identity = serde::serialize(path) + serde::serialize(expectedInode) + serde::serialize(object);
  return Uuid{generator(identity)};
}

}  // namespace hf3fs::client::cli::cache_admin
