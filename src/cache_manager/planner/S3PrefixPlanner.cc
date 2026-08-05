#include "cache_manager/planner/S3PrefixPlanner.h"

#include <boost/uuid/name_generator_sha1.hpp>
#include <utility>

#include "common/serde/Serde.h"

namespace hf3fs::cache_manager {
namespace {

constexpr uint32_t kS3PrefixCursorVersion = 1;
constexpr std::string_view kResumedObjectPath = "/";

struct S3PrefixCursor {
  SERDE_STRUCT_FIELD(version, kS3PrefixCursorVersion);
  SERDE_STRUCT_FIELD(sourceIndex, uint32_t{});
  SERDE_STRUCT_FIELD(continuation, std::string{});
  SERDE_STRUCT_FIELD(listingDone, false);
  SERDE_STRUCT_FIELD(objectsSeen, uint64_t{});
  SERDE_STRUCT_FIELD(childCursor, std::string{});
};

Uuid refreshRequestId(std::string_view path, meta::InodeId inode, const cache::ImmutableObjectIdentity &object) {
  const auto namespaceId = Uuid::from(0x4846334653434143ULL, 0x5052465852454631ULL);
  boost::uuids::name_generator_sha1 generator(namespaceId);
  auto identity = std::string(path) + serde::serialize(inode) + serde::serialize(object);
  return Uuid{generator(identity)};
}

Result<std::string> destinationPath(const cache::S3PrefixSource &source, std::string_view key) {
  if (!key.starts_with(source.prefix)) {
    return makeError(CacheCode::kInvalidResponse, "listed object escaped the requested prefix");
  }
  auto relative = key.substr(source.prefix.size());
  if (!relative.empty() && relative.front() == '/') relative.remove_prefix(1);
  if (relative.empty() || relative.back() == '/') return std::string{};
  size_t begin = 0;
  while (begin < relative.size()) {
    auto end = relative.find('/', begin);
    if (end == std::string_view::npos) end = relative.size();
    auto component = relative.substr(begin, end - begin);
    if (component.empty() || component == "." || component == ".." || component.find('\0') != std::string_view::npos) {
      return makeError(StatusCode::kInvalidArg, "object key cannot be mapped safely into the namespace");
    }
    begin = end + 1;
  }
  std::string result = source.destinationRoot;
  if (result != "/") result.push_back('/');
  result.append(relative);
  cache::NamespacePathSource destination{result, false};
  RETURN_ON_ERROR(destination.valid());
  return result;
}

Result<std::string> encodeCursor(const S3PrefixCursor &cursor) {
  auto encoded = serde::serialize(cursor);
  return std::string(encoded.data(), encoded.size());
}

Result<S3PrefixCursor> decodeCursor(std::string_view encoded,
                                    const PlannerContext &context,
                                    const S3PrefixPlannerConfig &config) {
  S3PrefixCursor cursor;
  RETURN_ON_ERROR(serde::deserialize(cursor, encoded));
  if (cursor.version != kS3PrefixCursorVersion || cursor.sourceIndex != context.sourceIndex ||
      cursor.objectsSeen > config.maxObjects || (cursor.listingDone && !cursor.continuation.empty())) {
    return makeError(StatusCode::kInvalidArg, "invalid S3 prefix planner cursor");
  }
  return cursor;
}

class ImportedInodeResolver final : public NamespaceFileResolver {
 public:
  explicit ImportedInodeResolver(meta::Inode inode)
      : inode_(std::move(inode)) {}

  CoTryTask<meta::Inode> stat(std::string_view) override { co_return inode_; }

 private:
  meta::Inode inode_;
};

}  // namespace

Result<Void> PrefixImportLayout::valid() const {
  meta::OriginFileMetadata metadata;
  metadata.object = {cache::OriginId{1}, "bucket", "key", {cache::VersionSelectorType::VERSION_ID, "version"}};
  metadata.tableId = tableId;
  metadata.blockSize = blockSize;
  metadata.stripeSize = stripeSize;
  metadata.permission = permission;
  return metadata.valid();
}

Result<Void> S3PrefixPlannerConfig::valid() const {
  RETURN_ON_ERROR(layout.valid());
  if (maxObjects == 0) return makeError(StatusCode::kInvalidConfig, "S3 prefix object limit is zero");
  return Void{};
}

CoTryTask<meta::Inode> MetaOriginFileImporter::import(std::string_view path,
                                                      const cache::origin::ObjectMetadata &object,
                                                      const PrefixImportLayout &layout) {
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "prefix importer MetaClient is not configured");
  meta::OriginFileMetadata metadata;
  metadata.object = object.identity;
  metadata.objectSize = object.size;
  metadata.tableId = layout.tableId;
  metadata.blockSize = layout.blockSize;
  metadata.stripeSize = layout.stripeSize;
  metadata.permission = layout.permission;
  CO_RETURN_ON_ERROR(metadata.valid());

  meta::ImportOriginFileReq request;
  request.user = user_;
  request.entry = {meta::PathAt{meta::InodeId::root(), Path{std::string(path)}}, metadata};
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto imported = co_await metaClient_->importOriginFile(request);
  if (!imported.hasError()) co_return std::move(imported->inode);
  if (imported.error().code() != MetaCode::kExists) co_return makeError(imported.error());

  auto existing = co_await metaClient_->stat(user_, meta::InodeId::root(), Path{std::string(path)}, false);
  CO_RETURN_ON_ERROR(existing);
  if (!existing->isOriginFile()) co_return makeError(MetaCode::kExists, "prefix destination is not an OriginFile");
  if (existing->asOriginFile().object == object.identity && existing->fileLength() == object.size) {
    co_return std::move(*existing);
  }
  meta::RefreshOriginFileReq refresh;
  refresh.user = user_;
  refresh.requestId = refreshRequestId(path, existing->id, object.identity);
  refresh.path = {meta::InodeId::root(), Path{std::string(path)}};
  refresh.expectedInode = existing->id;
  refresh.oldObject = existing->asOriginFile().object;
  refresh.newMetadata = std::move(metadata);
  refresh.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto refreshed = co_await metaClient_->refreshOriginFile(std::move(refresh));
  CO_RETURN_ON_ERROR(refreshed);
  co_return std::move(refreshed->newInode);
}

S3PrefixPlanner::S3PrefixPlanner(std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                 std::shared_ptr<OriginFileImporter> importer,
                                 cache::S3PrefixSource source,
                                 S3PrefixPlannerConfig config,
                                 PlannerContext context,
                                 uint32_t maxCursorBytes)
    : SourcePlanner(std::move(context), maxCursorBytes),
      objectStore_(std::move(objectStore)),
      importer_(std::move(importer)),
      source_(std::move(source)),
      config_(config) {}

CoTryTask<PlannerPage> S3PrefixPlanner::plan(std::string_view encodedCursor) {
  CO_RETURN_ON_ERROR(source_.valid());
  CO_RETURN_ON_ERROR(config_.valid());
  if (!objectStore_ || !importer_) {
    co_return makeError(StatusCode::kInvalidConfig, "S3 prefix planner dependencies are not configured");
  }

  S3PrefixCursor cursor;
  cursor.sourceIndex = context().sourceIndex;
  if (!encodedCursor.empty()) {
    auto decoded = decodeCursor(encodedCursor, context(), config_);
    CO_RETURN_ON_ERROR(decoded);
    cursor = std::move(*decoded);
  }

  while (true) {
    std::shared_ptr<NamespaceFileResolver> resolver;
    std::string childPath{kResumedObjectPath};
    if (cursor.childCursor.empty()) {
      if (cursor.listingDone) co_return PlannerPage{{}, {}, true};
      auto listed = co_await objectStore_->listObjects(
          {source_.originId, source_.bucket, source_.prefix, cursor.continuation, 1});
      CO_RETURN_ON_ERROR(listed);
      if (listed->objects.size() > 1 ||
          (!listed->done && (listed->nextContinuation.empty() || listed->nextContinuation == cursor.continuation))) {
        co_return makeError(CacheCode::kInvalidResponse, "invalid S3 prefix list page");
      }
      cursor.continuation = listed->done ? std::string{} : listed->nextContinuation;
      cursor.listingDone = listed->done;
      if (listed->objects.empty()) continue;
      ++cursor.objectsSeen;
      if (cursor.objectsSeen > config_.maxObjects) {
        co_return makeError(CacheCode::kRequestTooLarge, "S3 prefix exceeds object limit");
      }
      auto path = destinationPath(source_, listed->objects.front().identity.key);
      CO_RETURN_ON_ERROR(path);
      if (path->empty()) continue;
      auto imported = co_await importer_->import(*path, listed->objects.front(), config_.layout);
      CO_RETURN_ON_ERROR(imported);
      childPath = std::move(*path);
      resolver = std::make_shared<ImportedInodeResolver>(std::move(*imported));
    } else {
      resolver = std::make_shared<ImportedInodeResolver>(meta::Inode{});
    }

    NamespaceFilePlanner child{resolver,
                               cache::NamespacePathSource{std::move(childPath), false},
                               context(),
                               cache::kMaxDatasetPathLength};
    auto page = co_await child.nextPage(cursor.childCursor);
    CO_RETURN_ON_ERROR(page);
    cursor.childCursor = page->done ? std::string{} : std::move(page->nextCursor);
    if (page->entries.empty()) continue;
    PlannerPage result;
    result.entries = std::move(page->entries);
    result.done = cursor.childCursor.empty() && cursor.listingDone;
    if (!result.done) {
      auto encoded = encodeCursor(cursor);
      CO_RETURN_ON_ERROR(encoded);
      result.nextCursor = std::move(*encoded);
    }
    co_return result;
  }
}

SourcePlannerFactory::Builder makeS3PrefixPlannerBuilder(std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                                         std::shared_ptr<OriginFileImporter> importer,
                                                         S3PrefixPlannerConfig config) {
  return [objectStore = std::move(objectStore), importer = std::move(importer), config](
             const cache::DatasetSource &source,
             PlannerContext context,
             uint32_t maxCursorBytes) -> Result<std::unique_ptr<SourcePlanner>> {
    if (source.type() != cache::DatasetSourceType::S3_PREFIX) {
      return makeError(StatusCode::kInvalidArg, "S3 prefix planner received a different source type");
    }
    std::unique_ptr<SourcePlanner> planner =
        std::make_unique<S3PrefixPlanner>(objectStore,
                                          importer,
                                          std::get<cache::S3PrefixSource>(source.source),
                                          config,
                                          std::move(context),
                                          maxCursorBytes);
    return planner;
  };
}

}  // namespace hf3fs::cache_manager
