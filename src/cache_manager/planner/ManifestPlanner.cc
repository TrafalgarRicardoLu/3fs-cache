#include "cache_manager/planner/ManifestPlanner.h"

#include <algorithm>
#include <utility>

#include "common/serde/Serde.h"
#include "common/utils/Utf8.h"

namespace hf3fs::cache_manager {
namespace {

constexpr uint32_t kManifestCursorVersion = 1;
constexpr std::string_view kResumedChildPath = "/";

struct ManifestCursor {
  SERDE_STRUCT_FIELD(version, kManifestCursorVersion);
  SERDE_STRUCT_FIELD(sourceIndex, uint32_t{});
  SERDE_STRUCT_FIELD(manifest, meta::Inode{});
  SERDE_STRUCT_FIELD(offset, uint64_t{});
  SERDE_STRUCT_FIELD(lines, uint32_t{});
  SERDE_STRUCT_FIELD(expandedBlocks, uint64_t{});
  SERDE_STRUCT_FIELD(childCursor, std::string{});
};

Result<std::string> encodeCursor(const ManifestCursor &cursor) {
  auto encoded = serde::serialize(cursor);
  return std::string(encoded.data(), encoded.size());
}

Result<Void> validateCursor(const ManifestCursor &cursor,
                            const PlannerContext &context,
                            const ManifestPlannerConfig &config) {
  if (cursor.version != kManifestCursorVersion || cursor.sourceIndex != context.sourceIndex) {
    return makeError(StatusCode::kInvalidArg, "manifest cursor does not match its source");
  }
  RETURN_ON_ERROR(validateOriginFileSnapshot(cursor.manifest));
  if (cursor.manifest.fileLength() > config.maxBytes || cursor.offset > cursor.manifest.fileLength() ||
      cursor.lines > config.maxLines || cursor.expandedBlocks > config.maxExpandedBlocks) {
    return makeError(StatusCode::kInvalidArg, "manifest cursor exceeds configured bounds");
  }
  return Void{};
}

Result<ManifestCursor> decodeCursor(std::string_view encoded,
                                    const PlannerContext &context,
                                    const ManifestPlannerConfig &config) {
  ManifestCursor cursor;
  RETURN_ON_ERROR(serde::deserialize(cursor, encoded));
  RETURN_ON_ERROR(validateCursor(cursor, context, config));
  return cursor;
}

Result<std::optional<std::string>> parseLine(std::string line, const ManifestPlannerConfig &config) {
  if (!line.empty() && line.back() == '\r') line.pop_back();
  if (line.size() > config.maxLineBytes) {
    return makeError(CacheCode::kRequestTooLarge, "manifest line exceeds byte limit");
  }
  if (line.find('\0') != std::string::npos) {
    return makeError(StatusCode::kInvalidArg, "manifest line contains NUL");
  }
  if (utf8nvalid(reinterpret_cast<const utf8_int8_t *>(line.data()), line.size()) != nullptr) {
    return makeError(StatusCode::kInvalidArg, "manifest line is not valid UTF-8");
  }
  if (line.empty() || line.front() == '#') return std::optional<std::string>{};
  cache::NamespacePathSource path{std::move(line), true};
  RETURN_ON_ERROR(path.valid());
  return std::optional<std::string>{std::move(path.path)};
}

}  // namespace

Result<Void> ManifestPlannerConfig::valid() const {
  if (maxBytes == 0 || maxLineBytes == 0 || maxLineBytes > cache::kMaxDatasetPathLength || maxLines == 0 ||
      maxExpandedBlocks == 0 || rangeBytes == 0 || rangeBytes > maxBytes) {
    return makeError(StatusCode::kInvalidConfig, "invalid manifest planner limits");
  }
  return Void{};
}

ManifestPlanner::ManifestPlanner(std::shared_ptr<NamespaceFileResolver> resolver,
                                 std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                 cache::ManifestPathSource source,
                                 ManifestPlannerConfig config,
                                 PlannerContext context,
                                 uint32_t maxCursorBytes)
    : SourcePlanner(std::move(context), maxCursorBytes),
      resolver_(std::move(resolver)),
      objectStore_(std::move(objectStore)),
      source_(std::move(source)),
      config_(config) {}

CoTryTask<PlannerPage> ManifestPlanner::plan(std::string_view encodedCursor) {
  CO_RETURN_ON_ERROR(source_.valid());
  CO_RETURN_ON_ERROR(config_.valid());
  if (!resolver_ || !objectStore_) {
    co_return makeError(StatusCode::kInvalidConfig, "manifest planner dependencies are not configured");
  }

  ManifestCursor cursor;
  cursor.sourceIndex = context().sourceIndex;
  if (encodedCursor.empty()) {
    auto inode = co_await resolver_->stat(source_.path);
    CO_RETURN_ON_ERROR(inode);
    CO_RETURN_ON_ERROR(validateOriginFileSnapshot(*inode));
    if (inode->fileLength() > config_.maxBytes) {
      co_return makeError(CacheCode::kRequestTooLarge, "manifest exceeds byte limit");
    }
    cursor.manifest = std::move(*inode);
  } else {
    auto decoded = decodeCursor(encodedCursor, context(), config_);
    CO_RETURN_ON_ERROR(decoded);
    cursor = std::move(*decoded);
  }

  while (true) {
    std::optional<std::string> path;
    if (cursor.childCursor.empty()) {
      std::string partial;
      while (!path && cursor.offset < cursor.manifest.fileLength()) {
        if (context().cancellation.isCancellationRequested()) throw OperationCancelled();
        const auto length = std::min<uint64_t>(config_.rangeBytes, cursor.manifest.fileLength() - cursor.offset);
        auto bytes = co_await objectStore_->getRange(cursor.manifest.asOriginFile().object, {cursor.offset, length});
        CO_RETURN_ON_ERROR(bytes);
        if (bytes->size() != length) {
          co_return makeError(CacheCode::kInvalidResponse, "manifest range response length mismatch");
        }
        size_t consumed = 0;
        for (; consumed < bytes->size(); ++consumed) {
          const char value = static_cast<char>((*bytes)[consumed]);
          if (value != '\n') {
            partial.push_back(value);
            if (partial.size() > config_.maxLineBytes + 1) {
              co_return makeError(CacheCode::kRequestTooLarge, "manifest line exceeds byte limit");
            }
            continue;
          }
          ++cursor.lines;
          if (cursor.lines > config_.maxLines) {
            co_return makeError(CacheCode::kRequestTooLarge, "manifest exceeds line limit");
          }
          auto parsed = parseLine(std::exchange(partial, {}), config_);
          CO_RETURN_ON_ERROR(parsed);
          if (*parsed) {
            path = std::move(**parsed);
            ++consumed;
            break;
          }
        }
        cursor.offset += consumed;
      }
      if (!path && cursor.offset == cursor.manifest.fileLength() && !partial.empty()) {
        ++cursor.lines;
        if (cursor.lines > config_.maxLines) {
          co_return makeError(CacheCode::kRequestTooLarge, "manifest exceeds line limit");
        }
        auto parsed = parseLine(std::move(partial), config_);
        CO_RETURN_ON_ERROR(parsed);
        path = std::move(*parsed);
      }
      if (!path) co_return PlannerPage{{}, {}, true};
    }

    cache::DatasetSource childSource{
        cache::NamespacePathSource{path ? std::move(*path) : std::string{kResumedChildPath}, true}};
    NamespaceDatasetPlanner child{resolver_, childSource, context(), cache::kMaxDatasetPathLength};
    auto childPage = co_await child.nextPage(cursor.childCursor);
    CO_RETURN_ON_ERROR(childPage);
    if (childPage->entries.size() > config_.maxExpandedBlocks - cursor.expandedBlocks) {
      co_return makeError(CacheCode::kRequestTooLarge, "manifest expansion exceeds block limit");
    }
    cursor.expandedBlocks += childPage->entries.size();
    cursor.childCursor = childPage->done ? std::string{} : std::move(childPage->nextCursor);

    if (!childPage->entries.empty()) {
      PlannerPage result;
      result.entries = std::move(childPage->entries);
      result.done = cursor.childCursor.empty() && cursor.offset == cursor.manifest.fileLength();
      if (!result.done) {
        auto encoded = encodeCursor(cursor);
        CO_RETURN_ON_ERROR(encoded);
        result.nextCursor = std::move(*encoded);
      }
      co_return result;
    }
  }
}

SourcePlannerFactory::Builder makeManifestPlannerBuilder(std::shared_ptr<NamespaceFileResolver> resolver,
                                                         std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                                         ManifestPlannerConfig config) {
  return [resolver = std::move(resolver), objectStore = std::move(objectStore), config](
             const cache::DatasetSource &source,
             PlannerContext context,
             uint32_t maxCursorBytes) -> Result<std::unique_ptr<SourcePlanner>> {
    if (source.type() != cache::DatasetSourceType::MANIFEST_PATH) {
      return makeError(StatusCode::kInvalidArg, "manifest planner received a different source type");
    }
    std::unique_ptr<SourcePlanner> planner =
        std::make_unique<ManifestPlanner>(resolver,
                                          objectStore,
                                          std::get<cache::ManifestPathSource>(source.source),
                                          config,
                                          std::move(context),
                                          maxCursorBytes);
    return planner;
  };
}

}  // namespace hf3fs::cache_manager
