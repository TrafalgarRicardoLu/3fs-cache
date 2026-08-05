#include "cache_manager/planner/NamespaceDatasetPlanner.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "common/serde/Serde.h"

namespace hf3fs::cache_manager {
namespace {

constexpr uint32_t kNamespaceDatasetCursorVersion = 1;

struct DirectoryCursor {
  SERDE_STRUCT_FIELD(inode, meta::InodeId{});
  SERDE_STRUCT_FIELD(after, std::string{});
};

struct NamespaceDatasetCursor {
  SERDE_STRUCT_FIELD(version, kNamespaceDatasetCursorVersion);
  SERDE_STRUCT_FIELD(sourceIndex, uint32_t{});
  SERDE_STRUCT_FIELD(sourceType, cache::DatasetSourceType::NAMESPACE_PATH);
  SERDE_STRUCT_FIELD(nextRoot, uint32_t{});
  SERDE_STRUCT_FIELD(directories, std::vector<DirectoryCursor>{});
  SERDE_STRUCT_FIELD(file, std::optional<meta::Inode>{});
  SERDE_STRUCT_FIELD(nextBlock, uint64_t{});
};

std::vector<std::string_view> roots(const cache::DatasetSource &source) {
  if (source.type() == cache::DatasetSourceType::NAMESPACE_PATH) {
    return {std::get<cache::NamespacePathSource>(source.source).path};
  }
  std::vector<std::string_view> result;
  const auto &paths = std::get<cache::PathListSource>(source.source).paths;
  result.reserve(paths.size());
  for (const auto &path : paths) result.push_back(path);
  return result;
}

Result<Void> validateCursor(const NamespaceDatasetCursor &cursor,
                            const cache::DatasetSource &source,
                            const PlannerContext &context) {
  auto sourceRoots = roots(source);
  if (cursor.version != kNamespaceDatasetCursorVersion || cursor.sourceIndex != context.sourceIndex ||
      cursor.sourceType != source.type() || cursor.nextRoot > sourceRoots.size()) {
    return makeError(StatusCode::kInvalidArg, "namespace dataset cursor does not match its source");
  }
  for (const auto &directory : cursor.directories) {
    if (directory.inode == meta::InodeId{} || directory.after.size() > cache::kMaxDatasetPathLength) {
      return makeError(StatusCode::kInvalidArg, "invalid directory cursor");
    }
  }
  if (cursor.file) {
    RETURN_ON_ERROR(validateOriginFileSnapshot(*cursor.file));
    auto blocks = originFileBlockCount(*cursor.file);
    RETURN_ON_ERROR(blocks);
    if (cursor.nextBlock == 0 || cursor.nextBlock >= *blocks) {
      return makeError(StatusCode::kInvalidArg, "invalid namespace file position");
    }
  } else if (cursor.nextBlock != 0) {
    return makeError(StatusCode::kInvalidArg, "namespace cursor has a block without a file");
  }
  return Void{};
}

Result<std::string> encodeCursor(const NamespaceDatasetCursor &cursor) {
  auto encoded = serde::serialize(cursor);
  return std::string(encoded.data(), encoded.size());
}

Result<NamespaceDatasetCursor> decodeCursor(std::string_view encoded,
                                            const cache::DatasetSource &source,
                                            const PlannerContext &context) {
  NamespaceDatasetCursor cursor;
  RETURN_ON_ERROR(serde::deserialize(cursor, encoded));
  RETURN_ON_ERROR(validateCursor(cursor, source, context));
  return cursor;
}

Result<Void> acceptInode(NamespaceDatasetCursor &cursor, meta::Inode inode, bool recursive) {
  if (inode.isOriginFile()) {
    RETURN_ON_ERROR(validateOriginFileSnapshot(inode));
    if (inode.fileLength() != 0) cursor.file = std::move(inode);
    return Void{};
  }
  if (inode.isDirectory()) {
    if (!recursive) return makeError(MetaCode::kNotFile, "namespace source is a directory but recursion is disabled");
    cursor.directories.push_back({inode.id, {}});
    return Void{};
  }
  if (inode.isSymlink()) return Void{};
  return makeError(MetaCode::kNotFile, "namespace dataset contains a non-OriginFile");
}

}  // namespace

NamespaceDatasetPlanner::NamespaceDatasetPlanner(std::shared_ptr<NamespaceFileResolver> resolver,
                                                 cache::DatasetSource source,
                                                 PlannerContext context,
                                                 uint32_t maxCursorBytes)
    : SourcePlanner(std::move(context), maxCursorBytes),
      resolver_(std::move(resolver)),
      source_(std::move(source)) {}

CoTryTask<PlannerPage> NamespaceDatasetPlanner::plan(std::string_view encodedCursor) {
  CO_RETURN_ON_ERROR(source_.valid());
  if (source_.type() != cache::DatasetSourceType::NAMESPACE_PATH &&
      source_.type() != cache::DatasetSourceType::PATH_LIST) {
    co_return makeError(StatusCode::kInvalidArg, "namespace dataset planner received an unsupported source type");
  }
  if (!resolver_) co_return makeError(StatusCode::kInvalidConfig, "namespace dataset resolver is not configured");

  NamespaceDatasetCursor cursor;
  cursor.sourceIndex = context().sourceIndex;
  cursor.sourceType = source_.type();
  if (!encodedCursor.empty()) {
    auto decoded = decodeCursor(encodedCursor, source_, context());
    CO_RETURN_ON_ERROR(decoded);
    cursor = std::move(*decoded);
  }

  const auto sourceRoots = roots(source_);
  const bool recursive = source_.type() == cache::DatasetSourceType::PATH_LIST ||
                         std::get<cache::NamespacePathSource>(source_.source).recursive;
  PlannerPage result;
  while (result.entries.size() < context().pageLimit) {
    if (cursor.file) {
      auto blocks = originFileBlockCount(*cursor.file);
      CO_RETURN_ON_ERROR(blocks);
      const auto end = std::min(*blocks, cursor.nextBlock + context().pageLimit - result.entries.size());
      const auto blockSize = uint64_t{cursor.file->fileLayout().chunkSize};
      for (auto block = cursor.nextBlock; block < end; ++block) {
        const auto offset = block * blockSize;
        result.entries.push_back({context().jobId,
                                  {cursor.file->id.u64(), cache::CacheBlockIndex{static_cast<uint32_t>(block)}},
                                  std::min(blockSize, cursor.file->fileLength() - offset),
                                  context().priority,
                                  cache::PrefetchPlanEntryState::PLANNED,
                                  Uuid::zero()});
      }
      cursor.nextBlock = end;
      if (cursor.nextBlock == *blocks) {
        cursor.file.reset();
        cursor.nextBlock = 0;
      }
      continue;
    }

    if (!cursor.directories.empty()) {
      auto &directory = cursor.directories.back();
      auto page = co_await resolver_->list(directory.inode, directory.after, 1);
      CO_RETURN_ON_ERROR(page);
      if (page->entries.size() > 1 || (page->entries.empty() && page->more)) {
        co_return makeError(CacheCode::kInvalidResponse, "namespace list page made no progress");
      }
      if (page->entries.empty()) {
        cursor.directories.pop_back();
        continue;
      }
      auto entry = std::move(page->entries.front());
      if (entry.name.empty() || (!directory.after.empty() && entry.name <= directory.after)) {
        co_return makeError(CacheCode::kInvalidResponse, "namespace list cursor did not advance");
      }
      directory.after = std::move(entry.name);
      CO_RETURN_ON_ERROR(acceptInode(cursor, std::move(entry.inode), true));
      continue;
    }

    if (cursor.nextRoot < sourceRoots.size()) {
      auto inode = co_await resolver_->stat(sourceRoots[cursor.nextRoot]);
      CO_RETURN_ON_ERROR(inode);
      ++cursor.nextRoot;
      CO_RETURN_ON_ERROR(acceptInode(cursor, std::move(*inode), recursive));
      continue;
    }
    break;
  }

  result.done = !cursor.file && cursor.directories.empty() && cursor.nextRoot == sourceRoots.size();
  if (!result.done) {
    auto encoded = encodeCursor(cursor);
    CO_RETURN_ON_ERROR(encoded);
    result.nextCursor = std::move(*encoded);
  }
  co_return result;
}

SourcePlannerFactory::Builder makeNamespaceDatasetPlannerBuilder(std::shared_ptr<NamespaceFileResolver> resolver,
                                                                 cache::DatasetSourceType type) {
  return [resolver = std::move(resolver), type](const cache::DatasetSource &source,
                                                PlannerContext context,
                                                uint32_t maxCursorBytes) -> Result<std::unique_ptr<SourcePlanner>> {
    if (source.type() != type ||
        (type != cache::DatasetSourceType::NAMESPACE_PATH && type != cache::DatasetSourceType::PATH_LIST)) {
      return makeError(StatusCode::kInvalidArg, "namespace dataset planner builder source type mismatch");
    }
    std::unique_ptr<SourcePlanner> planner =
        std::make_unique<NamespaceDatasetPlanner>(resolver, source, std::move(context), maxCursorBytes);
    return planner;
  };
}

}  // namespace hf3fs::cache_manager
