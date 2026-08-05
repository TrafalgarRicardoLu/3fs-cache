#include "cache_manager/planner/NamespaceFilePlanner.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/serde/Serde.h"

namespace hf3fs::cache_manager {
namespace {

constexpr uint32_t kNamespaceFileCursorVersion = 1;

struct NamespaceFileCursor {
  SERDE_STRUCT_FIELD(version, kNamespaceFileCursorVersion);
  SERDE_STRUCT_FIELD(sourceIndex, uint32_t{});
  SERDE_STRUCT_FIELD(inode, meta::Inode{});
  SERDE_STRUCT_FIELD(nextBlock, uint64_t{});
};

Result<uint64_t> blockCount(const meta::Inode &inode) {
  const auto blockSize = uint64_t{inode.fileLayout().chunkSize};
  const auto count = inode.fileLength() / blockSize + (inode.fileLength() % blockSize != 0);
  if (count > uint64_t{std::numeric_limits<uint32_t>::max()} + 1) {
    return makeError(CacheCode::kRequestTooLarge, "OriginFile expands to too many cache blocks");
  }
  return count;
}

Result<Void> validateSnapshot(const meta::Inode &inode) {
  if (!inode.isOriginFile()) return makeError(MetaCode::kNotFile, "namespace source is not an OriginFile");
  const auto &origin = inode.asOriginFile();
  if (origin.superseded || origin.cacheAdmissionDisabled) {
    return makeError(CacheCode::kStateConflict, "namespace source OriginFile is superseded");
  }
  RETURN_ON_ERROR(origin.object.valid());
  if (!inode.fileLayout().empty()) {
    return makeError(MetaCode::kInvalidFileLayout, "OriginFile cache layout is not empty");
  }
  RETURN_ON_ERROR(inode.fileLayout().valid(true));
  RETURN_ON_ERROR(blockCount(inode));
  return Void{};
}

Result<std::string> encodeCursor(const NamespaceFileCursor &cursor) {
  auto encoded = serde::serialize(cursor);
  return std::string(encoded.data(), encoded.size());
}

Result<NamespaceFileCursor> decodeCursor(std::string_view encoded, const PlannerContext &context) {
  NamespaceFileCursor cursor;
  RETURN_ON_ERROR(serde::deserialize(cursor, encoded));
  if (cursor.version != kNamespaceFileCursorVersion || cursor.sourceIndex != context.sourceIndex) {
    return makeError(StatusCode::kInvalidArg, "namespace file cursor does not match its source");
  }
  RETURN_ON_ERROR(validateSnapshot(cursor.inode));
  auto blocks = blockCount(cursor.inode);
  RETURN_ON_ERROR(blocks);
  if (cursor.nextBlock == 0 || cursor.nextBlock >= *blocks) {
    return makeError(StatusCode::kInvalidArg, "namespace file cursor block is invalid");
  }
  return cursor;
}

}  // namespace

CoTryTask<meta::Inode> MetaNamespaceFileResolver::stat(std::string_view path) {
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "namespace planner MetaClient is not configured");
  co_return co_await metaClient_->stat(user_, meta::InodeId::root(), Path{std::string(path)}, true);
}

NamespaceFilePlanner::NamespaceFilePlanner(std::shared_ptr<NamespaceFileResolver> resolver,
                                           cache::NamespacePathSource source,
                                           PlannerContext context,
                                           uint32_t maxCursorBytes)
    : SourcePlanner(std::move(context), maxCursorBytes),
      resolver_(std::move(resolver)),
      source_(std::move(source)) {}

CoTryTask<PlannerPage> NamespaceFilePlanner::plan(std::string_view encodedCursor) {
  CO_RETURN_ON_ERROR(source_.valid());
  meta::Inode inode;
  uint64_t beginBlock = 0;
  if (encodedCursor.empty()) {
    if (!resolver_) co_return makeError(StatusCode::kInvalidConfig, "namespace file resolver is not configured");
    auto resolved = co_await resolver_->stat(source_.path);
    CO_RETURN_ON_ERROR(resolved);
    inode = std::move(*resolved);
    CO_RETURN_ON_ERROR(validateSnapshot(inode));
  } else {
    auto cursor = decodeCursor(encodedCursor, context());
    CO_RETURN_ON_ERROR(cursor);
    inode = std::move(cursor->inode);
    beginBlock = cursor->nextBlock;
  }

  auto blocks = blockCount(inode);
  CO_RETURN_ON_ERROR(blocks);
  PlannerPage page;
  const auto endBlock = std::min(*blocks, beginBlock + context().pageLimit);
  page.entries.reserve(endBlock - beginBlock);
  const auto blockSize = uint64_t{inode.fileLayout().chunkSize};
  for (auto block = beginBlock; block < endBlock; ++block) {
    const auto offset = block * blockSize;
    page.entries.push_back({context().jobId,
                            {inode.id.u64(), cache::CacheBlockIndex{static_cast<uint32_t>(block)}},
                            std::min(blockSize, inode.fileLength() - offset),
                            context().priority,
                            cache::PrefetchPlanEntryState::PLANNED,
                            Uuid::zero()});
  }

  page.done = endBlock == *blocks;
  if (!page.done) {
    auto cursor = encodeCursor({kNamespaceFileCursorVersion, context().sourceIndex, inode, endBlock});
    CO_RETURN_ON_ERROR(cursor);
    page.nextCursor = std::move(*cursor);
  }
  co_return page;
}

SourcePlannerFactory::Builder makeNamespaceFilePlannerBuilder(std::shared_ptr<NamespaceFileResolver> resolver) {
  return [resolver = std::move(resolver)](const cache::DatasetSource &source,
                                          PlannerContext context,
                                          uint32_t maxCursorBytes) -> Result<std::unique_ptr<SourcePlanner>> {
    if (source.type() != cache::DatasetSourceType::NAMESPACE_PATH) {
      return makeError(StatusCode::kInvalidArg, "namespace file planner received a different source type");
    }
    auto value = std::get<cache::NamespacePathSource>(source.source);
    std::unique_ptr<SourcePlanner> planner =
        std::make_unique<NamespaceFilePlanner>(resolver, std::move(value), std::move(context), maxCursorBytes);
    return planner;
  };
}

}  // namespace hf3fs::cache_manager
