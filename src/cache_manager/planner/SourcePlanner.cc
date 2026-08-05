#include "cache_manager/planner/SourcePlanner.h"

#include <array>
#include <utility>

namespace hf3fs::cache_manager {
namespace {

constexpr std::array kSourceTypes{cache::DatasetSourceType::NAMESPACE_PATH,
                                  cache::DatasetSourceType::PATH_LIST,
                                  cache::DatasetSourceType::MANIFEST_PATH,
                                  cache::DatasetSourceType::S3_PREFIX};

bool supported(cache::DatasetSourceType type) {
  for (auto candidate : kSourceTypes) {
    if (candidate == type) return true;
  }
  return false;
}

void checkCancellation(const CancellationToken &token) {
  if (token.isCancellationRequested()) throw OperationCancelled();
}

}  // namespace

Result<Void> PlannerContext::valid(uint32_t maxPageItems) const {
  if (jobId == cache::PrefetchJobId{}) return makeError(StatusCode::kInvalidArg, "planner job id not set");
  if (pageLimit == 0 || pageLimit > maxPageItems) {
    return makeError(StatusCode::kInvalidArg, "planner page limit is invalid");
  }
  return Void{};
}

Result<Void> PlannerPage::valid(const PlannerContext &context, uint32_t maxCursorBytes) const {
  if (entries.size() > context.pageLimit) {
    return makeError(CacheCode::kRequestTooLarge, "planner page exceeds item limit");
  }
  if (nextCursor.size() > maxCursorBytes) {
    return makeError(CacheCode::kRequestTooLarge, "planner cursor exceeds byte limit");
  }
  if (done && !nextCursor.empty()) return makeError(StatusCode::kInvalidArg, "completed planner page has a cursor");
  if (!done && nextCursor.empty()) return makeError(StatusCode::kInvalidArg, "incomplete planner page has no cursor");
  if (!done && entries.empty()) return makeError(StatusCode::kInvalidArg, "incomplete planner page is empty");
  for (const auto &entry : entries) {
    RETURN_ON_ERROR(entry.valid());
    if (entry.jobId != context.jobId || entry.priority != context.priority ||
        entry.state != cache::PrefetchPlanEntryState::PLANNED || entry.admissionAttemptId != Uuid::zero()) {
      return makeError(StatusCode::kInvalidArg, "planner entry violates frozen output contract");
    }
  }
  return Void{};
}

SourcePlanner::SourcePlanner(PlannerContext context, uint32_t maxCursorBytes)
    : context_(std::move(context)),
      maxCursorBytes_(maxCursorBytes) {}

CoTryTask<PlannerPage> SourcePlanner::nextPage(std::string_view cursor) {
  checkCancellation(context_.cancellation);
  if (cursor.size() > maxCursorBytes_) {
    co_return makeError(CacheCode::kRequestTooLarge, "planner resume cursor exceeds byte limit");
  }
  auto page = co_await plan(cursor);
  CO_RETURN_ON_ERROR(page);
  checkCancellation(context_.cancellation);
  CO_RETURN_ON_ERROR(page->valid(context_, maxCursorBytes_));
  co_return std::move(*page);
}

Result<Void> SourcePlannerFactoryConfig::valid() const {
  if (maxPageItems == 0 || maxPageItems > cache::kMaxPhase2BatchItems || maxCursorBytes == 0 ||
      maxCursorBytes > cache::kMaxDatasetPathLength) {
    return makeError(StatusCode::kInvalidConfig, "invalid source planner factory limits");
  }
  return Void{};
}

Result<std::unique_ptr<SourcePlannerFactory>> SourcePlannerFactory::create(SourcePlannerFactoryConfig config,
                                                                           Builders builders) {
  RETURN_ON_ERROR(config.valid());
  if (builders.size() != kSourceTypes.size()) {
    return makeError(StatusCode::kInvalidConfig, "source planner builders are incomplete");
  }
  for (const auto &[type, builder] : builders) {
    if (!supported(type) || !builder) {
      return makeError(StatusCode::kInvalidConfig, "unknown or empty source planner builder");
    }
  }
  return std::unique_ptr<SourcePlannerFactory>(new SourcePlannerFactory(config, std::move(builders)));
}

Result<std::unique_ptr<SourcePlanner>> SourcePlannerFactory::make(const cache::DatasetSource &source,
                                                                  PlannerContext context) const {
  RETURN_ON_ERROR(source.valid());
  RETURN_ON_ERROR(context.valid(config_.maxPageItems));
  auto type = source.type();
  auto builder = builders_.find(type);
  if (!supported(type) || builder == builders_.end()) {
    return makeError(StatusCode::kInvalidConfig, "source planner type is not registered");
  }
  auto planner = builder->second(source, std::move(context), config_.maxCursorBytes);
  RETURN_ON_ERROR(planner);
  if (!*planner) return makeError(StatusCode::kInvalidConfig, "source planner builder returned null");
  return planner;
}

}  // namespace hf3fs::cache_manager
