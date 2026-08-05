#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::cache_manager {

struct PlannerContext {
  cache::PrefetchJobId jobId;
  uint32_t sourceIndex{0};
  uint32_t priority{0};
  uint32_t pageLimit{0};
  CancellationToken cancellation;

  Result<Void> valid(uint32_t maxPageItems) const;
};

struct PlannerPage {
  std::vector<cache::PrefetchPlanEntry> entries;
  std::string nextCursor;
  bool done{false};

  Result<Void> valid(const PlannerContext &context, uint32_t maxCursorBytes) const;
};

class SourcePlanner {
 public:
  SourcePlanner(PlannerContext context, uint32_t maxCursorBytes);
  virtual ~SourcePlanner() = default;

  CoTryTask<PlannerPage> nextPage(std::string_view cursor);
  const PlannerContext &context() const { return context_; }

 protected:
  virtual CoTryTask<PlannerPage> plan(std::string_view cursor) = 0;

 private:
  PlannerContext context_;
  uint32_t maxCursorBytes_;
};

struct SourcePlannerFactoryConfig {
  uint32_t maxPageItems{cache::kMaxPhase2BatchItems};
  uint32_t maxCursorBytes{cache::kMaxDatasetPathLength};

  Result<Void> valid() const;
};

class SourcePlannerFactory {
 public:
  using Builder = std::function<
      Result<std::unique_ptr<SourcePlanner>>(const cache::DatasetSource &, PlannerContext, uint32_t maxCursorBytes)>;
  using Builders = std::map<cache::DatasetSourceType, Builder>;

  static Result<std::unique_ptr<SourcePlannerFactory>> create(SourcePlannerFactoryConfig config, Builders builders);

  Result<std::unique_ptr<SourcePlanner>> make(const cache::DatasetSource &source, PlannerContext context) const;

 private:
  SourcePlannerFactory(SourcePlannerFactoryConfig config, Builders builders)
      : config_(config),
        builders_(std::move(builders)) {}

  SourcePlannerFactoryConfig config_;
  Builders builders_;
};

}  // namespace hf3fs::cache_manager
