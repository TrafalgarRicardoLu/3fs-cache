#pragma once

#include "cache_manager/planner/NamespaceFilePlanner.h"

namespace hf3fs::cache_manager {

class NamespaceDatasetPlanner final : public SourcePlanner {
 public:
  NamespaceDatasetPlanner(std::shared_ptr<NamespaceFileResolver> resolver,
                          cache::DatasetSource source,
                          PlannerContext context,
                          uint32_t maxCursorBytes);

 protected:
  CoTryTask<PlannerPage> plan(std::string_view cursor) override;

 private:
  std::shared_ptr<NamespaceFileResolver> resolver_;
  cache::DatasetSource source_;
};

SourcePlannerFactory::Builder makeNamespaceDatasetPlannerBuilder(std::shared_ptr<NamespaceFileResolver> resolver,
                                                                 cache::DatasetSourceType type);

}  // namespace hf3fs::cache_manager
