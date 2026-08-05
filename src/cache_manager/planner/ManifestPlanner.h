#pragma once

#include "cache/origin/ObjectStore.h"
#include "cache_manager/planner/NamespaceDatasetPlanner.h"

namespace hf3fs::cache_manager {

struct ManifestPlannerConfig {
  uint64_t maxBytes{8U << 20};
  uint32_t maxLineBytes{cache::kMaxDatasetPathLength};
  uint32_t maxLines{cache::kMaxDatasetPaths};
  uint64_t maxExpandedBlocks{1U << 20};
  uint32_t rangeBytes{64U << 10};

  Result<Void> valid() const;
};

class ManifestPlanner final : public SourcePlanner {
 public:
  ManifestPlanner(std::shared_ptr<NamespaceFileResolver> resolver,
                  std::shared_ptr<cache::origin::ObjectStore> objectStore,
                  cache::ManifestPathSource source,
                  ManifestPlannerConfig config,
                  PlannerContext context,
                  uint32_t maxCursorBytes);

 protected:
  CoTryTask<PlannerPage> plan(std::string_view cursor) override;

 private:
  std::shared_ptr<NamespaceFileResolver> resolver_;
  std::shared_ptr<cache::origin::ObjectStore> objectStore_;
  cache::ManifestPathSource source_;
  ManifestPlannerConfig config_;
};

SourcePlannerFactory::Builder makeManifestPlannerBuilder(std::shared_ptr<NamespaceFileResolver> resolver,
                                                         std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                                         ManifestPlannerConfig config);

}  // namespace hf3fs::cache_manager
