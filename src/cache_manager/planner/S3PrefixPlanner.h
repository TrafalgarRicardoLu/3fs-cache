#pragma once

#include "cache_manager/planner/ManifestPlanner.h"

namespace hf3fs::cache_manager {

struct PrefixImportLayout {
  flat::ChainTableId tableId;
  uint32_t blockSize{0};
  uint32_t stripeSize{1};
  meta::Permission permission{0444};

  Result<Void> valid() const;
};

struct S3PrefixPlannerConfig {
  PrefixImportLayout layout;
  uint64_t maxObjects{1U << 20};

  Result<Void> valid() const;
};

class OriginFileImporter {
 public:
  virtual ~OriginFileImporter() = default;
  virtual CoTryTask<meta::Inode> import(std::string_view path,
                                        const cache::origin::ObjectMetadata &object,
                                        const PrefixImportLayout &layout) = 0;
};

class MetaOriginFileImporter final : public OriginFileImporter {
 public:
  MetaOriginFileImporter(std::shared_ptr<meta::client::MetaClient> metaClient, flat::UserInfo user)
      : metaClient_(std::move(metaClient)),
        user_(std::move(user)) {}

  CoTryTask<meta::Inode> import(std::string_view path,
                                const cache::origin::ObjectMetadata &object,
                                const PrefixImportLayout &layout) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  flat::UserInfo user_;
};

class S3PrefixPlanner final : public SourcePlanner {
 public:
  S3PrefixPlanner(std::shared_ptr<cache::origin::ObjectStore> objectStore,
                  std::shared_ptr<OriginFileImporter> importer,
                  cache::S3PrefixSource source,
                  S3PrefixPlannerConfig config,
                  PlannerContext context,
                  uint32_t maxCursorBytes);

 protected:
  CoTryTask<PlannerPage> plan(std::string_view cursor) override;

 private:
  std::shared_ptr<cache::origin::ObjectStore> objectStore_;
  std::shared_ptr<OriginFileImporter> importer_;
  cache::S3PrefixSource source_;
  S3PrefixPlannerConfig config_;
};

SourcePlannerFactory::Builder makeS3PrefixPlannerBuilder(std::shared_ptr<cache::origin::ObjectStore> objectStore,
                                                         std::shared_ptr<OriginFileImporter> importer,
                                                         S3PrefixPlannerConfig config);

}  // namespace hf3fs::cache_manager
