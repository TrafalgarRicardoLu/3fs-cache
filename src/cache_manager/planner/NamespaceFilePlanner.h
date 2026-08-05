#pragma once

#include <memory>
#include <string_view>

#include "cache_manager/planner/SourcePlanner.h"
#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

class NamespaceFileResolver {
 public:
  virtual ~NamespaceFileResolver() = default;

  virtual CoTryTask<meta::Inode> stat(std::string_view path) = 0;
};

class MetaNamespaceFileResolver final : public NamespaceFileResolver {
 public:
  MetaNamespaceFileResolver(std::shared_ptr<meta::client::MetaClient> metaClient, flat::UserInfo user)
      : metaClient_(std::move(metaClient)),
        user_(std::move(user)) {}

  CoTryTask<meta::Inode> stat(std::string_view path) override;

 private:
  std::shared_ptr<meta::client::MetaClient> metaClient_;
  flat::UserInfo user_;
};

class NamespaceFilePlanner final : public SourcePlanner {
 public:
  NamespaceFilePlanner(std::shared_ptr<NamespaceFileResolver> resolver,
                       cache::NamespacePathSource source,
                       PlannerContext context,
                       uint32_t maxCursorBytes);

 protected:
  CoTryTask<PlannerPage> plan(std::string_view cursor) override;

 private:
  std::shared_ptr<NamespaceFileResolver> resolver_;
  cache::NamespacePathSource source_;
};

SourcePlannerFactory::Builder makeNamespaceFilePlannerBuilder(std::shared_ptr<NamespaceFileResolver> resolver);

}  // namespace hf3fs::cache_manager
