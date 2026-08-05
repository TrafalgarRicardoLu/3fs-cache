#pragma once

#include <memory>
#include <string_view>

#include "cache_manager/planner/SourcePlanner.h"
#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

struct NamespaceEntry {
  std::string name;
  meta::Inode inode;
};

struct NamespaceListPage {
  std::vector<NamespaceEntry> entries;
  bool more{false};
};

class NamespaceFileResolver {
 public:
  virtual ~NamespaceFileResolver() = default;

  virtual CoTryTask<meta::Inode> stat(std::string_view path) = 0;
  virtual CoTryTask<NamespaceListPage> list(meta::InodeId directory, std::string_view after, uint32_t limit);
};

class MetaNamespaceFileResolver final : public NamespaceFileResolver {
 public:
  MetaNamespaceFileResolver(std::shared_ptr<meta::client::MetaClient> metaClient, flat::UserInfo user)
      : metaClient_(std::move(metaClient)),
        user_(std::move(user)) {}

  CoTryTask<meta::Inode> stat(std::string_view path) override;
  CoTryTask<NamespaceListPage> list(meta::InodeId directory, std::string_view after, uint32_t limit) override;

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

Result<Void> validateOriginFileSnapshot(const meta::Inode &inode);
Result<uint64_t> originFileBlockCount(const meta::Inode &inode);

}  // namespace hf3fs::cache_manager
