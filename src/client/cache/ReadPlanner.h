#pragma once

#include <memory>
#include <vector>

#include "client/cache/ReadPlanBatcher.h"
#include "client/meta/MetaClient.h"

namespace hf3fs::client::cache {

class IReadPlanSource {
 public:
  virtual ~IReadPlanSource() = default;
  virtual CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) = 0;
};

class MetaReadPlanSource final : public IReadPlanSource {
 public:
  explicit MetaReadPlanSource(meta::client::MetaClient &client)
      : client_(client) {}

  CoTryTask<meta::GetFileReadPlanRsp> fetch(meta::GetFileReadPlanReq request) final;

 private:
  meta::client::MetaClient &client_;
};

class ReadPlanner {
 public:
  explicit ReadPlanner(std::shared_ptr<IReadPlanSource> source)
      : source_(std::move(source)) {}

  CoTryTask<BatchedReadPlan> plan(const flat::UserInfo &user,
                                  const meta::Inode &inode,
                                  const meta::SessionInfo &session,
                                  uint64_t offset,
                                  uint64_t length);

 private:
  static Result<Void> validate(const meta::Inode &inode, hf3fs::cache::ByteRange request, const BatchedReadPlan &plan);

  std::shared_ptr<IReadPlanSource> source_;
};

}  // namespace hf3fs::client::cache
