#pragma once

#include <functional>
#include <vector>

#include "common/utils/Coroutine.h"
#include "fbs/meta/Service.h"

namespace hf3fs::client::cache {

struct BatchedReadPlan {
  meta::InodeId inode;
  hf3fs::cache::ImmutableObjectIdentity object;
  std::vector<meta::ReadBlockPlan> blocks;
};

class ReadPlanBatcher {
 public:
  using Fetch = std::function<CoTryTask<meta::GetFileReadPlanRsp>(meta::GetFileReadPlanReq)>;

  static CoTryTask<BatchedReadPlan> fetch(const meta::Inode &openedInode,
                                          meta::GetFileReadPlanReq request,
                                          Fetch fetch,
                                          uint32_t maxReplans = 1);
};

}  // namespace hf3fs::client::cache
