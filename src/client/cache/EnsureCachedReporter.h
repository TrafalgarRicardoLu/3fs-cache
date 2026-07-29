#pragma once

#include "common/utils/Duration.h"
#include "fbs/cache_manager/Service.h"
#include "fbs/meta/Service.h"
#include "stubs/cache_manager/ICacheManagerServiceStub.h"

namespace hf3fs::client::cache {

class IEnsureCachedReporter {
 public:
  virtual ~IEnsureCachedReporter() = default;
  virtual CoTryTask<void> ensure(meta::InodeId inode, hf3fs::cache::CacheBlockIndex block) = 0;
  virtual CoTryTask<void> report(const flat::UserInfo &user,
                                 const meta::SessionInfo &session,
                                 const meta::ReadBlockPlan &plan,
                                 cache_manager::InvalidReason reason) = 0;
};

class EnsureCachedReporter final : public IEnsureCachedReporter {
 public:
  EnsureCachedReporter(cache_manager::ICacheManagerServiceStub &stub,
                       cache_manager::ServiceIdentity service,
                       Duration timeout = 500_ms)
      : stub_(stub),
        service_(std::move(service)),
        timeout_(timeout) {}

  CoTryTask<void> ensure(meta::InodeId inode, hf3fs::cache::CacheBlockIndex block) final;
  CoTryTask<void> report(const flat::UserInfo &user,
                         const meta::SessionInfo &session,
                         const meta::ReadBlockPlan &plan,
                         cache_manager::InvalidReason reason) final;

 private:
  net::UserRequestOptions options() const;

  cache_manager::ICacheManagerServiceStub &stub_;
  cache_manager::ServiceIdentity service_;
  Duration timeout_;
};

}  // namespace hf3fs::client::cache
