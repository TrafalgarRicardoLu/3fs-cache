#pragma once

#include <string>
#include <string_view>

#include "common/utils/Result.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

struct PrefetchPlanKey {
  cache::PrefetchJobId jobId;
  cache::CacheBlockKey block;
  bool operator==(const PrefetchPlanKey &) const = default;
};

struct PinIndexKey {
  cache::CacheBlockKey block;
  cache::PinOwner owner;
  bool operator==(const PinIndexKey &) const = default;
};

class OrchestrationKey {
 public:
  static std::string jobPrefix();
  static std::string job(cache::PrefetchJobId jobId);
  static Result<cache::PrefetchJobId> unpackJob(std::string_view key);

  static std::string planPrefix(cache::PrefetchJobId jobId);
  static std::string plan(cache::PrefetchJobId jobId, const cache::CacheBlockKey &block);
  static Result<PrefetchPlanKey> unpackPlan(std::string_view key);

  static std::string pinByBlockPrefix(const cache::CacheBlockKey &block);
  static std::string pinByBlock(const cache::CacheBlockKey &block, const cache::PinOwner &owner);
  static Result<PinIndexKey> unpackPinByBlock(std::string_view key);

  static std::string pinByOwnerPrefix(const cache::PinOwner &owner);
  static std::string pinByOwner(const cache::PinOwner &owner, const cache::CacheBlockKey &block);
  static Result<PinIndexKey> unpackPinByOwner(std::string_view key);
};

}  // namespace hf3fs::meta::server
