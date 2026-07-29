#pragma once

#include <span>
#include <vector>

#include "client/storage/StorageClient.h"
#include "common/utils/Coroutine.h"
#include "fbs/meta/Service.h"

namespace hf3fs::client::cache {

class ICacheHitReader {
 public:
  virtual ~ICacheHitReader() = default;
  virtual CoTryTask<std::vector<uint8_t>> readFullBlock(const meta::ReadBlockPlan &plan,
                                                        const flat::UserInfo &user) = 0;
};

class StorageCacheHitReader final : public ICacheHitReader {
 public:
  explicit StorageCacheHitReader(storage::client::StorageClient &client)
      : client_(client) {}

  CoTryTask<std::vector<uint8_t>> readFullBlock(const meta::ReadBlockPlan &plan, const flat::UserInfo &user) final;

  static Result<Void> validate(const meta::ReadBlockPlan &plan,
                               std::span<const uint8_t> data,
                               hf3fs::cache::CacheGeneration generation);

 private:
  storage::client::StorageClient &client_;
};

}  // namespace hf3fs::client::cache
