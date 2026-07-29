#pragma once

#include <folly/experimental/coro/Baton.h>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::client::cache {

class LocalMissSingleflight {
 public:
  using Data = std::vector<uint8_t>;
  using Loader = std::function<CoTryTask<Data>()>;

  CoTryTask<Data> run(const hf3fs::cache::ImmutableObjectIdentity &object,
                      hf3fs::cache::ByteRange alignedRange,
                      Loader loader);

  size_t active() const;

 private:
  struct Entry {
    Result<Data> result = makeError(StatusCode::kUnknown, "singleflight load did not complete");
    folly::coro::Baton ready;
  };

  static std::string key(const hf3fs::cache::ImmutableObjectIdentity &object, hf3fs::cache::ByteRange alignedRange);
  void erase(const std::string &key, const std::shared_ptr<Entry> &entry);

  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Entry>> entries_;
};

}  // namespace hf3fs::client::cache
