#pragma once

#include <atomic>
#include <folly/experimental/coro/Baton.h>
#include <mutex>
#include <optional>
#include <vector>

#include "cache/origin/ObjectStore.h"

namespace hf3fs::client::cache::test {

inline hf3fs::cache::ImmutableObjectIdentity identity(std::string bucket = "bucket", std::string key = "key") {
  return {hf3fs::cache::OriginId{1},
          std::move(bucket),
          std::move(key),
          hf3fs::cache::VersionSelector{hf3fs::cache::VersionSelectorType::VERSION_ID, "version-1"}};
}

class FakeObjectStore : public hf3fs::cache::origin::ObjectStore {
 public:
  explicit FakeObjectStore(std::vector<uint8_t> object)
      : object_(std::move(object)) {}

  CoTryTask<hf3fs::cache::origin::ObjectMetadata> head(const hf3fs::cache::ObjectRef &) override {
    co_return makeError(StatusCode::kNotImplemented);
  }

  CoTryTask<std::vector<uint8_t>> getRange(const hf3fs::cache::ImmutableObjectIdentity &object,
                                           hf3fs::cache::ByteRange range) override {
    auto call = ++calls;
    {
      std::lock_guard lock(mutex_);
      requested.push_back(range);
    }
    if (entered != nullptr && call == 1) {
      entered->post();
      co_await *release;
    }
    if (object != expectedIdentity) co_return makeError(CacheCode::kVersionMismatch);
    if (failureCall.has_value() && call == *failureCall) co_return makeError(failureCode);
    auto end = range.end();
    CO_RETURN_ON_ERROR(end);
    if (*end > object_.size()) co_return makeError(CacheCode::kInvalidResponse);
    co_return std::vector<uint8_t>(object_.begin() + range.offset, object_.begin() + *end);
  }

  std::atomic<size_t> calls{0};
  hf3fs::cache::ImmutableObjectIdentity expectedIdentity = identity();
  std::optional<size_t> failureCall;
  status_code_t failureCode{CacheCode::kUnavailable};
  folly::coro::Baton *entered{nullptr};
  folly::coro::Baton *release{nullptr};
  std::vector<hf3fs::cache::ByteRange> requested;

 private:
  std::vector<uint8_t> object_;
  std::mutex mutex_;
};

inline std::vector<uint8_t> sequence(size_t size) {
  std::vector<uint8_t> result(size);
  for (size_t i = 0; i < size; ++i) result[i] = static_cast<uint8_t>(i);
  return result;
}

}  // namespace hf3fs::client::cache::test
