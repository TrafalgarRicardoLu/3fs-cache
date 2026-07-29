#include "client/cache/LocalMissSingleflight.h"

#include <folly/ScopeGuard.h>

#include "common/serde/Serde.h"

namespace hf3fs::client::cache {
namespace {

struct SingleflightKey {
  SERDE_STRUCT_FIELD(object, hf3fs::cache::ImmutableObjectIdentity{});
  SERDE_STRUCT_FIELD(range, hf3fs::cache::ByteRange{});
};

}  // namespace

std::string LocalMissSingleflight::key(const hf3fs::cache::ImmutableObjectIdentity &object,
                                       hf3fs::cache::ByteRange alignedRange) {
  return serde::serialize(SingleflightKey{object, alignedRange});
}

void LocalMissSingleflight::erase(const std::string &key, const std::shared_ptr<Entry> &entry) {
  std::lock_guard lock(mutex_);
  auto it = entries_.find(key);
  if (it != entries_.end() && it->second == entry) entries_.erase(it);
}

CoTryTask<LocalMissSingleflight::Data> LocalMissSingleflight::run(const hf3fs::cache::ImmutableObjectIdentity &object,
                                                                  hf3fs::cache::ByteRange alignedRange,
                                                                  Loader loader) {
  CO_RETURN_ON_ERROR(object.valid());
  CO_RETURN_ON_ERROR(alignedRange.end());
  if (alignedRange.empty()) co_return Data{};

  auto flightKey = key(object, alignedRange);
  std::shared_ptr<Entry> entry;
  bool producer = false;
  {
    std::lock_guard lock(mutex_);
    auto [it, inserted] = entries_.try_emplace(flightKey, std::make_shared<Entry>());
    entry = it->second;
    producer = inserted;
  }
  if (!producer) {
    co_await entry->ready;
    co_return entry->result;
  }

  bool completed = false;
  SCOPE_EXIT {
    if (!completed) {
      entry->result = makeError(StatusCode::kUnknown, "singleflight loader raised an exception");
      entry->ready.post();
      erase(flightKey, entry);
    }
  };
  entry->result = co_await loader();
  completed = true;
  entry->ready.post();
  erase(flightKey, entry);
  co_return entry->result;
}

size_t LocalMissSingleflight::active() const {
  std::lock_guard lock(mutex_);
  return entries_.size();
}

}  // namespace hf3fs::client::cache
