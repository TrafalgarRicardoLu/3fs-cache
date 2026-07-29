#include "cache/origin/RoutedObjectStore.h"

namespace hf3fs::cache::origin {

Result<std::shared_ptr<ObjectStore>> RoutedObjectStore::find(OriginId originId) const {
  auto it = stores_.find(originId.toUnderType());
  if (it == stores_.end() || !it->second) {
    return makeError(StatusCode::kInvalidConfig, "origin {} is not configured", originId.toUnderType());
  }
  return it->second;
}

CoTryTask<ObjectMetadata> RoutedObjectStore::head(const ObjectRef &object) {
  auto store = find(object.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->head(object);
}

CoTryTask<std::vector<uint8_t>> RoutedObjectStore::getRange(const ImmutableObjectIdentity &object, ByteRange range) {
  auto store = find(object.originId);
  CO_RETURN_ON_ERROR(store);
  co_return co_await (*store)->getRange(object, range);
}

}  // namespace hf3fs::cache::origin
