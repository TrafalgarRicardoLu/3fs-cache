#include "cache/origin/ObjectStore.h"

namespace hf3fs::cache::origin {

Result<Void> ListObjectsRequest::valid() const {
  if (originId == OriginId{} || bucket.empty() || prefix.empty()) {
    return makeError(StatusCode::kInvalidArg, "invalid object list origin, bucket, or prefix");
  }
  if (maxKeys == 0 || maxKeys > 1000) {
    return makeError(CacheCode::kRequestTooLarge, "invalid object list page limit");
  }
  return Void{};
}

CoTryTask<ListObjectsPage> ObjectStore::listObjects(const ListObjectsRequest &) {
  co_return makeError(StatusCode::kNotImplemented, "object listing is not implemented");
}

}  // namespace hf3fs::cache::origin
