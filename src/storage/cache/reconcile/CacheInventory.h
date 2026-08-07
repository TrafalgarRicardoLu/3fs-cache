#pragma once

#include <algorithm>
#include <boost/uuid/name_generator_sha1.hpp>
#include <string>
#include <utility>
#include <vector>

#include "common/serde/Serde.h"
#include "fbs/storage/Cache.h"

namespace hf3fs::storage {
namespace detail {

inline constexpr uint32_t kCacheInventoryCursorVersion = 1;

struct CacheInventoryCursor {
  SERDE_STRUCT_FIELD(version, kCacheInventoryCursorVersion);
  SERDE_STRUCT_FIELD(epoch, Uuid::zero());
  SERDE_STRUCT_FIELD(afterKey, std::string{});
};

inline std::string inventorySortKey(const CacheInventoryEntry &entry) {
  auto encoded = serde::serialize(entry.key);
  return std::string(encoded.data(), encoded.size());
}

inline Uuid inventoryEpoch(const Uuid &instanceEpoch, const std::vector<CacheInventoryEntry> &entries) {
  boost::uuids::name_generator_sha1 generator(instanceEpoch);
  std::string identity;
  for (const auto &entry : entries) identity += serde::serialize(entry);
  return Uuid{generator(identity)};
}

inline Result<std::string> encodeInventoryCursor(const CacheInventoryCursor &cursor) {
  auto encoded = serde::serialize(cursor);
  if (encoded.size() > kMaxCacheInventoryCursorBytes)
    return makeError(CacheCode::kInvalidResponse, "cache inventory cursor exceeds protocol limit");
  return std::string(encoded.data(), encoded.size());
}

inline Result<CacheInventoryCursor> decodeInventoryCursor(std::string_view encoded) {
  CacheInventoryCursor cursor;
  RETURN_ON_ERROR(serde::deserialize(cursor, encoded));
  if (cursor.version != kCacheInventoryCursorVersion || cursor.epoch == Uuid::zero() || cursor.afterKey.empty())
    return makeError(StatusCode::kInvalidArg, "invalid cache inventory cursor");
  return cursor;
}

}  // namespace detail

inline Result<ListCacheInventoryRsp> pageCacheInventory(std::vector<CacheInventoryEntry> entries,
                                                        const ListCacheInventoryReq &request,
                                                        const Uuid &instanceEpoch) {
  RETURN_ON_ERROR(request.valid());
  if (instanceEpoch == Uuid::zero()) return makeError(CacheCode::kUnavailable, "inventory instance epoch is not set");
  for (const auto &entry : entries) {
    RETURN_ON_ERROR(entry.valid());
    if (entry.targetId != request.targetId)
      return makeError(CacheCode::kRoleMismatch, "inventory entry belongs to another target");
  }
  std::sort(entries.begin(), entries.end(), [](const auto &lhs, const auto &rhs) {
    return detail::inventorySortKey(lhs) < detail::inventorySortKey(rhs);
  });
  const auto epoch = detail::inventoryEpoch(instanceEpoch, entries);
  std::string afterKey;
  if (!request.cursor.empty()) {
    CHECK_RESULT(cursor, detail::decodeInventoryCursor(request.cursor));
    if (cursor.epoch != epoch) return makeError(CacheCode::kStateConflict, "cache inventory changed while paging");
    afterKey = std::move(cursor.afterKey);
  }

  auto begin = entries.begin();
  if (!afterKey.empty()) {
    begin = std::lower_bound(entries.begin(), entries.end(), afterKey, [](const auto &entry, const auto &key) {
      return detail::inventorySortKey(entry) < key;
    });
    if (begin == entries.end() || detail::inventorySortKey(*begin) != afterKey)
      return makeError(StatusCode::kInvalidArg, "cache inventory cursor key is not in its snapshot");
    ++begin;
  }
  auto count = std::min<size_t>(request.limit, static_cast<size_t>(entries.end() - begin));
  ListCacheInventoryRsp response;
  response.inventoryEpoch = epoch;
  response.entries.assign(begin, begin + count);
  response.done = begin + count == entries.end();
  if (!response.done) {
    detail::CacheInventoryCursor cursor;
    cursor.epoch = epoch;
    cursor.afterKey = detail::inventorySortKey(response.entries.back());
    CHECK_RESULT(encoded, detail::encodeInventoryCursor(cursor));
    response.nextCursor = std::move(encoded);
  }
  RETURN_ON_ERROR(response.valid());
  return response;
}

}  // namespace hf3fs::storage
