#include "cache_manager/reconcile/StorageInventory.h"

#include <folly/experimental/coro/Collect.h>
#include <folly/experimental/coro/Invoke.h>
#include <set>

#include "common/serde/Serde.h"

namespace hf3fs::cache_manager {

CoTryTask<TargetCacheInventory> StorageInventoryReader::readTarget(storage::TargetId targetId) {
  if (targetId == storage::TargetId{} || pageSize_ == 0 || pageSize_ > storage::kMaxCacheStorageBatchItems) {
    co_return makeError(StatusCode::kInvalidArg, "invalid cache inventory reader configuration");
  }

  TargetCacheInventory result;
  result.targetId = targetId;
  std::string cursor;
  std::set<std::string> cursors;
  std::string previousKey;
  while (true) {
    if (shouldStop_ && shouldStop_()) {
      co_return makeError(CacheCode::kUnavailable, "cache inventory scan stopped");
    }
    storage::ListCacheInventoryReq request;
    request.targetId = targetId;
    request.cursor = cursor;
    request.limit = pageSize_;
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto page = co_await backend_->listCacheInventory(std::move(request));
    CO_RETURN_ON_ERROR(page);
    if (auto valid = page->valid(); valid.hasError()) {
      co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
    }
    if (!page->done && page->entries.empty()) {
      co_return makeError(CacheCode::kInvalidResponse, "cache inventory returned an empty nonterminal page");
    }
    if (result.inventoryEpoch == Uuid::zero()) {
      result.inventoryEpoch = page->inventoryEpoch;
    } else if (result.inventoryEpoch != page->inventoryEpoch) {
      co_return makeError(CacheCode::kInvalidResponse, "cache inventory epoch changed while paging");
    }
    for (auto &entry : page->entries) {
      if (entry.targetId != targetId) {
        co_return makeError(CacheCode::kInvalidResponse, "cache inventory entry belongs to another target");
      }
      auto key = serde::serialize(entry.key);
      if (!previousKey.empty() && key <= previousKey) {
        co_return makeError(CacheCode::kInvalidResponse, "cache inventory entries are not strictly ordered");
      }
      previousKey = std::move(key);
      result.entries.push_back(std::move(entry));
    }
    if (page->done) co_return result;
    if (page->nextCursor == cursor || !cursors.emplace(page->nextCursor).second) {
      co_return makeError(CacheCode::kInvalidResponse, "cache inventory cursor did not advance");
    }
    cursor = std::move(page->nextCursor);
  }
}

CoTryTask<void> StorageInventoryReader::readTargetPages(storage::TargetId targetId, PageHandler handler) {
  if (targetId == storage::TargetId{} || pageSize_ == 0 || pageSize_ > storage::kMaxCacheStorageBatchItems ||
      !handler) {
    co_return makeError(StatusCode::kInvalidArg, "invalid streaming cache inventory reader configuration");
  }

  std::string cursor;
  std::set<std::string> cursors;
  std::string previousKey;
  Uuid inventoryEpoch{Uuid::zero()};
  while (true) {
    if (shouldStop_ && shouldStop_()) co_return makeError(CacheCode::kUnavailable, "cache inventory scan stopped");
    storage::ListCacheInventoryReq request;
    request.targetId = targetId;
    request.cursor = cursor;
    request.limit = pageSize_;
    request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
    auto page = co_await backend_->listCacheInventory(std::move(request));
    CO_RETURN_ON_ERROR(page);
    if (auto valid = page->valid(); valid.hasError()) {
      co_return makeError(CacheCode::kInvalidResponse, valid.error().message());
    }
    if (!page->done && page->entries.empty()) {
      co_return makeError(CacheCode::kInvalidResponse, "cache inventory returned an empty nonterminal page");
    }
    if (inventoryEpoch == Uuid::zero()) {
      inventoryEpoch = page->inventoryEpoch;
    } else if (inventoryEpoch != page->inventoryEpoch) {
      co_return makeError(CacheCode::kInvalidResponse, "cache inventory epoch changed while paging");
    }
    for (const auto &entry : page->entries) {
      if (entry.targetId != targetId) {
        co_return makeError(CacheCode::kInvalidResponse, "cache inventory entry belongs to another target");
      }
      auto key = serde::serialize(entry.key);
      if (!previousKey.empty() && key <= previousKey) {
        co_return makeError(CacheCode::kInvalidResponse, "cache inventory entries are not strictly ordered");
      }
      previousKey = std::move(key);
    }
    TargetCacheInventory inventory{targetId, inventoryEpoch, std::move(page->entries)};
    CO_RETURN_ON_ERROR(co_await handler(std::move(inventory)));
    if (page->done) co_return Void{};
    if (page->nextCursor == cursor || !cursors.emplace(page->nextCursor).second) {
      co_return makeError(CacheCode::kInvalidResponse, "cache inventory cursor did not advance");
    }
    cursor = std::move(page->nextCursor);
  }
}

CoTask<std::vector<Result<TargetCacheInventory>>> StorageInventoryReader::readTargets(
    std::span<const storage::TargetId> targetIds) {
  std::vector<Result<TargetCacheInventory>> results;
  results.reserve(targetIds.size());
  if (maxConcurrency_ == 0) {
    results.resize(targetIds.size(), makeError(StatusCode::kInvalidArg, "cache inventory concurrency is zero"));
    co_return results;
  }
  for (size_t begin = 0; begin < targetIds.size(); begin += maxConcurrency_) {
    if (shouldStop_ && shouldStop_()) break;
    auto end = std::min(targetIds.size(), begin + maxConcurrency_);
    std::vector<CoTryTask<TargetCacheInventory>> tasks;
    tasks.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) tasks.push_back(readTarget(targetIds[index]));
    auto batch = co_await folly::coro::collectAllRange(std::move(tasks));
    std::move(batch.begin(), batch.end(), std::back_inserter(results));
  }
  co_return results;
}

}  // namespace hf3fs::cache_manager
