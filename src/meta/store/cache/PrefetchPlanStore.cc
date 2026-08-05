#include "meta/store/cache/PrefetchPlanStore.h"

#include <algorithm>
#include <limits>
#include <map>

#include "common/serde/Serde.h"
#include "meta/store/cache/OrchestrationKey.h"
#include "meta/store/cache/PrefetchJobStore.h"

namespace hf3fs::meta::server {
namespace {

constexpr size_t kMaxPrefetchPlanValueBytes = 16U << 10;

Result<std::string> encode(const cache::PrefetchPlanEntry &entry) {
  auto value = serde::serialize(entry);
  if (value.size() > kMaxPrefetchPlanValueBytes) {
    return makeError(CacheCode::kRequestTooLarge, "prefetch plan entry is too large");
  }
  return value;
}

Result<cache::PrefetchPlanEntry> decode(const kv::IReadOnlyTransaction::KeyValue &value) {
  auto identity = OrchestrationKey::unpackPlan(value.key);
  RETURN_ON_ERROR(identity);
  cache::PrefetchPlanEntry entry;
  auto deserialized = serde::deserialize(entry, value.value);
  if (deserialized.hasError() || entry.valid().hasError() || entry.jobId != identity->jobId ||
      entry.key != identity->block) {
    return makeError(StatusCode::kDataCorruption, "invalid prefetch plan entry");
  }
  return entry;
}

Result<Void> addChecked(uint64_t &value, uint64_t increment, std::string_view counter) {
  if (increment > std::numeric_limits<uint64_t>::max() - value) {
    return makeError(CacheCode::kStateConflict, std::string(counter) + " overflow");
  }
  value += increment;
  return Void{};
}

bool transitionAllowed(cache::PrefetchPlanEntryState from, cache::PrefetchPlanEntryState to) {
  using State = cache::PrefetchPlanEntryState;
  if (from == to) return true;
  switch (from) {
    case State::PLANNED:
      return to == State::ADMITTED || to == State::ATTACHED || to == State::READY || to == State::FAILED ||
             to == State::CANCELLED;
    case State::ADMITTED:
    case State::ATTACHED:
      return to == State::ATTACHED || to == State::READY || to == State::FAILED || to == State::CANCELLED;
    default:
      return false;
  }
}

}  // namespace

CoTryTask<AppendPrefetchPlanResult> PrefetchPlanStore::append(kv::IReadWriteTransaction &txn,
                                                              cache::PrefetchJobId jobId,
                                                              std::span<const cache::PrefetchPlanEntry> entries,
                                                              uint32_t plannerSourceIndex,
                                                              std::string_view plannerCursor,
                                                              bool planningComplete) {
  if (jobId == cache::PrefetchJobId{} || entries.size() > cache::kMaxPhase2BatchItems ||
      plannerCursor.size() > cache::kMaxDatasetPathLength) {
    co_return makeError(StatusCode::kInvalidArg, "invalid prefetch plan page");
  }
  auto loaded = co_await PrefetchJobStore::load(txn, jobId);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch job not found");
  auto job = std::move(**loaded);
  if ((job.state != cache::PrefetchJobState::PENDING && job.state != cache::PrefetchJobState::PLANNING) ||
      plannerSourceIndex > job.spec.sources.size()) {
    co_return makeError(CacheCode::kStateConflict, "prefetch job is not planning or cursor is invalid");
  }

  AppendPrefetchPlanResult result;
  std::map<std::string, std::string> inserts;
  for (const auto &entry : entries) {
    if (entry.jobId != jobId) co_return makeError(StatusCode::kInvalidArg, "prefetch plan entry job mismatch");
    CO_RETURN_ON_ERROR(entry.valid());
    auto key = OrchestrationKey::plan(jobId, entry.key);
    auto encoded = encode(entry);
    CO_RETURN_ON_ERROR(encoded);
    auto staged = inserts.find(key);
    if (staged != inserts.end()) {
      if (staged->second != *encoded) co_return makeError(CacheCode::kStateConflict, "prefetch plan key changed");
      continue;
    }
    auto existing = co_await txn.get(key);
    CO_RETURN_ON_ERROR(existing);
    if (existing->has_value()) {
      kv::IReadOnlyTransaction::KeyValue value{key, **existing};
      auto decoded = decode(value);
      CO_RETURN_ON_ERROR(decoded);
      if (*decoded != entry) co_return makeError(CacheCode::kStateConflict, "prefetch plan key changed");
      continue;
    }
    CO_RETURN_ON_ERROR(addChecked(result.insertedBlocks, 1, "prefetch inserted block count"));
    CO_RETURN_ON_ERROR(addChecked(result.insertedBytes, entry.blockLength, "prefetch inserted byte count"));
    inserts.emplace(std::move(key), std::move(*encoded));
  }

  const bool cursorChanged = job.plannerSourceIndex != plannerSourceIndex || job.plannerCursor != plannerCursor ||
                             job.planningComplete != planningComplete;
  if (job.planningComplete && (cursorChanged || !inserts.empty())) {
    co_return makeError(CacheCode::kStateConflict, "prefetch job planning is already complete");
  }
  if (result.insertedBlocks != 0 || cursorChanged) {
    CO_RETURN_ON_ERROR(addChecked(job.plannedBlocks, result.insertedBlocks, "prefetch planned block count"));
    CO_RETURN_ON_ERROR(addChecked(job.plannedBytes, result.insertedBytes, "prefetch planned byte count"));
    job.plannerSourceIndex = plannerSourceIndex;
    job.plannerCursor = plannerCursor;
    job.planningComplete = planningComplete;
    job.state = cache::PrefetchJobState::PLANNING;
    if (job.stateVersion == std::numeric_limits<uint64_t>::max() ||
        job.updatedAtMs == std::numeric_limits<uint64_t>::max()) {
      co_return makeError(CacheCode::kStateConflict, "prefetch job planner version exhausted");
    }
    ++job.stateVersion;
    ++job.updatedAtMs;
    auto updated = co_await PrefetchJobStore::update(txn, job.stateVersion - 1, job);
    CO_RETURN_ON_ERROR(updated);
    result.job = std::move(*updated);
  } else {
    result.job = std::move(job);
  }
  for (const auto &[key, value] : inserts) CO_RETURN_ON_ERROR(co_await txn.set(key, value));
  co_return result;
}

CoTryTask<PrefetchPlanPage> PrefetchPlanStore::snapshotList(kv::IReadOnlyTransaction &txn,
                                                            cache::PrefetchJobId jobId,
                                                            std::optional<cache::CacheBlockKey> after,
                                                            uint32_t limit) {
  if (jobId == cache::PrefetchJobId{} || limit == 0 || limit > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidArg, "invalid prefetch plan page");
  }
  if (after) CO_RETURN_ON_ERROR(after->valid());
  auto prefix = OrchestrationKey::planPrefix(jobId);
  auto begin = after ? OrchestrationKey::plan(jobId, *after) : prefix;
  auto end = kv::TransactionHelper::prefixListEndKey(prefix);
  auto values =
      co_await txn.snapshotGetRange({begin, !after.has_value()}, {end, false}, static_cast<int32_t>(limit + 1));
  CO_RETURN_ON_ERROR(values);
  PrefetchPlanPage page;
  page.more = values->kvs.size() > limit || values->hasMore;
  auto count = std::min<size_t>(values->kvs.size(), limit);
  page.entries.reserve(count);
  for (size_t index = 0; index < count; ++index) {
    auto entry = decode(values->kvs[index]);
    CO_RETURN_ON_ERROR(entry);
    page.entries.push_back(std::move(*entry));
  }
  co_return page;
}

CoTryTask<cache::PrefetchPlanEntry> PrefetchPlanStore::update(kv::IReadWriteTransaction &txn,
                                                              const cache::PrefetchPlanEntry &expected,
                                                              const cache::PrefetchPlanEntry &desired) {
  CO_RETURN_ON_ERROR(expected.valid());
  CO_RETURN_ON_ERROR(desired.valid());
  if (expected.jobId != desired.jobId || expected.key != desired.key || expected.blockLength != desired.blockLength ||
      expected.priority != desired.priority || !transitionAllowed(expected.state, desired.state)) {
    co_return makeError(StatusCode::kInvalidArg, "invalid prefetch plan entry transition");
  }
  auto key = OrchestrationKey::plan(expected.jobId, expected.key);
  auto loaded = co_await txn.get(key);
  CO_RETURN_ON_ERROR(loaded);
  if (!loaded->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch plan entry not found");
  kv::IReadOnlyTransaction::KeyValue currentValue{key, **loaded};
  auto current = decode(currentValue);
  CO_RETURN_ON_ERROR(current);
  if (*current == desired) co_return desired;
  if (*current != expected) co_return makeError(CacheCode::kStateConflict, "prefetch plan entry CAS mismatch");
  auto value = encode(desired);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(key, *value));
  co_return desired;
}

}  // namespace hf3fs::meta::server
