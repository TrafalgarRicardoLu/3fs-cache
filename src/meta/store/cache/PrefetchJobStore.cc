#include "meta/store/cache/PrefetchJobStore.h"

#include <algorithm>

#include "common/serde/Serde.h"
#include "meta/store/cache/OrchestrationKey.h"

namespace hf3fs::meta::server {
namespace {

constexpr size_t kMaxPrefetchJobValueBytes = 96U << 10;
constexpr int32_t kListScanBatch = 256;

bool terminal(cache::PrefetchJobState state) {
  return state == cache::PrefetchJobState::READY || state == cache::PrefetchJobState::FAILED ||
         state == cache::PrefetchJobState::CANCELLED;
}

Result<Void> validInitialJob(const cache::PrefetchJobRecord &job) {
  RETURN_ON_ERROR(job.valid());
  if (job.state != cache::PrefetchJobState::PENDING || job.stateVersion != 1 || job.plannedBytes != 0 ||
      job.readyBytes != 0 || job.failedBytes != 0 || job.plannedBlocks != 0 || job.readyBlocks != 0 ||
      job.failedBlocks != 0 || job.cancelEpoch != 0 || !job.error.empty()) {
    return makeError(StatusCode::kInvalidArg, "prefetch job is not in its initial state");
  }
  return Void{};
}

Result<std::string> encode(const cache::PrefetchJobRecord &job) {
  auto value = serde::serialize(job);
  if (value.size() > kMaxPrefetchJobValueBytes) {
    return makeError(CacheCode::kRequestTooLarge, "prefetch job record is too large");
  }
  return value;
}

Result<cache::PrefetchJobRecord> decode(std::string_view key, std::string_view value) {
  auto jobId = OrchestrationKey::unpackJob(key);
  RETURN_ON_ERROR(jobId);
  cache::PrefetchJobRecord job;
  auto deserialized = serde::deserialize(job, value);
  if (deserialized.hasError() || job.valid().hasError() || job.spec.jobId != *jobId) {
    return makeError(StatusCode::kDataCorruption, "invalid prefetch job record");
  }
  return job;
}

template <typename Transaction>
CoTryTask<std::optional<cache::PrefetchJobRecord>> loadJob(Transaction &txn,
                                                           cache::PrefetchJobId jobId,
                                                           bool snapshot) {
  if (jobId == cache::PrefetchJobId{}) co_return makeError(StatusCode::kInvalidArg, "prefetch job id not set");
  auto key = OrchestrationKey::job(jobId);
  auto value = snapshot ? co_await txn.snapshotGet(key) : co_await txn.get(key);
  CO_RETURN_ON_ERROR(value);
  if (!value->has_value()) co_return std::nullopt;
  auto job = decode(key, **value);
  CO_RETURN_ON_ERROR(job);
  co_return std::move(*job);
}

}  // namespace

CoTryTask<CreatePrefetchJobResult> PrefetchJobStore::create(kv::IReadWriteTransaction &txn,
                                                            const cache::PrefetchJobRecord &job) {
  CO_RETURN_ON_ERROR(validInitialJob(job));
  auto existing = co_await load(txn, job.spec.jobId);
  CO_RETURN_ON_ERROR(existing);
  if (existing->has_value()) {
    if ((*existing)->spec != job.spec) {
      co_return makeError(CacheCode::kStateConflict, "prefetch job id is already used by another spec");
    }
    co_return CreatePrefetchJobResult{std::move(**existing), false};
  }
  auto value = encode(job);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(OrchestrationKey::job(job.spec.jobId), *value));
  co_return CreatePrefetchJobResult{job, true};
}

CoTryTask<std::optional<cache::PrefetchJobRecord>> PrefetchJobStore::snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                                  cache::PrefetchJobId jobId) {
  co_return co_await loadJob(txn, jobId, true);
}

CoTryTask<std::optional<cache::PrefetchJobRecord>> PrefetchJobStore::load(kv::IReadWriteTransaction &txn,
                                                                          cache::PrefetchJobId jobId) {
  co_return co_await loadJob(txn, jobId, false);
}

CoTryTask<PrefetchJobPage> PrefetchJobStore::snapshotList(kv::IReadOnlyTransaction &txn,
                                                          std::optional<flat::Uid> ownerUid,
                                                          std::optional<cache::PrefetchJobId> after,
                                                          uint32_t limit) {
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems || (after && *after == cache::PrefetchJobId{})) {
    co_return makeError(StatusCode::kInvalidArg, "invalid prefetch job page");
  }

  auto prefix = OrchestrationKey::jobPrefix();
  auto beginKey = after ? OrchestrationKey::job(*after) : prefix;
  auto endKey = kv::TransactionHelper::prefixListEndKey(prefix);
  bool inclusive = !after.has_value();
  PrefetchJobPage page;
  while (page.jobs.size() <= limit) {
    auto values = co_await txn.snapshotGetRange({beginKey, inclusive}, {endKey, false}, kListScanBatch);
    CO_RETURN_ON_ERROR(values);
    for (const auto &value : values->kvs) {
      auto job = decode(value.key, value.value);
      CO_RETURN_ON_ERROR(job);
      if (!ownerUid || job->spec.ownerUid == *ownerUid) page.jobs.push_back(std::move(*job));
      if (page.jobs.size() > limit) break;
    }
    if (page.jobs.size() > limit || !values->hasMore) break;
    if (values->kvs.empty()) co_return makeError(StatusCode::kDataCorruption, "prefetch job range did not advance");
    beginKey = values->kvs.back().key;
    inclusive = false;
  }
  page.more = page.jobs.size() > limit;
  if (page.more) page.jobs.resize(limit);
  co_return page;
}

CoTryTask<cache::PrefetchJobRecord> PrefetchJobStore::update(kv::IReadWriteTransaction &txn,
                                                             uint64_t expectedStateVersion,
                                                             const cache::PrefetchJobRecord &job) {
  CO_RETURN_ON_ERROR(job.valid());
  if (expectedStateVersion == 0 || job.stateVersion != expectedStateVersion + 1) {
    co_return makeError(StatusCode::kInvalidArg, "invalid prefetch job state version transition");
  }
  auto existing = co_await load(txn, job.spec.jobId);
  CO_RETURN_ON_ERROR(existing);
  if (!existing->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch job not found");
  if ((*existing)->stateVersion != expectedStateVersion || (*existing)->spec != job.spec ||
      (*existing)->createdAtMs != job.createdAtMs) {
    co_return makeError(CacheCode::kStateConflict, "prefetch job update fence changed");
  }
  if (terminal((*existing)->state)) {
    co_return makeError(CacheCode::kStateConflict, "terminal prefetch job cannot be updated");
  }
  if (job.updatedAtMs < (*existing)->updatedAtMs) {
    co_return makeError(CacheCode::kStateConflict, "prefetch job update time moved backwards");
  }
  auto value = encode(job);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(OrchestrationKey::job(job.spec.jobId), *value));
  co_return job;
}

}  // namespace hf3fs::meta::server
