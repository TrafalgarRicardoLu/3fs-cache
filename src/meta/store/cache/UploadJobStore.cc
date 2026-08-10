#include "meta/store/cache/UploadJobStore.h"

#include <algorithm>
#include <limits>

#include "common/serde/Serde.h"
#include "meta/store/cache/UploadJobKey.h"

namespace hf3fs::meta::server {
namespace {

constexpr size_t kMaxUploadJobValueBytes = 96U << 10;
constexpr int32_t kListScanBatch = 256;

bool terminal(const cache::UploadJobRecord &job) {
  if (job.state != cache::UploadJobState::FAILED && job.state != cache::UploadJobState::CANCELLED) return false;
  return !job.completedObject || job.orphanCleanupState == cache::OrphanCleanupState::COMPLETE ||
         job.orphanCleanupState == cache::OrphanCleanupState::CONFLICT;
}

bool sameSpec(const cache::UploadJobRecord &left, const cache::UploadJobRecord &right) {
  return left.jobId == right.jobId && left.ownerUid == right.ownerUid && left.path == right.path &&
         left.stagingInode == right.stagingInode && left.destination == right.destination &&
         left.createdAtMs == right.createdAtMs;
}

bool validTransition(cache::UploadJobState from, cache::UploadJobState to) {
  if (from == to) return true;
  switch (from) {
    case cache::UploadJobState::OPEN:
      return to == cache::UploadJobState::SEALED || to == cache::UploadJobState::FAILED ||
             to == cache::UploadJobState::CANCELLED;
    case cache::UploadJobState::SEALED:
      return to == cache::UploadJobState::UPLOADING || to == cache::UploadJobState::FAILED ||
             to == cache::UploadJobState::CANCELLED;
    case cache::UploadJobState::UPLOADING:
      return to == cache::UploadJobState::COMPLETING || to == cache::UploadJobState::ABORTING ||
             to == cache::UploadJobState::FAILED;
    case cache::UploadJobState::COMPLETING:
      return to == cache::UploadJobState::PUBLISHING || to == cache::UploadJobState::ABORTING ||
             to == cache::UploadJobState::FAILED;
    case cache::UploadJobState::PUBLISHING:
      return to == cache::UploadJobState::PUBLISHED || to == cache::UploadJobState::FAILED;
    case cache::UploadJobState::ABORTING:
      return to == cache::UploadJobState::CANCELLED || to == cache::UploadJobState::FAILED;
    case cache::UploadJobState::FAILED:
      return to == cache::UploadJobState::SEALED || to == cache::UploadJobState::UPLOADING ||
             to == cache::UploadJobState::COMPLETING || to == cache::UploadJobState::PUBLISHING ||
             to == cache::UploadJobState::ABORTING ||
             to == cache::UploadJobState::CANCELLED;
    default:
      return false;
  }
}

bool validCleanupTransition(const cache::UploadJobRecord &current, const cache::UploadJobRecord &next) {
  auto from = current.orphanCleanupState;
  auto to = next.orphanCleanupState;
  if (from == to) return true;
  if (from == cache::OrphanCleanupState::NONE) {
    return to == cache::OrphanCleanupState::PENDING || to == cache::OrphanCleanupState::DELETING;
  }
  if (from == cache::OrphanCleanupState::PENDING) {
    return to == cache::OrphanCleanupState::DELETING ||
           (to == cache::OrphanCleanupState::NONE && current.state == cache::UploadJobState::FAILED &&
            next.state == cache::UploadJobState::PUBLISHING);
  }
  if (from == cache::OrphanCleanupState::DELETING) {
    return to == cache::OrphanCleanupState::COMPLETE || to == cache::OrphanCleanupState::CONFLICT;
  }
  return false;
}

Result<Void> validInitialJob(const cache::UploadJobRecord &job) {
  RETURN_ON_ERROR(job.valid());
  if (job.state != cache::UploadJobState::OPEN || job.stateVersion != 1 || job.stagingLength != 0 ||
      job.nextPartNumber != 1 || !job.parts.empty() || !job.multipartId.empty() || job.completedObject ||
      job.publishedInode != 0 || !job.error.empty()) {
    return makeError(StatusCode::kInvalidArg, "upload job is not in its initial state");
  }
  return Void{};
}

Result<std::string> encode(const cache::UploadJobRecord &job) {
  auto value = serde::serialize(job);
  if (value.size() > kMaxUploadJobValueBytes) {
    return makeError(CacheCode::kRequestTooLarge, "upload job record is too large");
  }
  return value;
}

Result<cache::UploadJobRecord> decode(std::string_view key, std::string_view value) {
  auto jobId = UploadJobKey::unpack(key);
  RETURN_ON_ERROR(jobId);
  cache::UploadJobRecord job;
  auto deserialized = serde::deserialize(job, value);
  if (deserialized.hasError() || job.valid().hasError() || job.jobId != *jobId) {
    return makeError(StatusCode::kDataCorruption, "invalid upload job record");
  }
  return job;
}

template <typename Transaction>
CoTryTask<std::optional<cache::UploadJobRecord>> loadJob(Transaction &txn, cache::UploadJobId jobId, bool snapshot) {
  if (jobId == cache::UploadJobId{}) co_return makeError(StatusCode::kInvalidArg, "upload job id not set");
  auto key = UploadJobKey::job(jobId);
  auto value = snapshot ? co_await txn.snapshotGet(key) : co_await txn.get(key);
  CO_RETURN_ON_ERROR(value);
  if (!value->has_value()) co_return std::nullopt;
  auto job = decode(key, **value);
  CO_RETURN_ON_ERROR(job);
  co_return std::move(*job);
}

}  // namespace

CoTryTask<CreateUploadJobResult> UploadJobStore::create(kv::IReadWriteTransaction &txn,
                                                        const cache::UploadJobRecord &job) {
  CO_RETURN_ON_ERROR(validInitialJob(job));
  auto existing = co_await load(txn, job.jobId);
  CO_RETURN_ON_ERROR(existing);
  if (existing->has_value()) {
    if (!sameSpec(**existing, job)) {
      co_return makeError(CacheCode::kStateConflict, "upload job id is already used by another spec");
    }
    CO_RETURN_ON_ERROR(co_await indexActive(txn, **existing));
    co_return CreateUploadJobResult{std::move(**existing), false};
  }
  auto value = encode(job);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::job(job.jobId), *value));
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::active(job.jobId), *value));
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::openLease(job.writerLeaseExpiresAtMs, job.jobId), *value));
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::state(job.state, job.jobId), *value));
  co_return CreateUploadJobResult{job, true};
}

CoTryTask<ExpiredOpenUploadPage> UploadJobStore::snapshotListExpiredOpen(
    kv::IReadOnlyTransaction &txn,
    uint64_t expiresBeforeMs,
    std::optional<meta::UploadOpenLeaseCursor> after,
    uint32_t limit) {
  if (expiresBeforeMs == 0 || limit == 0 || limit > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidArg, "invalid expired open upload page");
  }
  if (after) CO_RETURN_ON_ERROR(after->valid());
  auto prefix = UploadJobKey::openLeasePrefix();
  auto beginKey = after ? UploadJobKey::openLease(after->expiresAtMs, after->jobId) : prefix;
  auto endKey = expiresBeforeMs == std::numeric_limits<uint64_t>::max()
                    ? kv::TransactionHelper::prefixListEndKey(prefix)
                    : UploadJobKey::openLeaseTime(expiresBeforeMs + 1);
  auto values = co_await txn.snapshotGetRange({beginKey, !after.has_value()}, {endKey, false}, limit + 1);
  CO_RETURN_ON_ERROR(values);
  ExpiredOpenUploadPage page;
  page.more = values->kvs.size() > limit;
  auto count = std::min<size_t>(values->kvs.size(), limit);
  page.jobs.reserve(count);
  for (size_t index = 0; index < count; ++index) {
    const auto &value = values->kvs[index];
    auto cursor = UploadJobKey::unpackOpenLease(value.key);
    CO_RETURN_ON_ERROR(cursor);
    auto job = decode(UploadJobKey::job(cursor->second), value.value);
    CO_RETURN_ON_ERROR(job);
    if (job->state != cache::UploadJobState::OPEN || job->writerLeaseExpiresAtMs != cursor->first) {
      co_return makeError(StatusCode::kDataCorruption, "stale open upload lease index");
    }
    page.jobs.push_back(std::move(*job));
  }
  co_return page;
}

CoTryTask<std::optional<cache::UploadJobRecord>> UploadJobStore::snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                              cache::UploadJobId jobId) {
  co_return co_await loadJob(txn, jobId, true);
}

CoTryTask<std::optional<cache::UploadJobRecord>> UploadJobStore::load(kv::IReadWriteTransaction &txn,
                                                                      cache::UploadJobId jobId) {
  co_return co_await loadJob(txn, jobId, false);
}

CoTryTask<UploadJobPage> UploadJobStore::snapshotList(kv::IReadOnlyTransaction &txn,
                                                      std::optional<flat::Uid> ownerUid,
                                                      std::optional<cache::UploadJobId> after,
                                                      uint32_t limit,
                                                      bool includeTerminal) {
  if (limit == 0 || limit > cache::kMaxPhase2BatchItems || (after && *after == cache::UploadJobId{})) {
    co_return makeError(StatusCode::kInvalidArg, "invalid upload job page");
  }
  auto prefix = includeTerminal ? UploadJobKey::prefix() : UploadJobKey::activePrefix();
  auto beginKey = after ? (includeTerminal ? UploadJobKey::job(*after) : UploadJobKey::active(*after)) : prefix;
  auto endKey = kv::TransactionHelper::prefixListEndKey(prefix);
  bool inclusive = !after.has_value();
  UploadJobPage page;
  while (page.jobs.size() <= limit) {
    auto values = co_await txn.snapshotGetRange({beginKey, inclusive}, {endKey, false}, kListScanBatch);
    CO_RETURN_ON_ERROR(values);
    for (const auto &value : values->kvs) {
      auto jobId = includeTerminal ? UploadJobKey::unpack(value.key) : UploadJobKey::unpackActive(value.key);
      CO_RETURN_ON_ERROR(jobId);
      auto job = decode(UploadJobKey::job(*jobId), value.value);
      CO_RETURN_ON_ERROR(job);
      if ((!ownerUid || job->ownerUid == *ownerUid) && (includeTerminal || !terminal(*job))) {
        page.jobs.push_back(std::move(*job));
      }
      if (page.jobs.size() > limit) break;
    }
    if (page.jobs.size() > limit || !values->hasMore) break;
    if (values->kvs.empty()) co_return makeError(StatusCode::kDataCorruption, "upload job range did not advance");
    beginKey = values->kvs.back().key;
    inclusive = false;
  }
  page.more = page.jobs.size() > limit;
  if (page.more) page.jobs.resize(limit);
  co_return page;
}

CoTryTask<cache::UploadJobRecord> UploadJobStore::update(kv::IReadWriteTransaction &txn,
                                                         uint64_t expectedStateVersion,
                                                         const cache::UploadJobRecord &job) {
  CO_RETURN_ON_ERROR(job.valid());
  if (expectedStateVersion == 0 || expectedStateVersion == std::numeric_limits<uint64_t>::max() ||
      job.stateVersion != expectedStateVersion + 1) {
    co_return makeError(StatusCode::kInvalidArg, "invalid upload job state version transition");
  }
  auto existing = co_await load(txn, job.jobId);
  CO_RETURN_ON_ERROR(existing);
  if (!existing->has_value()) co_return makeError(CacheCode::kNotFound, "upload job not found");
  const auto &current = **existing;
  if (current.stateVersion != expectedStateVersion || !sameSpec(current, job)) {
    co_return makeError(CacheCode::kStateConflict, "upload job update fence changed");
  }
  if (!validTransition(current.state, job.state)) {
    co_return makeError(CacheCode::kStateConflict, "invalid upload job state transition");
  }
  if (!validCleanupTransition(current, job)) {
    co_return makeError(CacheCode::kStateConflict, "invalid orphan cleanup state transition");
  }
  if (job.updatedAtMs < current.updatedAtMs || job.parts.size() < current.parts.size() ||
      !std::equal(current.parts.begin(), current.parts.end(), job.parts.begin()) ||
      (!current.multipartId.empty() && current.multipartId != job.multipartId) ||
      (current.completedObject && current.completedObject != job.completedObject) ||
      job.orphanCleanupAttempts < current.orphanCleanupAttempts ||
      (current.orphanCleanupEligibleAtMs != 0 && job.orphanCleanupState != cache::OrphanCleanupState::NONE &&
       current.orphanCleanupEligibleAtMs != job.orphanCleanupEligibleAtMs) ||
      (current.orphanCleanupOperationId != Uuid::zero() &&
       current.orphanCleanupOperationId != job.orphanCleanupOperationId) ||
      (current.orphanCleanupState == cache::OrphanCleanupState::COMPLETE &&
       job.orphanCleanupState != cache::OrphanCleanupState::COMPLETE) ||
      (current.orphanCleanupState == cache::OrphanCleanupState::CONFLICT &&
       job.orphanCleanupState != cache::OrphanCleanupState::CONFLICT) ||
      (current.publishedInode != 0 && current.publishedInode != job.publishedInode) ||
      (current.state == cache::UploadJobState::OPEN && job.state == cache::UploadJobState::OPEN &&
       (job.stagingLength != current.stagingLength || job.writerLeaseId != current.writerLeaseId ||
        job.writerLeaseExpiresAtMs < current.writerLeaseExpiresAtMs)) ||
      (current.state == cache::UploadJobState::OPEN && job.stagingLength < current.stagingLength) ||
      (current.state != cache::UploadJobState::OPEN && current.stagingLength != job.stagingLength)) {
    co_return makeError(CacheCode::kStateConflict, "upload job durable progress moved backwards");
  }
  auto value = encode(job);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::job(job.jobId), *value));
  if (current.state != job.state) {
    CO_RETURN_ON_ERROR(co_await txn.clear(UploadJobKey::state(current.state, current.jobId)));
  }
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::state(job.state, job.jobId), *value));
  if (current.state == cache::UploadJobState::OPEN) {
    CO_RETURN_ON_ERROR(co_await txn.clear(UploadJobKey::openLease(current.writerLeaseExpiresAtMs, current.jobId)));
  }
  if (job.state == cache::UploadJobState::OPEN) {
    CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::openLease(job.writerLeaseExpiresAtMs, job.jobId), *value));
  }
  if (terminal(job)) {
    CO_RETURN_ON_ERROR(co_await txn.clear(UploadJobKey::active(job.jobId)));
  } else {
    CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::active(job.jobId), *value));
  }
  co_return job;
}

CoTryTask<void> UploadJobStore::removeActive(kv::IReadWriteTransaction &txn, cache::UploadJobId jobId) {
  if (jobId == cache::UploadJobId{}) co_return makeError(StatusCode::kInvalidArg, "upload job id not set");
  co_return co_await txn.clear(UploadJobKey::active(jobId));
}

CoTryTask<void> UploadJobStore::indexActive(kv::IReadWriteTransaction &txn, const cache::UploadJobRecord &job) {
  CO_RETURN_ON_ERROR(job.valid());
  auto value = encode(job);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::state(job.state, job.jobId), *value));
  if (terminal(job)) co_return Void{};
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::active(job.jobId), *value));
  if (job.state == cache::UploadJobState::OPEN) {
    CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::openLease(job.writerLeaseExpiresAtMs, job.jobId), *value));
  }
  co_return Void{};
}

CoTryTask<void> UploadJobStore::finishStateIndexMigration(kv::IReadWriteTransaction &txn) {
  co_return co_await txn.set(UploadJobKey::stateIndexMarker(), "1");
}

}  // namespace hf3fs::meta::server
