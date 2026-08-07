#include "meta/store/cache/UploadJobStore.h"

#include <algorithm>
#include <limits>

#include "common/serde/Serde.h"
#include "meta/store/cache/UploadJobKey.h"

namespace hf3fs::meta::server {
namespace {

constexpr size_t kMaxUploadJobValueBytes = 96U << 10;
constexpr int32_t kListScanBatch = 256;

bool terminal(cache::UploadJobState state) {
  return state == cache::UploadJobState::PUBLISHED || state == cache::UploadJobState::FAILED ||
         state == cache::UploadJobState::CANCELLED;
}

bool sameSpec(const cache::UploadJobRecord &left, const cache::UploadJobRecord &right) {
  return left.jobId == right.jobId && left.ownerUid == right.ownerUid && left.path == right.path &&
         left.stagingInode == right.stagingInode && left.destination == right.destination &&
         left.createdAtMs == right.createdAtMs;
}

bool validTransition(cache::UploadJobState from, cache::UploadJobState to) {
  if (from == to) return !terminal(from);
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
    default:
      return false;
  }
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
    co_return CreateUploadJobResult{std::move(**existing), false};
  }
  auto value = encode(job);
  CO_RETURN_ON_ERROR(value);
  CO_RETURN_ON_ERROR(co_await txn.set(UploadJobKey::job(job.jobId), *value));
  co_return CreateUploadJobResult{job, true};
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
  auto prefix = UploadJobKey::prefix();
  auto beginKey = after ? UploadJobKey::job(*after) : prefix;
  auto endKey = kv::TransactionHelper::prefixListEndKey(prefix);
  bool inclusive = !after.has_value();
  UploadJobPage page;
  while (page.jobs.size() <= limit) {
    auto values = co_await txn.snapshotGetRange({beginKey, inclusive}, {endKey, false}, kListScanBatch);
    CO_RETURN_ON_ERROR(values);
    for (const auto &value : values->kvs) {
      auto job = decode(value.key, value.value);
      CO_RETURN_ON_ERROR(job);
      if ((!ownerUid || job->ownerUid == *ownerUid) && (includeTerminal || !terminal(job->state))) {
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
  if (job.updatedAtMs < current.updatedAtMs || job.parts.size() < current.parts.size() ||
      !std::equal(current.parts.begin(), current.parts.end(), job.parts.begin()) ||
      (!current.multipartId.empty() && current.multipartId != job.multipartId) ||
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
  co_return job;
}

}  // namespace hf3fs::meta::server
