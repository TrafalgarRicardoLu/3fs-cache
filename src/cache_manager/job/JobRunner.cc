#include "cache_manager/job/JobRunner.h"

#include <tuple>
#include <utility>

namespace hf3fs::cache_manager {

JobRunner::JobRunner(std::shared_ptr<JobRunnerBackend> backend,
                     JobQuota &quota,
                     uint32_t pageLimit,
                     AttemptFactory attemptFactory)
    : backend_(std::move(backend)),
      quota_(quota),
      pageLimit_(pageLimit),
      attemptFactory_(std::move(attemptFactory)) {}

bool JobRunner::ClaimKeyLess::operator()(const ClaimKey &lhs, const ClaimKey &rhs) const {
  if (lhs.jobId != rhs.jobId) return lhs.jobId.toUnderType() < rhs.jobId.toUnderType();
  if (lhs.block.inode != rhs.block.inode) return lhs.block.inode < rhs.block.inode;
  return lhs.block.block.toUnderType() < rhs.block.block.toUnderType();
}

Result<std::shared_ptr<JobQuota::Permit>> JobRunner::acquire(const cache::PrefetchPlanEntry &entry,
                                                             const CancellationToken &cancellation) {
  ClaimKey key{entry.jobId, entry.key};
  {
    auto lock = std::scoped_lock(claims_->mutex);
    auto found = claims_->permits.find(key);
    if (found != claims_->permits.end()) return found->second;
  }
  auto acquired = quota_.tryAcquire(entry.jobId, entry.blockLength, cancellation);
  RETURN_ON_ERROR(acquired);
  auto permit = std::make_shared<JobQuota::Permit>(std::move(*acquired));
  auto lock = std::scoped_lock(claims_->mutex);
  auto [found, inserted] = claims_->permits.emplace(key, permit);
  return inserted ? permit : found->second;
}

void JobRunner::release(const cache::PrefetchPlanEntry &entry) {
  auto lock = std::scoped_lock(claims_->mutex);
  claims_->permits.erase(ClaimKey{entry.jobId, entry.key});
}

JobRunnerBackend::Completion JobRunner::completion(const cache::PrefetchPlanEntry &entry) {
  auto weak = std::weak_ptr<SharedClaims>(claims_);
  auto key = ClaimKey{entry.jobId, entry.key};
  return [weak, key](const Status &) {
    auto claims = weak.lock();
    if (!claims) return;
    auto lock = std::scoped_lock(claims->mutex);
    claims->permits.erase(key);
  };
}

CoTryTask<JobRunnerPageResult> JobRunner::runNextPage(const cache::PrefetchJobRecord &job,
                                                      std::optional<cache::CacheBlockKey> after,
                                                      const CancellationToken &cancellation) {
  CO_RETURN_ON_ERROR(job.valid());
  if (!job.spec.loadMissing) co_return JobRunnerPageResult{};
  if (pageLimit_ == 0 || pageLimit_ > cache::kMaxPhase2BatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid JobRunner page limit");
  }
  CO_RETURN_ON_ERROR(
      quota_.registerJob(job.spec.jobId, {job.spec.maxParallelLoads, job.spec.bandwidthLimitBytesPerSec}));
  auto page = co_await backend_->list(job.spec.jobId, after, pageLimit_);
  CO_RETURN_ON_ERROR(page);
  if (page->entries.size() > pageLimit_) co_return makeError(CacheCode::kInvalidResponse, "oversized plan page");

  JobRunnerPageResult result;
  result.more = page->more;
  for (auto current : page->entries) {
    if (cancellation.isCancellationRequested()) throw OperationCancelled();
    ++result.visited;
    if (current.jobId != job.spec.jobId) co_return makeError(CacheCode::kInvalidResponse, "foreign plan entry");
    CO_RETURN_ON_ERROR(current.valid());
    if (current.state == cache::PrefetchPlanEntryState::READY ||
        current.state == cache::PrefetchPlanEntryState::FAILED ||
        current.state == cache::PrefetchPlanEntryState::CANCELLED) {
      result.nextAfter = current.key;
      continue;
    }

    auto permit = acquire(current, cancellation);
    if (permit.hasError()) {
      if (permit.error().code() == CacheCode::kThrottled) {
        result.throttled = true;
        result.more = true;
        break;
      }
      co_return makeError(permit.error());
    }

    if (current.state == cache::PrefetchPlanEntryState::PLANNED) {
      auto desired = current;
      desired.state = cache::PrefetchPlanEntryState::ADMITTED;
      desired.admissionAttemptId = attemptFactory_();
      if (desired.admissionAttemptId == Uuid::zero()) {
        release(current);
        co_return makeError(StatusCode::kInvalidArg, "empty admission attempt identity");
      }
      auto persisted = co_await backend_->update(current, desired);
      if (persisted.hasError()) {
        release(current);
        co_return makeError(persisted.error());
      }
      current = std::move(*persisted);
    }

    auto admitted = co_await backend_->admit(current, completion(current));
    if (admitted.hasError()) {
      release(current);
      if (admitted.error().code() == RPCCode::kTimeout || admitted.error().code() == CacheCode::kTimeout ||
          admitted.error().code() == CacheCode::kUnavailable) {
        ++result.retryable;
        continue;
      }
      co_return makeError(admitted.error());
    }

    auto desired = current;
    switch (admitted->disposition) {
      case JobAdmissionDisposition::READY:
        if (!admitted->ready || admitted->ready->blockLength != current.blockLength) {
          release(current);
          co_return makeError(CacheCode::kInvalidResponse, "READY admission has no matching generation fence");
        }
        release(current);
        desired.state = cache::PrefetchPlanEntryState::READY;
        desired.ready = admitted->ready;
        ++result.ready;
        break;
      case JobAdmissionDisposition::QUEUED:
      case JobAdmissionDisposition::LOADING:
        desired.state = cache::PrefetchPlanEntryState::ATTACHED;
        ++result.attached;
        break;
      case JobAdmissionDisposition::CAPACITY_WAIT:
      case JobAdmissionDisposition::RETRYABLE:
        release(current);
        ++result.retryable;
        result.nextAfter = current.key;
        continue;
      case JobAdmissionDisposition::TERMINAL:
        release(current);
        desired.state = cache::PrefetchPlanEntryState::FAILED;
        ++result.failed;
        break;
    }
    if (desired != current) {
      auto persisted = co_await backend_->update(current, desired);
      if (persisted.hasError()) {
        release(current);
        co_return makeError(persisted.error());
      }
    }
    result.nextAfter = current.key;
  }
  co_return result;
}

}  // namespace hf3fs::cache_manager
