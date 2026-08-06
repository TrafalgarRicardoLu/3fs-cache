#include "cache_manager/job/JobTracker.h"

#include <limits>

namespace hf3fs::cache_manager {
namespace {

Result<Void> add(uint64_t &value, uint64_t increment, std::string_view name) {
  if (increment > std::numeric_limits<uint64_t>::max() - value) {
    return makeError(CacheCode::kStateConflict, std::string(name) + " overflow");
  }
  value += increment;
  return Void{};
}

}  // namespace

CoTryTask<meta::TrackPrefetchReadyRsp> MetaJobTrackerBackend::track(cache::PrefetchJobId jobId,
                                                                    std::optional<cache::CacheBlockKey> after,
                                                                    uint32_t limit) {
  meta::TrackPrefetchReadyReq request;
  request.service = service_;
  request.jobId = jobId;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  co_return co_await metaClient_->trackPrefetchReady(std::move(request));
}

CoTryTask<meta::AdvancePrefetchJobStateRsp> MetaJobTrackerBackend::advance(cache::PrefetchJobId jobId,
                                                                           uint64_t expectedStateVersion) {
  meta::AdvancePrefetchJobStateReq request;
  request.service = service_;
  request.jobId = jobId;
  request.expectedStateVersion = expectedStateVersion;
  request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  co_return co_await metaClient_->advancePrefetchJobState(std::move(request));
}

CoTryTask<JobTrackerResult> JobTracker::run(const cache::PrefetchJobRecord &job,
                                            const CancellationToken &cancellation) {
  CO_RETURN_ON_ERROR(job.valid());
  if (pageLimit_ == 0 || pageLimit_ > meta::kMaxCacheBatchItems) {
    co_return makeError(StatusCode::kInvalidConfig, "invalid JobTracker page limit");
  }
  JobTrackerResult result;
  result.job = job;
  std::optional<cache::CacheBlockKey> after;
  do {
    if (cancellation.isCancellationRequested()) throw OperationCancelled();
    auto page = co_await backend_->track(job.spec.jobId, after, pageLimit_);
    CO_RETURN_ON_ERROR(page);
    CO_RETURN_ON_ERROR(page->job.valid());
    if (page->job.spec.jobId != job.spec.jobId || page->job.readyBytes < result.job.readyBytes ||
        page->job.readyBlocks < result.job.readyBlocks) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid JobTracker monotonic counters");
    }
    result.job = std::move(page->job);
    CO_RETURN_ON_ERROR(add(result.currentReadyBytes, page->currentReadyBytes, "current ready bytes"));
    CO_RETURN_ON_ERROR(add(result.currentReadyBlocks, page->currentReadyBlocks, "current ready blocks"));
    if (!page->more) break;
    if (!page->nextAfter || (after && *page->nextAfter == *after)) {
      co_return makeError(CacheCode::kInvalidResponse, "JobTracker page did not advance");
    }
    after = page->nextAfter;
  } while (true);
  if (result.job.planningComplete) {
    auto advanced = co_await backend_->advance(job.spec.jobId, result.job.stateVersion);
    CO_RETURN_ON_ERROR(advanced);
    CO_RETURN_ON_ERROR(advanced->job.valid());
    if (advanced->job.spec.jobId != job.spec.jobId || advanced->job.readyBytes < result.job.readyBytes ||
        advanced->job.readyBlocks < result.job.readyBlocks) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid Job state transition response");
    }
    result.job = std::move(advanced->job);
    result.achievedReadyBps = advanced->achievedReadyBps;
  }
  co_return result;
}

}  // namespace hf3fs::cache_manager
