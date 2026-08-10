#include "cache_manager/upload/WritePublishController.h"

#include <algorithm>
#include <deque>
#include <folly/ScopeGuard.h>
#include <folly/experimental/coro/Collect.h>
#include <utility>

#include "cache/metrics/CacheMetrics.h"
#include "client/meta/MetaClient.h"
#include "common/utils/UtcTime.h"

namespace hf3fs::cache_manager {
Result<Void> WritePublishControllerConfig::valid() const {
  if (pageSize == 0 || pageSize > cache::kMaxPhase2BatchItems || globalConcurrency == 0 ||
      globalConcurrency > cache::kMaxPhase2BatchItems || perOwnerConcurrency == 0 ||
      perOwnerConcurrency > globalConcurrency || perOriginConcurrency == 0 || perOriginConcurrency > globalConcurrency ||
      !cacheTableId || cacheBlockSize == 0 || cacheStripeSize == 0 ||
      publishedPrefetchPriority > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return makeError(StatusCode::kInvalidConfig, "invalid write publish controller limits or layout");
  }
  RETURN_ON_ERROR(uploader.valid());
  return finalizer.valid();
}

RealWritePublishControllerBackend::RealWritePublishControllerBackend(
    std::shared_ptr<meta::client::MetaClient> metaClient,
    std::shared_ptr<storage::client::StorageClient> storageClient,
    std::shared_ptr<client::ICommonMgmtdClient> mgmtdClient,
    std::shared_ptr<cache::origin::ObjectStore> objectStore,
    meta::CacheServiceIdentity service,
    WritePublishControllerConfig config)
    : metaClient_(std::move(metaClient)),
      storageClient_(std::move(storageClient)),
      mgmtdClient_(std::move(mgmtdClient)),
      objectStore_(std::move(objectStore)),
      service_(std::move(service)),
      config_(config) {}

CoTryTask<UploadJobPage> RealWritePublishControllerBackend::list(std::optional<cache::UploadJobId> after,
                                                                 uint32_t limit,
                                                                 bool includeTerminal) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "upload metadata client is missing");
  meta::ListUploadJobsReq request;
  request.service = service_;
  request.includeTerminal = includeTerminal;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->listUploadJobs(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return UploadJobPage{std::move(response->jobs), response->more};
}

CoTryTask<UploadJobPage> RealWritePublishControllerBackend::listExpiredOpen(
    std::optional<meta::UploadOpenLeaseCursor> after,
    uint64_t expiresBeforeMs,
    uint32_t limit) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  meta::ListExpiredOpenUploadsReq request;
  request.service = service_;
  request.expiresBeforeMs = expiresBeforeMs;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->listExpiredOpenUploads(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return UploadJobPage{std::move(response->jobs), response->more};
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::recoverOpen(cache::UploadJobRecord job) {
  meta::RecoverExpiredOpenUploadReq request;
  request.service = service_;
  request.jobId = job.jobId;
  request.expectedStateVersion = job.stateVersion;
  request.expectedWriterLeaseId = job.writerLeaseId;
  request.expectedWriterLeaseExpiresAtMs = job.writerLeaseExpiresAtMs;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->recoverExpiredOpenUpload(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->job);
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::finalizeCancelled(
    cache::UploadJobRecord job) {
  meta::FinalizeCancelledUploadReq request;
  request.service = service_;
  request.jobId = job.jobId;
  request.expectedStateVersion = job.stateVersion;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->finalizeCancelledUpload(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return std::move(response->job);
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::upload(cache::UploadJobRecord job) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  auto backend = std::make_shared<RealMultipartUploaderBackend>(metaClient_,
                                                                storageClient_,
                                                                mgmtdClient_,
                                                                objectStore_,
                                                                service_.name,
                                                                service_.token,
                                                                flat::UserInfo{job.ownerUid, flat::Gid{}});
  auto key = job.jobId.toUnderType();
  {
    auto lock = std::scoped_lock(activeMutex_);
    uploaders_[key] = backend;
    if (stopping_.load(std::memory_order_acquire)) backend->cancel();
  }
  auto cleanup = folly::makeGuard([&] {
    auto lock = std::scoped_lock(activeMutex_);
    uploaders_.erase(key);
  });
  MultipartUploader uploader(std::move(backend), config_.uploader);
  co_return co_await uploader.upload(std::move(job));
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::complete(cache::UploadJobRecord job) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  auto backend =
      std::make_shared<RealMultipartUploadFinalizerBackend>(metaClient_, objectStore_, service_.name, service_.token);
  auto key = job.jobId.toUnderType();
  {
    auto lock = std::scoped_lock(activeMutex_);
    finalizers_[key] = backend;
    if (stopping_.load(std::memory_order_acquire)) backend->cancel();
  }
  auto cleanup = folly::makeGuard([&] {
    auto lock = std::scoped_lock(activeMutex_);
    finalizers_.erase(key);
  });
  MultipartUploadFinalizer finalizer(std::move(backend), config_.finalizer);
  co_return co_await finalizer.complete(std::move(job));
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::publish(cache::UploadJobRecord job) {
  bool succeeded = false;
  auto publishMetric = folly::makeGuard([&] {
    cache::metrics::recordCount(
        cache::metrics::Event::MANAGER_PUBLISH_RESULT,
        1,
        {.originId = job.destination.originId.toUnderType(), .reason = succeeded ? "published" : "failed"});
  });
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  if (!metaClient_ || !job.completedObject) {
    co_return makeError(StatusCode::kInvalidConfig, "upload publish metadata is missing");
  }
  flat::UserInfo user{job.ownerUid, flat::Gid{}};
  auto staging = co_await metaClient_->stat(user, meta::InodeId{job.stagingInode}, std::nullopt, false);
  CO_RETURN_ON_ERROR(staging);
  if (!staging->isFile() || staging->id.u64() != job.stagingInode) {
    co_return makeError(CacheCode::kStateConflict, "staging inode changed before publish");
  }
  user.gid = staging->acl.gid;
  meta::PublishOriginFileFromStagingReq request;
  request.user = std::move(user);
  request.service = service_;
  request.jobId = job.jobId;
  request.expectedStateVersion = job.stateVersion;
  request.expectedStagingInode = staging->id;
  request.metadata.object = *job.completedObject;
  request.metadata.objectSize = job.stagingLength;
  request.metadata.tableId = config_.cacheTableId;
  request.metadata.blockSize = config_.cacheBlockSize;
  request.metadata.stripeSize = config_.cacheStripeSize;
  request.metadata.permission = staging->acl.perm;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto published = co_await metaClient_->publishOriginFileFromStaging(std::move(request));
  CO_RETURN_ON_ERROR(published);
  job.state = cache::UploadJobState::PUBLISHED;
  ++job.stateVersion;
  job.publishedInode = published->inode.id.u64();
  succeeded = true;
  co_return job;
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::warm(cache::UploadJobRecord job) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  if (!metaClient_ || job.state != cache::UploadJobState::PUBLISHED || job.publishedInode == 0) {
    co_return makeError(StatusCode::kInvalidConfig, "published upload warming metadata is missing");
  }
  auto prefetchId = cache::publishedPrefetchJobId(job.jobId);
  auto nowMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds() / 1000);
  if (nowMs == 0) co_return makeError(StatusCode::kInvalidArg, "cache manager clock is invalid");
  cache::PrefetchJobRecord prefetch;
  prefetch.spec.jobId = prefetchId;
  prefetch.spec.ownerUid = job.ownerUid;
  cache::DatasetSource source;
  source.source = cache::NamespacePathSource{job.path, false};
  prefetch.spec.sources = {std::move(source)};
  prefetch.spec.priority = config_.publishedPrefetchPriority;
  prefetch.state = cache::PrefetchJobState::PENDING;
  prefetch.stateVersion = 1;
  prefetch.createdAtMs = prefetch.updatedAtMs = nowMs;
  meta::CreatePrefetchJobReq create;
  create.service = service_;
  create.job = std::move(prefetch);
  create.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
  CO_RETURN_ON_ERROR(co_await metaClient_->createPrefetchJob(std::move(create)));
  meta::MutateMultipartUploadReq mark;
  mark.service = service_;
  mark.jobId = job.jobId;
  mark.expectedStateVersion = job.stateVersion;
  mark.multipartId = job.multipartId;
  mark.mutation = meta::MultipartUploadMutation::MARK_PREFETCH_SUBMITTED;
  mark.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto marked = co_await metaClient_->mutateMultipartUpload(std::move(mark));
  CO_RETURN_ON_ERROR(marked);
  co_return std::move(marked->job);
}

CoTryTask<cache::UploadJobRecord> RealWritePublishControllerBackend::abort(cache::UploadJobRecord job) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  auto backend =
      std::make_shared<RealMultipartUploadFinalizerBackend>(metaClient_, objectStore_, service_.name, service_.token);
  auto key = job.jobId.toUnderType();
  {
    auto lock = std::scoped_lock(activeMutex_);
    finalizers_[key] = backend;
    if (stopping_.load(std::memory_order_acquire)) backend->cancel();
  }
  auto cleanup = folly::makeGuard([&] {
    auto lock = std::scoped_lock(activeMutex_);
    finalizers_.erase(key);
  });
  MultipartUploadFinalizer finalizer(std::move(backend), config_.finalizer);
  co_return co_await finalizer.cancel(std::move(job), "upload cancellation resumed");
}

void RealWritePublishControllerBackend::stop() {
  if (stopping_.exchange(true, std::memory_order_acq_rel)) return;
  auto lock = std::scoped_lock(activeMutex_);
  for (const auto &[_, backend] : uploaders_) backend->cancel();
  for (const auto &[_, backend] : finalizers_) backend->cancel();
}

WritePublishController::WritePublishController(std::shared_ptr<WritePublishControllerBackend> backend,
                                               WritePublishControllerConfig config)
    : backend_(std::move(backend)),
      config_(config) {}

WritePublishController::~WritePublishController() { stop(); }

bool WritePublishController::actionable(const cache::UploadJobRecord &job) {
  return job.state == cache::UploadJobState::OPEN || job.state == cache::UploadJobState::SEALED ||
         job.state == cache::UploadJobState::UPLOADING ||
         job.state == cache::UploadJobState::COMPLETING || job.state == cache::UploadJobState::PUBLISHING ||
         job.state == cache::UploadJobState::ABORTING || job.state == cache::UploadJobState::PUBLISHED;
}

CoTryTask<std::vector<cache::UploadJobRecord>> WritePublishController::scanExpiredOpen() {
  std::vector<cache::UploadJobRecord> jobs;
  std::optional<meta::UploadOpenLeaseCursor> after;
  auto nowUs = UtcClock::now().toMicroseconds();
  if (nowUs <= 0) co_return makeError(StatusCode::kDataCorruption, "cache manager clock is invalid");
  auto expiresBeforeMs = static_cast<uint64_t>(nowUs / 1000);
  while (true) {
    auto page = co_await backend_->listExpiredOpen(after, expiresBeforeMs, config_.pageSize);
    CO_RETURN_ON_ERROR(page);
    if (page->jobs.size() > config_.pageSize || (page->more && page->jobs.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid expired open upload page");
    }
    for (auto &job : page->jobs) {
      CO_RETURN_ON_ERROR(job.valid());
      if (job.state != cache::UploadJobState::OPEN || job.writerLeaseExpiresAtMs > expiresBeforeMs) {
        co_return makeError(CacheCode::kInvalidResponse, "invalid expired open upload record");
      }
      jobs.push_back(std::move(job));
    }
    if (!page->more) break;
    const auto &last = jobs.back();
    after = meta::UploadOpenLeaseCursor{last.writerLeaseExpiresAtMs, last.jobId};
  }
  co_return jobs;
}

bool WritePublishController::terminal(cache::UploadJobState state) {
  return state == cache::UploadJobState::PUBLISHED || state == cache::UploadJobState::FAILED ||
         state == cache::UploadJobState::CANCELLED;
}

CoTryTask<std::vector<cache::UploadJobRecord>> WritePublishController::scan(bool includeTerminal) {
  std::vector<cache::UploadJobRecord> jobs;
  auto after = includeTerminal ? std::optional<cache::UploadJobId>{} : activeScanAfter_;
  auto window = static_cast<size_t>(config_.pageSize) +
                static_cast<size_t>(config_.globalConcurrency) * config_.globalConcurrency;
  while (true) {
    if (stopping_.load(std::memory_order_acquire)) co_return jobs;
    auto page = co_await backend_->list(after, config_.pageSize, includeTerminal);
    CO_RETURN_ON_ERROR(page);
    if (page->jobs.size() > config_.pageSize || (page->more && page->jobs.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid upload recovery page");
    }
    for (auto &job : page->jobs) {
      CO_RETURN_ON_ERROR(job.valid());
      jobs.push_back(std::move(job));
      if (!includeTerminal && jobs.size() >= window) {
        activeScanAfter_ = jobs.back().jobId;
        co_return jobs;
      }
    }
    if (!page->more) {
      if (!includeTerminal) activeScanAfter_.reset();
      break;
    }
    auto next = jobs.back().jobId;
    if (after && next == *after)
      co_return makeError(CacheCode::kInvalidResponse, "upload recovery page did not advance");
    after = next;
  }
  co_return jobs;
}

CoTryTask<void> WritePublishController::migrateActiveIndex() {
  std::optional<cache::UploadJobId> after;
  while (true) {
    if (stopping_.load(std::memory_order_acquire)) co_return Void{};
    auto page = co_await backend_->list(after, config_.pageSize, true);
    CO_RETURN_ON_ERROR(page);
    if (page->jobs.size() > config_.pageSize || (page->more && page->jobs.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid upload migration page");
    }
    auto next = page->jobs.empty() ? std::optional<cache::UploadJobId>{}
                                   : std::optional<cache::UploadJobId>{page->jobs.back().jobId};
    for (auto &job : page->jobs) {
      if (job.state == cache::UploadJobState::CANCELLED) {
        CO_RETURN_ON_ERROR(co_await backend_->finalizeCancelled(std::move(job)));
      }
    }
    if (!page->more) co_return Void{};
    if (!next || (after && *next == *after)) {
      co_return makeError(CacheCode::kInvalidResponse, "upload migration page did not advance");
    }
    after = *next;
  }
}

std::vector<cache::UploadJobRecord> WritePublishController::select(std::vector<cache::UploadJobRecord> jobs) {
  std::stable_partition(jobs.begin(), jobs.end(), [](const auto &job) {
    return job.state != cache::UploadJobState::PUBLISHED;
  });
  std::map<flat::Uid, std::deque<cache::UploadJobRecord>> byOwner;
  for (auto &job : jobs) {
    if (actionable(job)) byOwner[job.ownerUid].push_back(std::move(job));
  }
  std::vector<flat::Uid> owners;
  owners.reserve(byOwner.size());
  for (const auto &[owner, _] : byOwner) owners.push_back(owner);
  if (!owners.empty()) {
    roundRobinOffset_ %= owners.size();
    std::rotate(owners.begin(), owners.begin() + roundRobinOffset_, owners.end());
  }

  std::map<flat::Uid, uint32_t> ownerUsage;
  std::map<cache::OriginId, uint32_t> originUsage;
  std::vector<cache::UploadJobRecord> selected;
  bool progressed = true;
  while (progressed && selected.size() < config_.globalConcurrency) {
    progressed = false;
    for (auto owner : owners) {
      auto &queue = byOwner[owner];
      if (queue.empty() || ownerUsage[owner] >= config_.perOwnerConcurrency) continue;
      auto eligible = std::find_if(queue.begin(), queue.end(), [&](const auto &job) {
        return originUsage[job.destination.originId] < config_.perOriginConcurrency;
      });
      if (eligible == queue.end()) continue;
      auto origin = eligible->destination.originId;
      selected.push_back(std::move(*eligible));
      queue.erase(eligible);
      ++ownerUsage[owner];
      ++originUsage[origin];
      progressed = true;
      if (selected.size() == config_.globalConcurrency) break;
    }
  }
  if (!owners.empty()) roundRobinOffset_ = (roundRobinOffset_ + 1) % owners.size();
  return selected;
}

CoTryTask<cache::UploadJobRecord> WritePublishController::advance(cache::UploadJobRecord job) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  if (job.state == cache::UploadJobState::OPEN) {
    auto recovered = co_await backend_->recoverOpen(std::move(job));
    CO_RETURN_ON_ERROR(recovered);
    job = std::move(*recovered);
  }
  if (job.state == cache::UploadJobState::SEALED || job.state == cache::UploadJobState::UPLOADING) {
    auto uploaded = co_await backend_->upload(std::move(job));
    CO_RETURN_ON_ERROR(uploaded);
    job = std::move(*uploaded);
  }
  if (stopping_.load(std::memory_order_acquire)) co_return job;
  if (job.state == cache::UploadJobState::UPLOADING || job.state == cache::UploadJobState::COMPLETING) {
    auto completed = co_await backend_->complete(std::move(job));
    CO_RETURN_ON_ERROR(completed);
    job = std::move(*completed);
  }
  if (stopping_.load(std::memory_order_acquire)) co_return job;
  if (job.state == cache::UploadJobState::PUBLISHING) {
    auto published = co_await backend_->publish(std::move(job));
    CO_RETURN_ON_ERROR(published);
    job = std::move(*published);
  } else if (job.state == cache::UploadJobState::ABORTING) {
    auto aborted = co_await backend_->abort(std::move(job));
    CO_RETURN_ON_ERROR(aborted);
    job = std::move(*aborted);
  }
  if (!stopping_.load(std::memory_order_acquire) && job.state == cache::UploadJobState::PUBLISHED) {
    auto warmed = co_await backend_->warm(std::move(job));
    CO_RETURN_ON_ERROR(warmed);
    job = std::move(*warmed);
  }
  co_return job;
}

CoTryTask<WritePublishRunResult> WritePublishController::run(bool includeTerminal) {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_) co_return makeError(StatusCode::kInvalidConfig, "write publish controller backend is missing");
  WritePublishRunResult result;
  if (stopping_.load(std::memory_order_acquire)) {
    result.stopped = true;
    co_return result;
  }
  auto scanned = co_await scan(includeTerminal);
  CO_RETURN_ON_ERROR(scanned);
  std::erase_if(*scanned, [](const auto &job) { return job.state == cache::UploadJobState::OPEN; });
  auto expiredOpen = co_await scanExpiredOpen();
  CO_RETURN_ON_ERROR(expiredOpen);
  scanned->insert(scanned->end(),
                  std::make_move_iterator(expiredOpen->begin()),
                  std::make_move_iterator(expiredOpen->end()));
  result.scanned = static_cast<uint32_t>(scanned->size());
  auto selected = select(std::move(*scanned));
  result.scheduled = static_cast<uint32_t>(selected.size());
  std::vector<CoTryTask<cache::UploadJobRecord>> tasks;
  tasks.reserve(selected.size());
  for (auto &job : selected) tasks.push_back(advance(std::move(job)));
  auto advanced = co_await folly::coro::collectAllRange(std::move(tasks));
  for (auto &job : advanced) {
    if (job.hasError()) {
      if (job.error().code() != MetaCode::kRequestCanceled) ++result.failed;
      continue;
    }
    if (terminal(job->state)) ++result.completed;
  }
  result.stopped = stopping_.load(std::memory_order_acquire);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_UPLOAD_RUN,
                              1,
                              {.reason = result.stopped  ? "stopped"
                                         : result.failed ? "degraded"
                                                         : "complete"});
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_UPLOAD_SCANNED, result.scanned);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_UPLOAD_SCHEDULED, result.scheduled);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_UPLOAD_COMPLETED, result.completed);
  cache::metrics::recordCount(cache::metrics::Event::MANAGER_UPLOAD_FAILED, result.failed);
  co_return result;
}

CoTryTask<WritePublishRunResult> WritePublishController::runOnce() { co_return co_await run(false); }

CoTryTask<WritePublishRunResult> WritePublishController::recover() {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_) co_return makeError(StatusCode::kInvalidConfig, "write publish controller backend is missing");
  CO_RETURN_ON_ERROR(co_await migrateActiveIndex());
  co_return co_await run(false);
}

void WritePublishController::stop() {
  if (!stopping_.exchange(true, std::memory_order_acq_rel) && backend_) backend_->stop();
}

}  // namespace hf3fs::cache_manager
