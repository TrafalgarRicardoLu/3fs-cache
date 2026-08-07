#include "cache_manager/upload/WritePublishController.h"

#include <algorithm>
#include <deque>
#include <folly/ScopeGuard.h>
#include <folly/experimental/coro/Collect.h>
#include <utility>

#include "client/meta/MetaClient.h"

namespace hf3fs::cache_manager {

Result<Void> WritePublishControllerConfig::valid() const {
  if (pageSize == 0 || pageSize > cache::kMaxPhase2BatchItems || globalConcurrency == 0 || perOwnerConcurrency == 0 ||
      perOriginConcurrency == 0 || !cacheTableId || cacheBlockSize == 0 || cacheStripeSize == 0) {
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
                                                                 uint32_t limit) {
  if (stopping_.load(std::memory_order_acquire)) {
    co_return makeError(MetaCode::kRequestCanceled, "write publish controller stopped");
  }
  if (!metaClient_) co_return makeError(StatusCode::kInvalidConfig, "upload metadata client is missing");
  meta::ListUploadJobsReq request;
  request.service = service_;
  request.includeTerminal = false;
  request.after = after;
  request.limit = limit;
  request.cacheProtocolVersion = cache::kCachePhase4ProtocolVersion;
  auto response = co_await metaClient_->listUploadJobs(std::move(request));
  CO_RETURN_ON_ERROR(response);
  co_return UploadJobPage{std::move(response->jobs), response->more};
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
  co_return job;
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

bool WritePublishController::actionable(cache::UploadJobState state) {
  return state == cache::UploadJobState::SEALED || state == cache::UploadJobState::UPLOADING ||
         state == cache::UploadJobState::COMPLETING || state == cache::UploadJobState::PUBLISHING ||
         state == cache::UploadJobState::ABORTING;
}

bool WritePublishController::terminal(cache::UploadJobState state) {
  return state == cache::UploadJobState::PUBLISHED || state == cache::UploadJobState::FAILED ||
         state == cache::UploadJobState::CANCELLED;
}

CoTryTask<std::vector<cache::UploadJobRecord>> WritePublishController::scan() {
  std::vector<cache::UploadJobRecord> jobs;
  std::optional<cache::UploadJobId> after;
  while (true) {
    if (stopping_.load(std::memory_order_acquire)) co_return jobs;
    auto page = co_await backend_->list(after, config_.pageSize);
    CO_RETURN_ON_ERROR(page);
    if (page->jobs.size() > config_.pageSize || (page->more && page->jobs.empty())) {
      co_return makeError(CacheCode::kInvalidResponse, "invalid upload recovery page");
    }
    for (auto &job : page->jobs) {
      CO_RETURN_ON_ERROR(job.valid());
      jobs.push_back(std::move(job));
    }
    if (!page->more) break;
    auto next = jobs.back().jobId;
    if (after && next == *after)
      co_return makeError(CacheCode::kInvalidResponse, "upload recovery page did not advance");
    after = next;
  }
  co_return jobs;
}

std::vector<cache::UploadJobRecord> WritePublishController::select(std::vector<cache::UploadJobRecord> jobs) {
  std::map<flat::Uid, std::deque<cache::UploadJobRecord>> byOwner;
  for (auto &job : jobs) {
    if (actionable(job.state)) byOwner[job.ownerUid].push_back(std::move(job));
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
  co_return job;
}

CoTryTask<WritePublishRunResult> WritePublishController::runOnce() {
  CO_RETURN_ON_ERROR(config_.valid());
  if (!backend_) co_return makeError(StatusCode::kInvalidConfig, "write publish controller backend is missing");
  WritePublishRunResult result;
  if (stopping_.load(std::memory_order_acquire)) {
    result.stopped = true;
    co_return result;
  }
  auto scanned = co_await scan();
  CO_RETURN_ON_ERROR(scanned);
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
  co_return result;
}

CoTryTask<WritePublishRunResult> WritePublishController::recover() { co_return co_await runOnce(); }

void WritePublishController::stop() {
  if (!stopping_.exchange(true, std::memory_order_acq_rel) && backend_) backend_->stop();
}

}  // namespace hf3fs::cache_manager
