#include "storage/service/StorageOperator.h"

#include <boost/range/adaptor/reversed.hpp>
#include <fmt/format.h>

#include "cache/metrics/CacheMetrics.h"
#include "common/monitor/Recorder.h"
#include "common/net/RDMAControl.h"
#include "common/net/RequestOptions.h"
#include "common/utils/Duration.h"
#include "common/utils/Result.h"
#include "common/utils/SemaphoreGuard.h"
#include "storage/aio/BatchReadJob.h"
#include "storage/service/CachePermitCoordinator.h"
#include "storage/service/Components.h"
#include "storage/update/UpdateJob.h"

namespace hf3fs::storage {

monitor::OperationRecorder storageAioEnqueueRecorder{"storage.aio_enqueue"};
monitor::OperationRecorder storageWaitAioRecorder{"storage.wait_aio"};
monitor::OperationRecorder storageWaitSemRecorder{"storage.wait_sem"};
monitor::OperationRecorder storageWaitBatchRecorder{"storage.wait_batch"};
monitor::OperationRecorder storageWaitPostRecorder{"storage.wait_post"};
monitor::OperationRecorder storageWaitAioAndPostRecorder{"storage.wait_aio_and_post"};
monitor::OperationRecorder storageReadPrepareTarget{"storage.read_prepare_target"};
monitor::OperationRecorder storageReadPrepareBuffer{"storage.read_prepare_buffer"};

monitor::OperationRecorder storageReqReadRecorder{"storage.req_read"};
monitor::DistributionRecorder storageReqReadSize{"storage.req_read.size"};
monitor::CountRecorder storageReadCount{"storage.read.count"};
monitor::CountRecorder storageReadBytes{"storage.read.bytes"};
monitor::LambdaRecorder storageReadAvgBytes{"storage.read.avg_bytes"};
monitor::CountRecorder aioTotalHeadLength{"storage.aio_align.total_head_length"};
monitor::CountRecorder aioTotalTailLength{"storage.aio_align.total_tail_length"};
monitor::CountRecorder aioTotalAlignedLength{"storage.aio_align.total_length"};

monitor::OperationRecorder storageReqWriteRecorder{"storage.req_write"};
monitor::CountRecorder storageWriteBytes{"storage.req_write.bytes"};

monitor::OperationRecorder storageReqUpdateRecorder{"storage.req_update"};
monitor::CountRecorder storageUpdateBytes{"storage.req_update.bytes"};

monitor::CountRecorder storageTotalWriteBytes{"storage.write.bytes"};

monitor::OperationRecorder storageDoUpdateRecorder{"storage.do_update"};
monitor::OperationRecorder storageWriteWaitSemRecorder{"storage.write_wait_sem"};
monitor::OperationRecorder storageWriteWaitPostRecorder{"storage.write_wait_post"};
monitor::OperationRecorder storageDoCommitRecorder{"storage.do_commit"};
monitor::OperationRecorder storageDoQueryRecorder{"storage.do_query"};
monitor::CountRecorder storageNumChunksInQueryRes{"storage.do_query.num_chunks"};
monitor::OperationRecorder waitChunkLockRecorder{"storage.wait_chunk_lock"};
monitor::OperationRecorder storageDoTruncateRecorder{"storage.do_truncate"};
monitor::OperationRecorder storageDoRemoveRecorder{"storage.do_remove"};
monitor::CountRecorder storageNumChunksRemoved{"storage.do_remove.num_chunks"};
monitor::OperationRecorder syncStartRecorder{"storage.sync_start"};
monitor::OperationRecorder syncDoneRecorder{"storage.sync_done"};

monitor::OperationRecorder storageReqRemoveChunksRecorder{"storage.req_remove_chunks"};
monitor::OperationRecorder storageRemoveRangeRecorder{"storage.remove_range"};
monitor::CountRecorder storageCacheReplaceCount{"storage.cache.replace"};
monitor::CountRecorder storageCacheStaleReplaceCount{"storage.cache.replace_stale"};
monitor::CountRecorder storageCacheRetireCount{"storage.cache.retire"};
monitor::CountRecorder storageCacheTombstoneCount{"storage.cache.tombstone"};

Result<Void> StorageOperator::init(uint32_t numberOfDisks) {
  auto localEvictionPolicy = createLocalEvictionPolicy(config_.local_eviction_policy());
  RETURN_ON_ERROR(localEvictionPolicy);
  localEvictionPolicy_ = std::move(*localEvictionPolicy);
  storageReadAvgBytes.setLambda([&] {
    auto totalReadBytes = totalReadBytes_.exchange(0);
    auto totalReadIOs = totalReadIOs_.exchange(0);
    return totalReadBytes / std::max(1ul, totalReadIOs);
  });

  if (!storageEventTrace_.open()) {
    XLOGF(CRITICAL, "Failed to open trace log in directory: {}", config_.event_trace_log().trace_file_dir());
    return makeError(StorageCode::kStorageInitFailed);
  }

  return updateWorker_.start(numberOfDisks);
}

Result<Void> StorageOperator::stopAndJoin() {
  storageReadAvgBytes.reset();
  updateWorker_.stopAndJoin();
  storageEventTrace_.close();
  return Void{};
}

CoTryTask<BatchReadRsp> StorageOperator::batchRead(ServiceRequestContext &requestCtx,
                                                   const BatchReadReq &req,
                                                   serde::CallContext &ctx) {
  XLOGF(DBG5, "Received batch read request {} with tag {} and {} IOs", fmt::ptr(&req), req.tag, req.payloads.size());

  auto recordGuard = storageReqReadRecorder.record(monitor::instanceTagSet(std::to_string(req.userInfo.uid)));

  auto prepareTargetRecordGuard = storageReadPrepareTarget.record();
  auto snapshot = components_.targetMap.snapshot();
  auto batchSize = req.payloads.size();
  BatchReadRsp rsp;
  rsp.results.resize(batchSize);
  BatchReadJob batch(req.payloads, rsp.results, req.checksumType);
  storageReadCount.addSample(batchSize);
  storageReqReadSize.addSample(batchSize);

  size_t totalLength = 0;
  size_t totalHeadLength = 0;
  size_t totalTailLength = 0;
  for (AioReadJobIterator it(&batch); it; it++) {
    // get target for batch read, need check public and local state.
    auto targetResult = FAULT_INJECTION_POINT(
        requestCtx.debugFlags.injectServerError(),
        makeError(StorageCode::kChainVersionMismatch),
        snapshot->getByChainId(it->readIO().key.vChainId, config_.batch_read_ignore_chain_version()));
    if (UNLIKELY(!targetResult)) {
      auto msg = fmt::format("read get target failed, req {}, error {}", it->readIO(), targetResult.error());
      XLOG(ERR, msg);
      co_return makeError(std::move(targetResult.error()));
    }
    auto target = std::move(*targetResult);
    if (UNLIKELY(!target->upToDate())) {
      auto msg = fmt::format("read target is not upToDate, req {}, target {}", it->readIO(), *target);
      XLOG(ERR, msg);
      co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
    }
    it->state().storageTarget = target->storageTarget.get();
    totalLength += it->readIO().length;
    totalHeadLength += it->state().headLength;
    totalTailLength += it->state().tailLength;
    if (FAULT_INJECTION_POINT(requestCtx.debugFlags.injectServerError(),
                              true,
                              UNLIKELY(it->readIO().length > it->readIO().rdmabuf.size()))) {
      auto msg = fmt::format("invalid read buffer size {}", it->readIO());
      XLOG(ERR, msg);
      co_return makeError(StatusCode::kInvalidArg, std::move(msg));
    }
    it->state().readUncommitted = BITFLAGS_CONTAIN(req.featureFlags, FeatureFlags::ALLOW_READ_UNCOMMITTED);
  }
  totalReadBytes_ += totalLength;
  totalReadIOs_ += batchSize;
  storageReadBytes.addSample(totalLength);
  aioTotalHeadLength.addSample(totalHeadLength);
  aioTotalTailLength.addSample(totalTailLength);
  aioTotalAlignedLength.addSample(totalLength + totalHeadLength + totalTailLength);
  prepareTargetRecordGuard.report(true);

  auto prepareBufferRecordGuard = storageReadPrepareBuffer.record();
  auto buffer = components_.rdmabufPool.get();
  for (AioReadJobIterator it(&batch); it; it++) {
    auto &job = *it;
    auto allocateResult = buffer.tryAllocate(job.alignedLength());
    if (UNLIKELY(!allocateResult)) {
      allocateResult = co_await buffer.allocate(job.alignedLength());
    }
    if (UNLIKELY(!allocateResult)) {
      auto msg = fmt::format("read allocate buffer failed, req {}, length {}", job.readIO(), job.alignedLength());
      XLOG(ERR, msg);
      co_return makeError(RPCCode::kRDMANoBuf, std::move(msg));
    }
    job.state().localbuf = std::move(*allocateResult);
    job.state().bufferIndex = buffer.index();
  }
  prepareBufferRecordGuard.report(true);

  if (BITFLAGS_CONTAIN(req.featureFlags, FeatureFlags::BYPASS_DISKIO)) {
    for (AioReadJobIterator it(&batch); it; it++) {
      it->result().lengthInfo = it->readIO().length;
      batch.finish(&*it);
    }
  } else {
    auto recordGuard = storageAioEnqueueRecorder.record();
    auto splitSize = config_.batch_read_job_split_size();
    for (uint32_t start = 0; start < batchSize; start += splitSize) {
      co_await components_.aioReadWorker.enqueue(AioReadJobIterator(&batch, start, splitSize));
    }
    recordGuard.report(true);
  }

  auto waitAioAndPostRecordGuard = storageWaitAioAndPostRecorder.record();
  auto waitAioRecordGuard = storageWaitAioRecorder.record();
  co_await batch.complete();
  waitAioRecordGuard.report(true);

  if (config_.enable_cache_phase2()) {
    const auto observedAtNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
    for (AioReadJobIterator it(&batch); it; it++) {
      if (!it->result().lengthInfo || it->result().cacheGeneration == cache::CacheGeneration{}) continue;
      auto accessResult =
          co_await it->state().storageTarget->recordCacheAccess(it->readIO().key.chunkId,
                                                                it->result().cacheGeneration,
                                                                observedAtNs,
                                                                config_.local_access_persist_interval());
      XLOGF_IF(WARN,
               accessResult.hasError(),
               "record local cache access failed for chunk {} generation {}: {}",
               it->readIO().key.chunkId,
               it->result().cacheGeneration,
               accessResult.error());
    }
  }

  if (BITFLAGS_CONTAIN(req.featureFlags, FeatureFlags::SEND_DATA_INLINE)) {
    batch.copyToRespBuffer(rsp.inlinebuf.data);
  } else if (!BITFLAGS_CONTAIN(req.featureFlags, FeatureFlags::BYPASS_RDMAXMIT)) {
    auto ibSocket = ctx.transport()->ibSocket();
    if (UNLIKELY(ibSocket == nullptr)) {
      XLOGF(ERR, "batch read no RDMA socket");
      co_return makeError(StatusCode::kInvalidArg, "batch read no RDMA socket");
    }

    auto waitBatchRecordGuard = storageWaitBatchRecorder.record();
    auto writeBatch = ctx.writeTransmission();
    batch.addBufferToBatch(writeBatch);
    waitBatchRecordGuard.report(true);

    auto rdmaSemaphoreIter = concurrentRdmaWriteSemaphore_.find(ibSocket->device()->id());
    if (rdmaSemaphoreIter == concurrentRdmaWriteSemaphore_.end()) {
      XLOGF(CRITICAL,
            "Cannot find RDMA operation semaphore for IB device #{} {}",
            ibSocket->device()->id(),
            ibSocket->device()->name());
      co_return makeError(RPCCode::kIBDeviceNotFound);
    }

    auto RDMATransmissionReqTimeout = config_.rdma_transmission_req_timeout();
    bool applyTransmissionBeforeGettingSemaphore = config_.apply_transmission_before_getting_semaphore();
    if (ctx.packet().controlRDMA() && RDMATransmissionReqTimeout != 0_ms && applyTransmissionBeforeGettingSemaphore) {
      co_await writeBatch.applyTransmission(RDMATransmissionReqTimeout);
    }

    auto ibdevTagSet = monitor::instanceTagSet(ibSocket->device()->name());
    auto waitSemRecordGuard = storageWaitSemRecorder.record(ibdevTagSet);
    SemaphoreGuard guard(rdmaSemaphoreIter->second);
    co_await guard.coWait();
    waitSemRecordGuard.report(true);

    if (ctx.packet().controlRDMA() && RDMATransmissionReqTimeout != 0_ms && !applyTransmissionBeforeGettingSemaphore) {
      co_await writeBatch.applyTransmission(RDMATransmissionReqTimeout);
    }

    auto waitPostRecordGuard = storageWaitPostRecorder.record(ibdevTagSet);
    auto postResult = FAULT_INJECTION_POINT(requestCtx.debugFlags.injectServerError(),
                                            makeError(RPCCode::kRDMAPostFailed),
                                            (co_await writeBatch.post()));
    if (UNLIKELY(!postResult)) {
      for (AioReadJobIterator it(&batch); it; it++) {
        it->result().lengthInfo = makeError(std::move(postResult.error()));
      }
    } else {
      waitPostRecordGuard.succ();
    }
  }
  waitAioAndPostRecordGuard.report(true);

  recordGuard.succ();
  co_return rsp;
}

CoTryTask<WriteRsp> StorageOperator::write(ServiceRequestContext &requestCtx,
                                           const WriteReq &req,
                                           net::IBSocket *ibSocket) {
  auto recordGuard = storageReqWriteRecorder.record(monitor::instanceTagSet(std::to_string(req.userInfo.uid)));

  XLOGF(DBG1,
        "Received write request {} with tag {} to chunk {} on {}",
        fmt::ptr(&req),
        req.tag,
        req.payload.key.chunkId,
        req.payload.key.vChainId.chainId);

  WriteRsp rsp;
  rsp.tag = req.tag;

  // get target for write from client.
  auto targetResult = FAULT_INJECTION_POINT(requestCtx.debugFlags.injectServerError(),
                                            makeError(StorageCode::kChainVersionMismatch),
                                            components_.targetMap.getByChainId(req.payload.key.vChainId));
  if (UNLIKELY(!targetResult)) {
    rsp.result.lengthInfo = makeError(std::move(targetResult.error()));
    co_return rsp;
  }
  auto target = std::move(*targetResult);

  UpdateReq updateReq{req.payload, {}, req.tag, req.retryCount, req.userInfo, req.featureFlags};
  updateReq.options.fromClient = true;
  rsp.result = co_await components_.reliableUpdate.update(requestCtx, updateReq, ibSocket, target);
  if (LIKELY(bool(rsp.result.lengthInfo))) {
    XLOGF_IF(DFATAL,
             *rsp.result.lengthInfo != req.payload.length,
             "Length info in response {} not equal to write size in request {}, result: {}, write io: {}",
             *rsp.result.lengthInfo,
             req.payload.length,
             rsp.result,
             req.payload);
    storageWriteBytes.addSample(*rsp.result.lengthInfo);
    storageTotalWriteBytes.addSample(*rsp.result.lengthInfo);
    recordGuard.succ();
  }

  XLOGF(DBG1,
        "Processed write request {} with tag {} to chunk {} on {}, result: {}",
        fmt::ptr(&req),
        req.tag,
        req.payload.key.chunkId,
        req.payload.key.vChainId.chainId,
        rsp.result);
  co_return rsp;
}

CoTryTask<UpdateRsp> StorageOperator::update(ServiceRequestContext &requestCtx,
                                             const UpdateReq &updateReq,
                                             net::IBSocket *ibSocket) {
  auto recordGuard = storageReqUpdateRecorder.record(monitor::instanceTagSet(std::to_string(updateReq.userInfo.uid)));

  auto req = updateReq;
  XLOGF(DBG1,
        "Received update request {} with tag {} to chunk {} on {}",
        fmt::ptr(&req),
        req.tag,
        req.payload.key.chunkId,
        req.payload.key.vChainId.chainId);

  UpdateRsp rsp;
  rsp.tag = req.tag;

  // get target for update from predecessor server.
  auto targetResult = FAULT_INJECTION_POINT(requestCtx.debugFlags.injectServerError(),
                                            makeError(StorageCode::kChainVersionMismatch),
                                            components_.targetMap.getByChainId(req.payload.key.vChainId));
  if (UNLIKELY(!targetResult)) {
    rsp.result.lengthInfo = makeError(std::move(targetResult.error()));
    co_return rsp;
  }
  auto target = std::move(*targetResult);

  if (req.payload.updateType == UpdateType::REMOVE && req.tag.channel.id == ChannelId{0}) {
    rsp.result = co_await handleUpdate(requestCtx, req, ibSocket, target);
  } else {
    rsp.result = co_await components_.reliableUpdate.update(requestCtx, req, ibSocket, target);
  }

  if (LIKELY(bool(rsp.result.lengthInfo))) {
    storageUpdateBytes.addSample(*rsp.result.lengthInfo);
    storageTotalWriteBytes.addSample(*rsp.result.lengthInfo);
    recordGuard.succ();
  }

  XLOGF(DBG1,
        "Processed update request {} with tag {} to chunk {} on {}, result: {}",
        fmt::ptr(&req),
        req.tag,
        req.payload.key.chunkId,
        req.payload.key.vChainId.chainId,
        rsp.result);

  co_return rsp;
}

CoTask<IOResult> StorageOperator::handleUpdate(ServiceRequestContext &requestCtx,
                                               UpdateReq &req,
                                               net::IBSocket *ibSocket,
                                               TargetPtr &target) {
  // 1. get target.
  if (UNLIKELY(req.options.fromClient && !target->isHead)) {
    XLOGF(ERR, "non-head node receive a client update request");
    co_return makeError(StorageClientCode::kRoutingError, "non-head node receive a client update request");
  }
  if (UNLIKELY(req.options.fromClient && config_.read_only())) {
    auto msg = fmt::format("storage is readonly!");
    XLOG(ERR, msg);
    co_return makeError(StatusCode::kReadOnlyMode, std::move(msg));
  }
  if (UNLIKELY(req.payload.key.chunkId.data().empty())) {
    auto msg = fmt::format("update request with empty chunk id: {}", req);
    XLOG(ERR, msg);
    co_return makeError(StatusCode::kInvalidArg, std::move(msg));
  }
  if (req.options.isSyncing && (req.payload.isTruncate() || req.payload.isExtend())) {
    auto msg = fmt::format("reject truncate/extend request from syncing client: {}", req);
    XLOG(ERR, msg);
    co_return makeError(StatusCode::kInvalidArg, std::move(msg));
  }

  XLOGF(DBG1, "Start the replication process, target: {}, tag: {}, req: {}", target->targetId, req.tag, req);

  const auto &appInfo = components_.getAppInfo();
  auto trace = storageEventTrace_.newEntry(StorageEventTrace{
      .clusterId = appInfo.clusterId,
      .nodeId = appInfo.nodeId,
      .targetId = target->targetId,
      .updateReq = req,
  });

  // 2. lock chunk.
  folly::coro::Baton baton;
  auto recordGuard = waitChunkLockRecorder.record();
  auto lockGuard = target->storageTarget->lockChunk(baton, req.payload.key.chunkId, fmt::to_string(req.tag));
  if (!lockGuard.locked()) {
    XLOGF(DBG1,
          "write wait lock on chunk {}, current owner: {}, req: {}",
          req.payload.key.chunkId,
          lockGuard.currentTag(),
          req);
    co_await lockGuard.lock();
  }
  recordGuard.report(true);

  // re-check chain version after acquiring the lock.
  auto targetResult = components_.targetMap.getByChainId(req.payload.key.vChainId);
  if (UNLIKELY(!targetResult)) {
    co_return makeError(std::move(targetResult.error()));
  }
  target = std::move(*targetResult);

  ChunkEngineUpdateJob chunkEngineJob{};

  // 3. update local target.
  auto buffer = components_.rdmabufPool.get();
  net::RDMARemoteBuf remoteBuf;
  auto updateResult = co_await doUpdate(requestCtx,
                                        req.payload,
                                        req.options,
                                        req.featureFlags,
                                        target->storageTarget,
                                        ibSocket,
                                        buffer,
                                        remoteBuf,
                                        chunkEngineJob,
                                        !(req.options.fromClient && target->rejectCreateChunk));
  trace->updateRes = updateResult;

  uint32_t code = updateResult.lengthInfo ? 0 : updateResult.lengthInfo.error().code();
  if (code == 0) {
    // 1. write success.
    if (req.payload.updateVer == 0) {
      req.payload.updateVer = updateResult.updateVer;
    } else if (UNLIKELY(req.payload.updateVer != updateResult.updateVer)) {
      auto msg = fmt::format("write update version mismatch, req {}, result {}", req, updateResult);
      XLOG(DFATAL, msg);
      co_return makeError(StorageCode::kChunkVersionMismatch, std::move(msg));
    }
  } else if (code == StorageCode::kChunkMissingUpdate) {
    // 2. missing update.
    XLOGF(DFATAL, "write missing update and block, req {}, result {}", req, updateResult);
    co_return updateResult;
  } else if (code == StorageCode::kChunkCommittedUpdate) {
    // 3. committed update, considered as a successful write.
    updateResult.lengthInfo = req.payload.length;
    updateResult.updateVer = req.payload.updateVer;
    updateResult.commitVer = req.payload.updateVer;
    XLOGF(DFATAL, "write committed update, req {}, result {}", req, updateResult);
    co_return updateResult;
  } else if (code == StorageCode::kChunkStaleUpdate) {
    // 3. stale update, considered as a successful write.
    updateResult.lengthInfo = req.payload.length;
    updateResult.updateVer = req.payload.updateVer;
    XLOGF(CRITICAL, "write stale update, req {}, result {}", req, updateResult);
  } else if (code == StorageCode::kChunkAdvanceUpdate) {
    // 4. advance update.
    XLOGF(DFATAL, "write advance update, req {}, result {}", req, updateResult);
    co_return updateResult;
  } else {
    XLOGF(CRITICAL, "write update failed, req {}, result {}", req, updateResult);
    co_return updateResult;
  }

  XLOGF(DBG1, "Updated local chunk, target: {}, tag: {}, result: {}", target->targetId, req.tag, updateResult);

  // 4. forward to successor.
  CommitIO commitIO;
  commitIO.key = req.payload.key;
  commitIO.commitVer = updateResult.updateVer;
  commitIO.isRemove = req.payload.isRemove();

  auto forwardResult = co_await components_.reliableForwarding
                           .forwardWithRetry(requestCtx, req, remoteBuf, chunkEngineJob, target, commitIO);
  if (UNLIKELY(commitIO.commitVer != updateResult.updateVer)) {
    auto msg = fmt::format("commit version mismatch, req: {}, successor {} != local {}",
                           req,
                           commitIO.commitVer,
                           updateResult.updateVer);
    XLOG(DFATAL, msg);
    co_return makeError(StorageCode::kChunkVersionMismatch, std::move(msg));
  }

  XLOGF(DBG1,
        "Forwarded update to successor {}, target: {}, tag: {}, result: {}",
        (target->successor ? target->successor->targetInfo.targetId : TargetId{0}),
        target->targetId,
        req.tag,
        forwardResult);
  trace->forwardRes = forwardResult;
  trace->commitIO = commitIO;

  if (forwardResult.lengthInfo) {
    if (commitIO.isRemove && (forwardResult.checksum.type == ChecksumType::NONE ||
                              updateResult.checksum.type == ChecksumType::NONE || commitIO.isSyncing)) {
      // The known issue is that during the delete operation, it is possible for one side to encounter a "chunk not
      // found" situation.
      XLOGF(INFO,
            "Remove op local checksum {} not equal to checksum {} generated by successor, key: {}, syncing: {}",
            updateResult.checksum,
            forwardResult.checksum,
            req.payload.key,
            commitIO.isSyncing);
    } else if (forwardResult.checksum != updateResult.checksum) {
      auto msg = fmt::format("Local checksum {} not equal to checksum {} generated by successor, key: {}",
                             updateResult.checksum,
                             forwardResult.checksum,
                             req.payload.key);
      XLOG_IF(DFATAL, !requestCtx.debugFlags.faultInjectionEnabled(), msg);
      co_return makeError(StorageClientCode::kChecksumMismatch, std::move(msg));
    }
  } else if (forwardResult.lengthInfo.error().code() != StorageCode::kNoSuccessorTarget) {
    co_return forwardResult;
  }

  // 5. commit.
  auto commitResult =
      co_await doCommit(requestCtx, commitIO, req.options, chunkEngineJob, req.featureFlags, target->storageTarget);

  code = commitResult.lengthInfo ? 0 : commitResult.lengthInfo.error().code();

  if (LIKELY(code == 0)) {
    // 1. commit success.
  } else if (code == StorageCode::kChunkStaleCommit) {
    // 2. stale commit, considered as a successful commit.
    XLOGF(INFO, "write stale commit, req {}, result {}", req, commitResult);
    commitResult.commitVer = updateResult.updateVer;
  } else {
    // 3. commit fail.
    XLOGF(ERR, "write commit fail, req {}, result {}", req, commitResult);
    co_return commitResult;
  }

  commitResult.lengthInfo = updateResult.lengthInfo;
  commitResult.checksum = updateResult.checksum;

  XLOGF(DBG1, "Committed local chunk, target: {}, tag: {}, result: {}", target->targetId, req.tag, commitResult);
  trace->commitRes = commitResult;

  // storageEventTrace_.append(const StorageEventTrace &obj)

  co_return commitResult;
}

CoTask<IOResult> StorageOperator::doUpdate(ServiceRequestContext &requestCtx,
                                           const UpdateIO &updateIO,
                                           const UpdateOptions &updateOptions,
                                           uint32_t featureFlags,
                                           const std::shared_ptr<StorageTarget> &target,
                                           net::IBSocket *ibSocket,
                                           BufferPool::Buffer &buffer,
                                           net::RDMARemoteBuf &remoteBuf,
                                           ChunkEngineUpdateJob &chunkEngineJob,
                                           bool allowToAllocate) {
  auto recordGuard = storageDoUpdateRecorder.record();
  UpdateJob job(requestCtx, updateIO, updateOptions, chunkEngineJob, target, allowToAllocate);

  if (BITFLAGS_CONTAIN(featureFlags, FeatureFlags::SEND_DATA_INLINE)) {
    if (updateIO.inlinebuf.data.size() != updateIO.length) {
      auto msg = fmt::format("[BUG] Inline buffer size {} not equal to update size {}, io: {}",
                             updateIO.inlinebuf.data.size(),
                             updateIO.length,
                             updateIO);
      XLOG(DFATAL, msg);
      co_return makeError(StorageClientCode::kFoundBug, std::move(msg));
    }
    job.state().data = updateIO.inlinebuf.data.data();
  } else if (updateIO.isWrite()) {
    if (UNLIKELY(ibSocket == nullptr)) {
      auto msg = fmt::format("update no RDMA socket, io: {}", updateIO);
      XLOG(ERR, msg);
      co_return makeError(StatusCode::kInvalidArg, std::move(msg));
    }

    auto allocateResult = buffer.tryAllocate(updateIO.rdmabuf.size());
    if (UNLIKELY(!allocateResult)) {
      allocateResult = co_await buffer.allocate(updateIO.rdmabuf.size());
    }
    if (UNLIKELY(!allocateResult)) {
      auto msg = fmt::format("write allocate buffer failed, req {}, error {}, length {}",
                             updateIO,
                             allocateResult.error(),
                             updateIO.rdmabuf.size());
      XLOG(ERR, msg);
      co_return makeError(RPCCode::kRDMANoBuf, std::move(msg));
    }
    job.state().data = allocateResult->ptr();
    remoteBuf = allocateResult->toRemoteBuf();
    if (!BITFLAGS_CONTAIN(featureFlags, FeatureFlags::BYPASS_RDMAXMIT)) {
      auto readBatch = ibSocket->rdmaReadBatch();
      auto batchAddResult = readBatch.add(updateIO.rdmabuf, std::move(*allocateResult));
      if (UNLIKELY(!batchAddResult)) {
        XLOGF(ERR, "write add to batch failed, req {}, error {}", updateIO, batchAddResult.error());
        co_return makeError(batchAddResult.error());
      }

      auto rdmaSemaphoreIter = concurrentRdmaReadSemaphore_.find(ibSocket->device()->id());
      if (rdmaSemaphoreIter == concurrentRdmaReadSemaphore_.end()) {
        auto msg = fmt::format("Cannot find RDMA operation semaphore for IB device #{} {}",
                               ibSocket->device()->id(),
                               ibSocket->device()->name());
        XLOG(CRITICAL, msg);
        co_return makeError(RPCCode::kIBDeviceNotFound, std::move(msg));
      }

      auto ibdevTagSet = monitor::instanceTagSet(ibSocket->device()->name());
      auto waitSemRecordGuard = storageWriteWaitSemRecorder.record(ibdevTagSet);
      SemaphoreGuard guard(rdmaSemaphoreIter->second);
      co_await guard.coWait();
      waitSemRecordGuard.report(true);

      auto waitPostRecordGuard = storageWriteWaitPostRecorder.record(ibdevTagSet);
      auto postResult = co_await readBatch.post();
      if (UNLIKELY(!postResult)) {
        XLOGF(ERR, "write post RDMA failed, req {}, error {}", updateIO, postResult.error());
        co_return makeError(std::move(postResult.error()));
      } else {
        waitPostRecordGuard.report(true);
      }
    }
  }

  if (BITFLAGS_CONTAIN(featureFlags, FeatureFlags::BYPASS_DISKIO)) {
    job.setResult(updateIO.length);
  } else {
    co_await updateWorker_.enqueue(&job);
    co_await job.complete();
  }
  if (LIKELY(bool(job.result().lengthInfo))) {
    recordGuard.succ();
  } else {
    auto code = job.result().lengthInfo.error().code();
    if (code == StorageCode::kChunkWriteFailed || code == StorageCode::kChunkMetadataSetError) {
      components_.targetMap.offlineTargets(target->path().parent_path());
    }
  }
  co_return std::move(job.result());
}

CoTask<IOResult> StorageOperator::doCommit(ServiceRequestContext &requestCtx,
                                           const CommitIO &commitIO,
                                           const UpdateOptions &updateOptions,
                                           ChunkEngineUpdateJob &chunkEngineJob,
                                           uint32_t featureFlags,
                                           const std::shared_ptr<StorageTarget> &target) {
  auto recordGuard = storageDoCommitRecorder.record();
  UpdateJob job(requestCtx, commitIO, updateOptions, chunkEngineJob, target);
  if (BITFLAGS_CONTAIN(featureFlags, FeatureFlags::BYPASS_DISKIO)) {
    job.setResult(0);
    job.result().commitVer = commitIO.commitVer;
    job.result().commitChainVer = commitIO.commitChainVer;
  } else {
    co_await updateWorker_.enqueue(&job);
    co_await job.complete();
  }
  if (LIKELY(bool(job.result().lengthInfo))) {
    recordGuard.succ();
  }
  co_return job.result();
}

Result<std::vector<std::pair<ChunkId, ChunkMetadata>>> StorageOperator::doQuery(ServiceRequestContext &requestCtx,
                                                                                const VersionedChainId &vChainId,
                                                                                const ChunkIdRange &chunkIdRange) {
  auto recordGuard = storageDoQueryRecorder.record();
  // get target for chunk query from client.
  CHECK_RESULT(target, components_.targetMap.getByChainId(vChainId));

  auto queryResult = FAULT_INJECTION_POINT(requestCtx.debugFlags.injectServerError(),
                                           makeError(StorageCode::kMetaStoreInvalidIterator),
                                           target->storageTarget->queryChunks(chunkIdRange));

  if (LIKELY(bool(queryResult))) {
    storageNumChunksInQueryRes.addSample(queryResult->size());
    recordGuard.succ();
  }

  return queryResult;
}

// returns number of processed chunks on success
CoTryTask<uint32_t> StorageOperator::processQueryResults(ServiceRequestContext &requestCtx,
                                                         const VersionedChainId &vChainId,
                                                         const ChunkIdRange &chunkIdRange,
                                                         ChunkMetadataProcessor processor,
                                                         bool &moreChunksInRange) {
  const uint32_t numChunksToProcess = chunkIdRange.maxNumChunkIdsToProcess
                                          ? std::min(chunkIdRange.maxNumChunkIdsToProcess, UINT32_MAX - 1)
                                          : (UINT32_MAX - 1);
  const uint32_t maxNumResultsPerQuery = config_.max_num_results_per_query();
  ChunkIdRange currentRange = {chunkIdRange.begin, chunkIdRange.end, 0};
  uint32_t numQueryResults = 0;
  Status status(StatusCode::kOK);

  while (true) {
    currentRange.maxNumChunkIdsToProcess = std::min(numChunksToProcess - numQueryResults + 1, maxNumResultsPerQuery);

    auto queryResult = doQuery(requestCtx, vChainId, currentRange);

    if (UNLIKELY(queryResult.hasError())) {
      status = queryResult.error();
      goto exit;
    }

    for (const auto &[chunkId, metadata] : *queryResult) {
      switch (metadata.recycleState) {
        case RecycleState::NORMAL:
          break;
        case RecycleState::REMOVAL_IN_PROGRESS:
          XLOGF(INFO,
                "Ignore chunk {} being removed, recycle state {}, commit version {}, update version {}",
                chunkId,
                int(metadata.recycleState),
                metadata.commitVer,
                metadata.updateVer);
          continue;
        case RecycleState::REMOVAL_IN_RETRYING:
          XLOGF(INFO,
                "Ignore dummy chunk {} being removed, recycle state {}, commit version {}, update version {}",
                chunkId,
                int(metadata.recycleState),
                metadata.commitVer,
                metadata.updateVer);
          continue;
      }

      if (numQueryResults < numChunksToProcess) {
        auto result = co_await processor(chunkId, metadata);

        if (UNLIKELY(result.hasError())) {
          status = result.error();
          goto exit;
        }
      }

      numQueryResults++;

      if (numQueryResults >= numChunksToProcess + 1) {
        XLOGF(DBG5,
              "Enough chunks in range found, number of results: {}/{}, current range: {}",
              numQueryResults,
              numChunksToProcess,
              currentRange);
        goto exit;
      }
    }

    if (queryResult->size() < currentRange.maxNumChunkIdsToProcess) {
      XLOGF(DBG5,
            "No more chunk in range, number of results: {}/{}, current range: {}",
            numQueryResults,
            numChunksToProcess,
            currentRange);
      goto exit;
    } else {
      // there could be more chunks in the range, update range for next query
      const auto &[chunkId, _] = *(queryResult->crbegin());
      currentRange.end = chunkId;
    }
  }

exit:
  if (status.code() != StatusCode::kOK) {
    XLOGF(ERR,
          "Failed to process chunk metadata in range: {}, error {}, {} chunks processed before failure",
          chunkIdRange,
          status,
          numQueryResults);
    co_return makeError(status);
  }

  moreChunksInRange = numQueryResults > numChunksToProcess;

  XLOGF(DBG3,
        "Processed metadata of {} chunks in range: {}, more chunks: {}",
        numQueryResults,
        chunkIdRange,
        moreChunksInRange);
  co_return std::min(numQueryResults, numChunksToProcess);
}

CoTask<IOResult> StorageOperator::doTruncate(ServiceRequestContext &requestCtx,
                                             const TruncateChunkOp &op,
                                             flat::UserInfo userInfo,
                                             uint32_t featureFlags) {
  auto recordGuard = storageDoTruncateRecorder.record();
  UpdateIO updateIO{0 /*offset*/,
                    op.chunkLen,
                    op.chunkSize,
                    GlobalKey{op.vChainId, op.chunkId},
                    {} /*rdmabuf*/,
                    ChunkVer(0) /*updateVer*/,
                    op.onlyExtendChunk ? UpdateType::EXTEND : UpdateType::TRUNCATE,
                    ChecksumInfo{ChecksumType::NONE, 0}};
  UpdateReq updateReq{updateIO, {}, op.tag, op.retryCount, userInfo, featureFlags};
  updateReq.options.fromClient = true;

  // get target for truncate from client.
  auto targetResult = components_.targetMap.getByChainId(op.vChainId);
  if (UNLIKELY(!targetResult)) {
    IOResult rsp;
    rsp.lengthInfo = makeError(std::move(targetResult.error()));
    co_return rsp;
  }
  auto target = std::move(*targetResult);

  auto updateRes = co_await components_.reliableUpdate.update(requestCtx, updateReq, nullptr /*ibSocket*/, target);

  XLOGF_IF(ERR,
           updateRes.lengthInfo.hasError(),
           "Failed to truncate chunk {} on {}, tag: {}, result: {}",
           updateIO.key.chunkId,
           updateIO.key.vChainId.chainId,
           updateReq.tag,
           updateRes);
  XLOGF_IF(INFO,
           !updateRes.lengthInfo.hasError(),
           "Truncated chunk {} on {}, tag: {}, result: {}",
           updateIO.key.chunkId,
           updateIO.key.vChainId.chainId,
           updateReq.tag,
           updateRes);

  if (LIKELY(bool(updateRes.lengthInfo))) {
    recordGuard.succ();
  }
  co_return updateRes;
}

CoTask<IOResult> StorageOperator::doRemove(ServiceRequestContext &requestCtx,
                                           const RemoveChunksOp &op,
                                           flat::UserInfo userInfo,
                                           uint32_t featureFlags) {
  auto recordGuard = storageDoRemoveRecorder.record();
  // this method requires that the chunk id range specifies one chunk
  assert(op.chunkIdRange.begin == op.chunkIdRange.end);
  UpdateIO updateIO{0 /*offset*/,
                    0 /*length*/,
                    0 /*chunkSize*/,
                    GlobalKey{op.vChainId, op.chunkIdRange.begin},
                    {} /*rdmabuf*/,
                    ChunkVer(0) /*updateVer*/,
                    UpdateType::REMOVE,
                    ChecksumInfo{ChecksumType::NONE, 0}};
  UpdateReq updateReq{updateIO, {}, op.tag, op.retryCount, userInfo, featureFlags};
  updateReq.options.fromClient = true;

  // get target for remove from client.
  auto targetResult = components_.targetMap.getByChainId(op.vChainId);

  if (UNLIKELY(!targetResult)) {
    IOResult rsp;
    rsp.lengthInfo = makeError(std::move(targetResult.error()));
    co_return rsp;
  }
  auto target = std::move(*targetResult);

  IOResult updateRes;

  if (op.tag.channel.id == ChannelId{0}) {
    updateRes = co_await handleUpdate(requestCtx, updateReq, nullptr /*ibSocket*/, target);
  } else {
    updateRes = co_await components_.reliableUpdate.update(requestCtx, updateReq, nullptr /*ibSocket*/, target);
  }

  XLOGF_IF(ERR,
           updateRes.lengthInfo.hasError(),
           "Failed to remove chunk {} on {}, tag: {}, result: {}",
           updateIO.key.chunkId,
           updateIO.key.vChainId.chainId,
           updateReq.tag,
           updateRes);
  XLOGF_IF(INFO,
           !updateRes.lengthInfo.hasError(),
           "Removed chunk {} on {}, tag: {}, result: {}",
           updateIO.key.chunkId,
           updateIO.key.vChainId.chainId,
           updateReq.tag,
           updateRes);

  if (LIKELY(bool(updateRes.lengthInfo))) {
    recordGuard.succ();
  }
  co_return updateRes;
}

CoTryTask<QueryLastChunkRsp> StorageOperator::queryLastChunk(ServiceRequestContext &requestCtx,
                                                             const QueryLastChunkReq &req) {
  XLOGF(DBG3, "Query request {} with {} ops", fmt::ptr(&req), req.payloads.size());

  QueryLastChunkRsp rsp;
  rsp.results.reserve(req.payloads.size());

  for (auto &payload : req.payloads) {
    QueryLastChunkResult queryResult{
        Void{},
        ChunkId(), /*lastChunkId*/
        0 /*lastChunkLen*/,
        0 /*totalChunkLen*/,
        0 /*totalNumChunks*/,
        false /*moreChunksInRange*/,
    };

    auto processMetadata = [&queryResult](const ChunkId &chunkId, const ChunkMetadata &metadata) -> CoTryTask<void> {
      if (queryResult.lastChunkId.data().empty() || queryResult.lastChunkId < chunkId) {
        queryResult.lastChunkId = chunkId;
        queryResult.lastChunkLen = metadata.size;
      }

      queryResult.totalChunkLen += metadata.size;
      queryResult.totalNumChunks++;

      XLOGF(DBG5,
            "Query chunk {}, lastChunkId {}, totalChunkLen {}, metadata: {}",
            chunkId,
            queryResult.lastChunkId,
            queryResult.totalChunkLen,
            metadata);
      co_return Void{};
    };

    XLOGF(DBG3, "Query request {}: start to query chunks in range: {}", fmt::ptr(&req), payload.chunkIdRange);

    auto processResult = co_await processQueryResults(requestCtx,
                                                      payload.vChainId,
                                                      payload.chunkIdRange,
                                                      processMetadata,
                                                      queryResult.moreChunksInRange);

    if (UNLIKELY(processResult.hasError())) {
      queryResult.statusCode = makeError(processResult.error());
    }

    XLOGF(DBG3,
          "Query request {}: found {} chunks in range {}, status code: {}",
          fmt::ptr(&req),
          queryResult.totalNumChunks,
          payload.chunkIdRange,
          queryResult.statusCode.hasError() ? queryResult.statusCode.error() : Status::OK);

    rsp.results.push_back(queryResult);
  }

  co_return rsp;
}

CoTryTask<TruncateChunksRsp> StorageOperator::truncateChunks(ServiceRequestContext &requestCtx,
                                                             const TruncateChunksReq &req) {
  XLOGF(INFO, "Truncate request {} with {} ops", fmt::ptr(&req), req.payloads.size());

  size_t numTruncatedChunks = 0;
  TruncateChunksRsp rsp;
  rsp.results.reserve(req.payloads.size());

  for (const auto &payload : req.payloads) {
    auto result = co_await doTruncate(requestCtx, payload, req.userInfo, req.featureFlags);
    rsp.results.push_back(result);
    numTruncatedChunks += result.lengthInfo.hasValue();
  }

  XLOGF(INFO, "Truncate request {}: {}/{} chunks truncated", fmt::ptr(&req), numTruncatedChunks, req.payloads.size());
  co_return rsp;
}

CoTryTask<RemoveChunksRsp> StorageOperator::removeChunks(ServiceRequestContext &requestCtx,
                                                         const RemoveChunksReq &req) {
  auto recordGuard = storageReqRemoveChunksRecorder.record();
  XLOGF(DBG7, "Remove request {} with {} ops", fmt::ptr(&req), req.payloads.size());

  RemoveChunksRsp rsp;
  rsp.results.reserve(req.payloads.size());

  for (const auto &payload : req.payloads) {
    auto recordGuard = storageRemoveRangeRecorder.record();
    RemoveChunksResult removeRes{Void{}, 0 /*numChunksRemoved*/, false /*moreChunksInRange*/};
    auto removeOp = payload;

    auto removeChunk = [req, &requestCtx, &removeOp, &removeRes, this](
                           const ChunkId &chunkId,
                           const ChunkMetadata &metadata) -> CoTryTask<void> {
      removeOp.chunkIdRange = {chunkId, chunkId};

      auto result = co_await doRemove(requestCtx, removeOp, req.userInfo, req.featureFlags);

      if (result.lengthInfo.hasError()) {
        if (result.lengthInfo.error().code() == StorageCode::kChunkMetadataNotFound) {
          XLOGF(WARN,
                "Chunk {} on {} is already removed by another concurrent remove request",
                chunkId,
                removeOp.vChainId.chainId);
        } else {
          co_return makeError(result.lengthInfo.error());
        }
      } else {
        removeRes.numChunksRemoved++;
      }

      removeOp.tag.channel.seqnum++;  // increment the sequence number for next remove
      co_return Void{};
    };

    XLOGF(DBG3, "Remove request {}: start to remove chunks in range: {}", fmt::ptr(&req), payload.chunkIdRange);

    auto processResult = co_await processQueryResults(requestCtx,
                                                      payload.vChainId,
                                                      payload.chunkIdRange,
                                                      removeChunk,
                                                      removeRes.moreChunksInRange);

    if (UNLIKELY(processResult.hasError())) {
      removeRes.statusCode = makeError(processResult.error());
    } else {
      storageNumChunksRemoved.addSample(removeRes.numChunksRemoved);
      recordGuard.succ();
    }

    XLOGF(DBG7,
          "Remove request {}: removed {} chunks in range {}, result: {}",
          fmt::ptr(&req),
          removeRes.numChunksRemoved,
          payload.chunkIdRange,
          removeRes);

    rsp.results.push_back(removeRes);
  }

  recordGuard.succ();
  co_return rsp;
}

CoTryTask<TargetSyncInfo> StorageOperator::syncStart(const SyncStartReq &req) {
  auto recordGuard = syncStartRecorder.record();

  // get target for sync start from predecessor.
  auto targetResult = components_.targetMap.getByChainId(req.vChainId);
  if (UNLIKELY(!targetResult)) {
    auto msg = fmt::format("sync start {} get target failed: {}", req, targetResult.error());
    XLOG(ERR, msg);
    co_return makeError(std::move(targetResult.error()));
  }

  auto target = std::move(*targetResult);
  auto targetId = target->targetId;

  if (UNLIKELY(target->publicState != flat::PublicTargetState::SYNCING)) {
    auto msg = fmt::format("target {} check state failed: {}", targetId, magic_enum::enum_name(target->publicState));
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kSyncStartFailed, std::move(msg));
  }

  if (UNLIKELY(target->localState != hf3fs::flat::LocalTargetState::ONLINE)) {
    auto msg = fmt::format("target {} check state failed: {}", targetId, serde::toJsonString(target->localState));
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kSyncStartFailed, std::move(msg));
  }

  TargetSyncInfo info;
  auto result = target->storageTarget->getAllMetadata(info.metas);
  if (UNLIKELY(!result)) {
    XLOGF(ERR, "sync start {} failed: {}", req, result.error());
    co_return makeError(std::move(result.error()));
  }

  // re-check current chain version.
  targetResult = components_.targetMap.getByChainId(req.vChainId);
  if (UNLIKELY(!targetResult)) {
    auto msg = fmt::format("sync start {} get target failed: {}", req, targetResult.error());
    XLOG(ERR, msg);
    co_return makeError(std::move(targetResult.error()));
  }

  recordGuard.succ();
  co_return Result<TargetSyncInfo>(std::move(info));
}

CoTryTask<SyncDoneRsp> StorageOperator::syncDone(const SyncDoneReq &req) {
  auto recordGuard = syncDoneRecorder.record();
  auto result = components_.targetMap.syncReceiveDone(req.vChainId);
  if (UNLIKELY(!result)) {
    XLOGF(ERR, "sync done {} failed: {}", req, result.error());
    co_return makeError(std::move(result.error()));
  }
  recordGuard.succ();
  SyncDoneRsp rsp;
  rsp.result.lengthInfo = 0;
  co_return rsp;
}

CoTryTask<SpaceInfoRsp> StorageOperator::spaceInfo(const SpaceInfoReq &req) {
  auto spaceInfoResult = components_.storageTargets.spaceInfos(req.force);
  if (UNLIKELY(!spaceInfoResult)) {
    co_return makeError(std::move(spaceInfoResult.error()));
  }
  SpaceInfoRsp rsp;
  rsp.spaceInfos = std::move(*spaceInfoResult);
  co_return rsp;
}

CoTryTask<CreateTargetRsp> StorageOperator::createTarget(const CreateTargetReq &req) {
  auto createResult = components_.storageTargets.create(req);
  if (UNLIKELY(!createResult)) {
    XLOGF(ERR, "create target {} failed {}", req, createResult.error());
    co_return makeError(std::move(createResult.error()));
  }
  co_return CreateTargetRsp{};
}

CoTryTask<OfflineTargetRsp> StorageOperator::offlineTarget(const OfflineTargetReq &req) {
  auto targetResult = components_.targetMap.getByTargetId(req.targetId);
  if (UNLIKELY(!targetResult)) {
    auto msg = fmt::format("offline target failed: {}, {}", req, targetResult.error());
    XLOG(ERR, msg);
    co_return makeError(std::move(targetResult.error()));
  }
  auto &target = **targetResult;

  if (target.isHead && target.isTail && !req.force) {
    auto msg = fmt::format("offline failed: target is the last online target! {}", target);
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
  }

  CO_RETURN_AND_LOG_ON_ERROR(components_.targetMap.offlineTarget(req.targetId));
  co_return OfflineTargetRsp{};
}

CoTryTask<RemoveTargetRsp> StorageOperator::removeTarget(const RemoveTargetReq &req) {
  // 1. get storage target.
  auto targetResult = components_.targetMap.getByTargetId(req.targetId);
  if (UNLIKELY(!targetResult)) {
    auto msg = fmt::format("remove target failed: {}, {}", req, targetResult.error());
    XLOG(ERR, msg);
    co_return makeError(std::move(targetResult.error()));
  }
  auto &target = **targetResult;

  // 2. check status.
  if (!target.unrecoverableOffline()) {
    auto msg = fmt::format("remove failed: target is not offline! {}", target);
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
  }

  if (target.vChainId != VersionedChainId{} && !req.force) {
    auto msg = fmt::format("remove failed: target is still in a chain! {}", target);
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
  }

  if (!target.weakStorageTarget.expired()) {
    auto msg = fmt::format("remove failed: target is still in use! {}", target);
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
  }

  // 3. do remove.
  if (target.useChunkEngine) {
    if (target.chainId == ChainId{}) {
      auto msg = fmt::format("remove failed: chain id is empty! {}", target);
      XLOG(ERR, msg);
      co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
    }
    auto result = components_.storageTargets.removeChunkEngineTarget(target.chainId, target.diskIndex);
    CO_RETURN_AND_LOG_ON_ERROR(result);
  }

  boost::system::error_code ec{};
  boost::filesystem::remove_all(target.path, ec);
  if (ec.failed()) {
    auto msg = fmt::format("remove failed: remove path failed! {}, {}", target, ec.message());
    XLOG(ERR, msg);
    co_return makeError(StorageCode::kTargetStateInvalid, std::move(msg));
  }

  CO_RETURN_AND_LOG_ON_ERROR(components_.targetMap.removeTarget(req.targetId));

  co_return RemoveTargetRsp{};
}

CoTryTask<QueryChunkRsp> StorageOperator::queryChunk(const QueryChunkReq &req) {
  // get target for query chunk from client.
  auto targetResult = components_.targetMap.getByChainId(VersionedChainId{req.chainId, {}}, true);
  if (UNLIKELY(!targetResult)) {
    auto msg = fmt::format("queryChunk {} get target failed: {}", req, targetResult.error());
    XLOG(ERR, msg);
    co_return makeError(std::move(targetResult.error()));
  }

  QueryChunkRsp rsp;
  rsp.target = **targetResult;
  if (rsp.target.storageTarget && !req.chunkId.data().empty()) {
    rsp.meta = rsp.target.storageTarget->queryChunk(req.chunkId);
  }
  rsp.target.storageTarget = nullptr;
  rsp.target.weakStorageTarget.reset();
  co_return rsp;
}

CoTryTask<GetAllChunkMetadataRsp> StorageOperator::getAllChunkMetadata(const GetAllChunkMetadataReq &req) {
  auto targetResult = components_.targetMap.getByTargetId(req.targetId);
  if (UNLIKELY(!targetResult)) {
    auto msg = fmt::format("get all chunk metadata: {}, get target failed: {}", req, targetResult.error());
    XLOG(ERR, msg);
    co_return makeError(std::move(targetResult.error()));
  }

  auto target = std::move(*targetResult);
  auto targetId = target->targetId;

  if (UNLIKELY(target->publicState != flat::PublicTargetState::SERVING)) {
    auto msg = fmt::format("target {} check state failed: {}", targetId, magic_enum::enum_name(target->publicState));
    XLOG(ERR, msg);
    co_return makeError(StorageClientCode::kNotAvailable, std::move(msg));
  }

  if (UNLIKELY(target->localState != hf3fs::flat::LocalTargetState::UPTODATE)) {
    auto msg = fmt::format("target {} check state failed: {}", targetId, serde::toJsonString(target->localState));
    XLOG(ERR, msg);
    co_return makeError(StorageClientCode::kNotAvailable, std::move(msg));
  }

  GetAllChunkMetadataRsp response;
  auto result = target->storageTarget->getAllMetadata(response.chunkMetaVec);
  if (UNLIKELY(!result)) {
    XLOGF(ERR, "get all chunk metadata, {} failed: {}", req, result.error());
    co_return makeError(std::move(result.error()));
  }

  co_return Result<GetAllChunkMetadataRsp>(std::move(response));
}

CoTryTask<ReplaceCacheChunksRsp> StorageOperator::replaceCacheChunks(const ReplaceCacheChunksReq &req) {
  if (req.items.size() > kMaxCacheStorageBatchItems) {
    co_return makeError(CacheCode::kRequestTooLarge, "too many cache chunks");
  }
  if (config_.enable_cache_phase2()) {
    CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, true));
  }
  ReplaceCacheChunksRsp response;
  response.results.reserve(req.items.size());
  response.descriptors.reserve(req.items.size());
  for (const auto &item : req.items) {
    std::optional<CacheChunkDescriptor> responseDescriptor;
    auto appendDescriptor = folly::makeGuard([&] { response.descriptors.push_back(std::move(responseDescriptor)); });
    auto valid = item.valid();
    if (!valid) {
      response.results.push_back(makeError(std::move(valid.error())));
      continue;
    }
    auto targetResult = components_.targetMap.getByChainId(item.key.vChainId);
    if (!targetResult) {
      response.results.push_back(makeError(std::move(targetResult.error())));
      continue;
    }
    auto target = std::move(*targetResult);
    if (!target->cacheData) {
      response.results.push_back(makeError(CacheCode::kStateConflict, "chain is not CACHE_DATA"));
      continue;
    }
    auto localItem = item;
    bool pinned = false;
    if (config_.enable_cache_phase2()) {
      if (!item.permit || !item.logicalKey) {
        response.results.push_back(makeError(CacheCode::kPermitExpired, "phase two cache replace requires a permit"));
        continue;
      }
      if (target->storageRole != StorageRole::CACHE_ONLY || target->storageTarget == nullptr) {
        response.results.push_back(makeError(CacheCode::kRoleMismatch, "cache replace target is not cache-only"));
        continue;
      }
      auto eventJournal = components_.storageTargets.cacheEventJournal(target->physicalDiskId);
      if (eventJournal == nullptr) {
        response.results.push_back(makeError(CacheCode::kUnavailable, "cache event journal is unavailable"));
        continue;
      }
      auto journalWritable = eventJournal->requireWritable();
      if (journalWritable.hasError()) {
        response.results.push_back(makeError(std::move(journalWritable.error())));
        continue;
      }
      if (!std::binary_search(item.permit->placement.expectedReplicaTargets.begin(),
                              item.permit->placement.expectedReplicaTargets.end(),
                              target->targetId)) {
        response.results.push_back(
            makeError(CacheCode::kPlacementMismatch, "local target is outside permit placement"));
        continue;
      }
      auto footprint = item.permit->footprintByTarget.find(target->targetId);
      auto actualFootprint = target->storageTarget->physicalFootprint(item.chunkSize, item.data.size());
      if (footprint == item.permit->footprintByTarget.end() || actualFootprint.hasError() ||
          footprint->second != *actualFootprint) {
        response.results.push_back(
            makeError(CacheCode::kPermitConflict, "cache replace footprint differs from permit"));
        continue;
      }
      const auto nowNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
      auto pin = pinLocalPermit(*item.permit, nowNs);
      if (pin.hasError()) {
        response.results.push_back(makeError(std::move(pin.error())));
        continue;
      }
      pinned = true;
      localItem.descriptor = CacheChunkDescriptor{*item.logicalKey,
                                                  item.cacheGeneration,
                                                  item.permit->placement,
                                                  target->targetId,
                                                  nowNs,
                                                  nowNs};
    } else if (item.permit || item.logicalKey || item.descriptor) {
      response.results.push_back(makeError(CacheCode::kFeatureDisabled, "phase two cache replace is disabled"));
      continue;
    }

    folly::coro::Baton baton;
    auto lock = target->storageTarget->lockChunk(baton, item.key.chunkId, "replaceCacheChunk");
    if (!lock.locked()) co_await lock.lock();
    auto result = target->storageTarget->replaceCacheChunk(localItem, updateWorker_.backgroundExecutor());
    if (pinned) {
      auto consumed = consumeLocalPermit(*item.permit);
      if (consumed.hasError() && result.hasValue()) result = makeError(std::move(consumed.error()));
    }
    if (!result && result.error().code() == CacheCode::kStaleGeneration) {
      storageCacheStaleReplaceCount.addSample(1);
      cache::metrics::recordCount(cache::metrics::Event::STORAGE_GENERATION_STALE, 1, {.reason = "replace"});
    }
    if (result) {
      responseDescriptor = localItem.descriptor;
      storageCacheReplaceCount.addSample(1);
      cache::metrics::recordCount(cache::metrics::Event::STORAGE_GENERATION_REPLACE, 1, {.reason = "replace"});
    }
    XLOGF_IF(WARN,
             result.hasError(),
             "Cache generation replace failed chunk {} generation {}: {}",
             item.key.chunkId,
             item.cacheGeneration,
             result.error().describe());
    response.results.push_back(std::move(result));
  }
  co_return response;
}

CoTryTask<RetireCacheChunkGenerationsRsp> StorageOperator::retireCacheChunkGenerations(
    const RetireCacheChunkGenerationsReq &req) {
  RetireCacheChunkGenerationsRsp response;
  response.results.reserve(req.items.size());
  response.descriptors.reserve(req.items.size());
  for (const auto &item : req.items) {
    std::optional<CacheChunkDescriptor> responseDescriptor;
    auto appendDescriptor = folly::makeGuard([&] { response.descriptors.push_back(std::move(responseDescriptor)); });
    auto valid = item.valid();
    if (!valid) {
      response.results.push_back(makeError(std::move(valid.error())));
      continue;
    }
    auto targetResult = components_.targetMap.getByChainId(item.key.vChainId);
    if (!targetResult) {
      response.results.push_back(makeError(std::move(targetResult.error())));
      continue;
    }
    auto target = std::move(*targetResult);
    if (!target->cacheData) {
      response.results.push_back(makeError(CacheCode::kStateConflict, "chain is not CACHE_DATA"));
      continue;
    }

    folly::coro::Baton baton;
    auto lock = target->storageTarget->lockChunk(baton, item.key.chunkId, "retireCacheChunk");
    if (!lock.locked()) co_await lock.lock();
    auto result = target->storageTarget->retireCacheChunk(item);
    if (result) {
      auto descriptor = target->storageTarget->queryCacheChunkDescriptor(item.key.chunkId);
      if (descriptor) responseDescriptor = std::move(*descriptor);
      storageCacheRetireCount.addSample(1);
      storageCacheTombstoneCount.addSample(1);
      cache::metrics::recordCount(cache::metrics::Event::STORAGE_TOMBSTONE, 1, {.reason = "retire"});
    }
    XLOGF_IF(WARN,
             result.hasError(),
             "Cache tombstone failed chunk {} generation {}: {}",
             item.key.chunkId,
             item.expectedGeneration,
             result.error().describe());
    response.results.push_back(std::move(result));
  }
  co_return response;
}

CoTryTask<QueryCacheChunkGenerationsRsp> StorageOperator::queryCacheChunkGenerations(
    const QueryCacheChunkGenerationsReq &req) {
  QueryCacheChunkGenerationsRsp response;
  response.results.reserve(req.keys.size());
  response.descriptors.reserve(req.keys.size());
  for (const auto &key : req.keys) {
    std::optional<CacheChunkDescriptor> responseDescriptor;
    auto appendDescriptor = folly::makeGuard([&] { response.descriptors.push_back(std::move(responseDescriptor)); });
    auto valid = key.valid();
    if (!valid) {
      response.results.push_back(makeError(std::move(valid.error())));
      continue;
    }
    auto targetResult = components_.targetMap.getByChainId(key.vChainId);
    if (!targetResult) {
      response.results.push_back(makeError(std::move(targetResult.error())));
      continue;
    }
    auto target = std::move(*targetResult);
    if (!target->cacheData) {
      response.results.push_back(makeError(CacheCode::kStateConflict, "chain is not CACHE_DATA"));
      continue;
    }
    auto result = target->storageTarget->queryCacheChunk(key.chunkId);
    if (result) {
      auto descriptor = target->storageTarget->queryCacheChunkDescriptor(key.chunkId);
      if (descriptor) responseDescriptor = std::move(*descriptor);
    }
    response.results.push_back(std::move(result));
  }
  co_return response;
}

Result<std::vector<StorageOperator::LocalPermitDisk>> StorageOperator::resolveLocalPermitDisks(
    const PermitIdentity &permit,
    uint64_t nowNs) {
  RETURN_ON_ERROR(permit.valid());
  std::map<PhysicalDiskId, LocalPermitDisk> disks;
  auto targets = components_.targetMap.snapshot();
  for (const auto &[targetId, footprint] : permit.footprintByTarget) {
    auto target = targets->getTargets().find(targetId);
    if (target == targets->getTargets().end()) continue;
    if (target->second.storageRole != StorageRole::CACHE_ONLY || target->second.storageTarget == nullptr)
      return makeError(CacheCode::kRoleMismatch, "permit target is not cache-only");
    auto &disk = disks[target->second.physicalDiskId];
    disk.diskId = target->second.physicalDiskId;
    disk.gate = components_.storageTargets.cacheSpaceGate(disk.diskId);
    if (disk.gate == nullptr) return makeError(CacheCode::kUnavailable, "cache space gate is unavailable");
    if (footprint > std::numeric_limits<uint64_t>::max() - disk.footprintBytes)
      return makeError(CacheCode::kPermitConflict, "permit footprint overflow");
    disk.footprintBytes += footprint;
  }
  if (disks.empty()) return makeError(CacheCode::kPlacementMismatch, "permit has no target on this storage node");

  CHECK_RESULT(spaceInfos, components_.storageTargets.spaceInfos(true));
  for (auto &[diskId, disk] : disks) {
    auto expectedDiskId = diskId;
    auto info = std::find_if(spaceInfos.begin(), spaceInfos.end(), [expectedDiskId](const SpaceInfo &candidate) {
      return candidate.physicalDiskId == expectedDiskId;
    });
    if (info == spaceInfos.end()) return makeError(CacheCode::kUnavailable, "cache disk space snapshot is missing");
    CHECK_RESULT(currentPermits, disk.gate->reservedBytes(nowNs));
    disk.capacity.capacityBytes = info->cacheCapacityBytes;
    disk.capacity.physicalUsedBytes = info->cachePhysicalUsedBytes;
    disk.capacity.reservedBytes =
        info->cacheReservedBytes >= currentPermits ? info->cacheReservedBytes - currentPermits : uint64_t{0};
    disk.capacity.allocatableBytes = info->cacheAllocatableBytes;
    disk.highWatermark = info->enforcedAdmissionHighWatermark;
  }
  std::vector<LocalPermitDisk> result;
  result.reserve(disks.size());
  for (auto &[_, disk] : disks) result.push_back(std::move(disk));
  return result;
}

Result<std::vector<CacheSpaceGate *>> StorageOperator::resolveLocalPermitGates(const PermitIdentity &permit) const {
  RETURN_ON_ERROR(permit.valid());
  std::map<PhysicalDiskId, CacheSpaceGate *> disks;
  auto targets = components_.targetMap.snapshot();
  for (const auto &[targetId, _] : permit.footprintByTarget) {
    auto target = targets->getTargets().find(targetId);
    if (target == targets->getTargets().end()) continue;
    if (target->second.storageRole != StorageRole::CACHE_ONLY)
      return makeError(CacheCode::kRoleMismatch, "permit target is not cache-only");
    auto gate = components_.storageTargets.cacheSpaceGate(target->second.physicalDiskId);
    if (gate == nullptr) return makeError(CacheCode::kUnavailable, "cache space gate is unavailable");
    disks.emplace(target->second.physicalDiskId, gate);
  }
  if (disks.empty()) return makeError(CacheCode::kPlacementMismatch, "permit has no target on this storage node");
  std::vector<CacheSpaceGate *> result;
  result.reserve(disks.size());
  for (auto [_, gate] : disks) result.push_back(gate);
  return result;
}

Result<std::vector<StorageOperator::PermitReplicaNode>> StorageOperator::resolvePermitReplicaNodes(
    const PermitIdentity &permit,
    bool &localIsCoordinator,
    bool requireCurrentPlacement) const {
  RETURN_ON_ERROR(permit.valid());
  auto mgmtd = components_.mgmtdClient.load();
  if (!mgmtd) return makeError(CacheCode::kUnavailable, "routing client is unavailable");
  auto routing = mgmtd->getRoutingInfo();
  if (!routing || !routing->raw()) return makeError(CacheCode::kUnavailable, "routing info is unavailable");
  if (requireCurrentPlacement) {
    auto chain = routing->getChain(permit.placement.versionedChain.chainId);
    if (!chain || chain->chainVersion != permit.placement.versionedChain.chainVer)
      return makeError(CacheCode::kPlacementMismatch, "permit chain version changed");
    std::vector<TargetId> routingTargets;
    routingTargets.reserve(chain->targets.size());
    for (const auto &target : chain->targets) routingTargets.push_back(target.targetId);
    std::sort(routingTargets.begin(), routingTargets.end());
    if (routingTargets != permit.placement.expectedReplicaTargets)
      return makeError(CacheCode::kPlacementMismatch, "permit replica set changed");
  }

  std::map<NodeId, PermitReplicaNode> nodes;
  std::optional<NodeId> coordinatorNode;
  for (auto targetId : permit.placement.expectedReplicaTargets) {
    auto target = routing->getTarget(targetId);
    if (!target || !target->nodeId ||
        (requireCurrentPlacement && (target->chainId != permit.placement.versionedChain.chainId ||
                                     target->publicState != flat::PublicTargetState::SERVING)))
      return makeError(CacheCode::kPlacementMismatch, "permit target is not serving on the recorded chain");
    if (target->storageRole != StorageRole::CACHE_ONLY)
      return makeError(CacheCode::kRoleMismatch, "permit target is not cache-only");
    auto node = routing->getNode(*target->nodeId);
    if (!node || node->type != flat::NodeType::STORAGE)
      return makeError(CacheCode::kUnavailable, "permit storage node is unavailable");
    auto &replicaNode = nodes[*target->nodeId];
    replicaNode.nodeId = *target->nodeId;
    replicaNode.local = *target->nodeId == components_.getAppInfo().nodeId;
    if (!replicaNode.local && !replicaNode.address) {
      auto addresses = node->extractAddresses("StorageSerde");
      if (addresses.empty()) return makeError(CacheCode::kUnavailable, "permit storage node has no address");
      replicaNode.address = addresses.front();
    }
    if (targetId == permit.placement.coordinatorTargetId) coordinatorNode = *target->nodeId;
  }
  if (!coordinatorNode) return makeError(CacheCode::kPlacementMismatch, "permit coordinator target is missing");
  localIsCoordinator = *coordinatorNode == components_.getAppInfo().nodeId;
  std::vector<PermitReplicaNode> result;
  result.reserve(nodes.size());
  for (auto &[_, node] : nodes) result.push_back(std::move(node));
  return result;
}

Result<CachePermitResult> StorageOperator::prepareLocalPermit(const CachePermitRequestItem &item, uint64_t nowNs) {
  CHECK_RESULT(disks, resolveLocalPermitDisks(item.permit, nowNs));
  std::vector<CacheSpaceGate *> newlyPrepared;
  Result<CachePermitResult> result = makeError(CacheCode::kUnavailable);
  for (auto &disk : disks) {
    bool existed = disk.gate->query(item.permit, nowNs).hasValue();
    result = disk.gate->prepare(item, disk.footprintBytes, disk.capacity, disk.highWatermark, nowNs);
    if (!result) break;
    if (!existed) newlyPrepared.push_back(disk.gate);
  }
  if (!result) {
    for (auto *gate : newlyPrepared) {
      auto rollback = gate->release(item.permit, nowNs);
      XLOGF_IF(ERR, rollback.hasError(), "rollback local cache permit failed: {}", rollback.error());
    }
  }
  return result;
}

Result<CachePermitResult> StorageOperator::renewLocalPermit(const CachePermitRequestItem &item, uint64_t nowNs) {
  CHECK_RESULT(disks, resolveLocalPermitDisks(item.permit, nowNs));
  Result<CachePermitResult> result = makeError(CacheCode::kUnavailable);
  for (auto &disk : disks) {
    result = disk.gate->renew(item, nowNs);
    if (!result) break;
  }
  return result;
}

Result<Void> StorageOperator::releaseLocalPermit(const PermitIdentity &permit, uint64_t nowNs) {
  CHECK_RESULT(gates, resolveLocalPermitGates(permit));
  Result<Void> result = Void{};
  for (auto *gate : gates) {
    result = gate->release(permit, nowNs);
    if (!result) break;
  }
  return result;
}

Result<CachePermitResult> StorageOperator::queryLocalPermit(const PermitIdentity &permit, uint64_t nowNs) {
  CHECK_RESULT(gates, resolveLocalPermitGates(permit));
  Result<CachePermitResult> result = makeError(CacheCode::kNotFound);
  for (auto *gate : gates) {
    result = gate->query(permit, nowNs);
    if (!result) break;
  }
  return result;
}

Result<CachePermitResult> StorageOperator::pinLocalPermit(const PermitIdentity &permit, uint64_t nowNs) {
  CHECK_RESULT(gates, resolveLocalPermitGates(permit));
  std::vector<CacheSpaceGate *> pinned;
  Result<CachePermitResult> result = makeError(CacheCode::kUnavailable);
  for (auto *gate : gates) {
    result = gate->pin(permit, nowNs);
    if (!result) break;
    pinned.push_back(gate);
  }
  if (!result) {
    for (auto *gate : pinned) {
      auto rollback = gate->consume(permit);
      XLOGF_IF(ERR, rollback.hasError(), "rollback pinned cache permit failed: {}", rollback.error());
    }
  }
  return result;
}

Result<Void> StorageOperator::consumeLocalPermit(const PermitIdentity &permit) {
  CHECK_RESULT(gates, resolveLocalPermitGates(permit));
  Result<Void> result = Void{};
  for (auto *gate : gates) {
    auto consumed = gate->consume(permit);
    if (consumed.hasError() && result.hasValue()) result = makeError(std::move(consumed.error()));
  }
  return result;
}

#define PERMIT_REPLICA_METHOD(NAME, RESULT, ITEM, REQ, RSP, FIELD, MESSENGER, LOCAL)                   \
  CoTryTask<RESULT> StorageOperator::NAME(const PermitReplicaNode &node,                               \
                                          const ITEM &item,                                            \
                                          const flat::UserInfo &userInfo,                              \
                                          uint32_t cacheProtocolVersion,                               \
                                          uint64_t nowNs) {                                            \
    if (node.local) co_return LOCAL(item, nowNs);                                                      \
    if (!node.address) co_return makeError(CacheCode::kUnavailable, "permit replica address missing"); \
    REQ request;                                                                                       \
    request.userInfo = userInfo;                                                                       \
    request.FIELD.push_back(item);                                                                     \
    request.cacheProtocolVersion = cacheProtocolVersion;                                               \
    auto response = co_await components_.messenger.MESSENGER(*node.address, request);                  \
    CO_RETURN_ON_ERROR(response);                                                                      \
    if (response->results.size() != 1)                                                                 \
      co_return makeError(CacheCode::kInvalidResponse, "permit replica result count mismatch");        \
    co_return std::move(response->results.front());                                                    \
  }
PERMIT_REPLICA_METHOD(preparePermitOnReplica,
                      CachePermitResult,
                      CachePermitRequestItem,
                      PrepareCachePermitsReq,
                      PrepareCachePermitsRsp,
                      items,
                      prepareCachePermits,
                      prepareLocalPermit);
PERMIT_REPLICA_METHOD(renewPermitOnReplica,
                      CachePermitResult,
                      CachePermitRequestItem,
                      RenewCachePermitsReq,
                      RenewCachePermitsRsp,
                      items,
                      renewCachePermits,
                      renewLocalPermit);
PERMIT_REPLICA_METHOD(releasePermitOnReplica,
                      Void,
                      PermitIdentity,
                      ReleaseCachePermitsReq,
                      ReleaseCachePermitsRsp,
                      permits,
                      releaseCachePermits,
                      releaseLocalPermit);
PERMIT_REPLICA_METHOD(queryPermitOnReplica,
                      CachePermitResult,
                      PermitIdentity,
                      QueryCachePermitsReq,
                      QueryCachePermitsRsp,
                      permits,
                      queryCachePermits,
                      queryLocalPermit);
#undef PERMIT_REPLICA_METHOD

CoTryTask<QueryCacheSpaceRsp> StorageOperator::queryCacheSpace(const QueryCacheSpaceReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  auto spaceInfosResult = components_.storageTargets.spaceInfos(true);
  CO_RETURN_ON_ERROR(spaceInfosResult);
  auto &spaceInfos = *spaceInfosResult;

  auto convert = [](const SpaceInfo &info) {
    return CacheSpaceInfo{info.physicalDiskId,
                          info.storageRole,
                          info.targetIds,
                          info.cacheCapacityBytes,
                          info.cachePhysicalUsedBytes,
                          info.cacheAllocatableBytes,
                          info.cacheReservedBytes,
                          info.enforcedAdmissionHighWatermark,
                          info.sampledAtNs};
  };
  QueryCacheSpaceRsp response;
  if (req.targetIds.empty()) {
    for (const auto &info : spaceInfos) {
      if (info.storageRole == StorageRole::CACHE_ONLY) response.results.emplace_back(convert(info));
    }
  } else {
    response.results.reserve(req.targetIds.size());
    for (auto targetId : req.targetIds) {
      auto info = std::find_if(spaceInfos.begin(), spaceInfos.end(), [targetId](const auto &candidate) {
        return std::find(candidate.targetIds.begin(), candidate.targetIds.end(), targetId) != candidate.targetIds.end();
      });
      if (info == spaceInfos.end()) {
        response.results.emplace_back(makeError(CacheCode::kNotFound, "cache target has no physical space snapshot"));
      } else if (info->storageRole != StorageRole::CACHE_ONLY) {
        response.results.emplace_back(makeError(CacheCode::kRoleMismatch, "target is not on a cache-only disk"));
      } else {
        response.results.emplace_back(convert(*info));
      }
    }
  }
  response.footprintResults.reserve(req.footprints.size());
  for (const auto &query : req.footprints) {
    auto target = components_.targetMap.getByTargetId(query.targetId);
    if (!target) {
      response.footprintResults.emplace_back(makeError(std::move(target.error())));
      continue;
    }
    if ((*target)->storageRole != StorageRole::CACHE_ONLY || (*target)->storageTarget == nullptr) {
      response.footprintResults.emplace_back(makeError(CacheCode::kRoleMismatch, "target is not cache-only"));
      continue;
    }
    auto footprint = (*target)->storageTarget->physicalFootprint(query.chunkSize, query.payloadLength);
    if (!footprint) {
      response.footprintResults.emplace_back(makeError(std::move(footprint.error())));
      continue;
    }
    response.footprintResults.emplace_back(CacheFootprintInfo{query.targetId, (*target)->physicalDiskId, *footprint});
  }
  co_return response;
}

CoTryTask<PrepareCachePermitsRsp> StorageOperator::prepareCachePermits(const PrepareCachePermitsReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  const auto nowNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
  PrepareCachePermitsRsp response;
  response.results.reserve(req.items.size());
  for (const auto &item : req.items) {
    bool localIsCoordinator = false;
    auto nodes = resolvePermitReplicaNodes(item.permit, localIsCoordinator, true);
    if (!nodes) {
      response.results.emplace_back(makeError(std::move(nodes.error())));
      continue;
    }
    if (!localIsCoordinator) {
      response.results.emplace_back(prepareLocalPermit(item, nowNs));
      continue;
    }

    folly::coro::Baton baton;
    auto lock = permitCoordinatorLocks_.lock(baton, serde::serializeBytes(item.permit).toString());
    co_await lock.lock();
    auto result = co_await CachePermitCoordinator::prepare<PermitReplicaNode>(
        *nodes,
        [&](const auto &node) {
          return queryPermitOnReplica(node, item.permit, req.userInfo, req.cacheProtocolVersion, nowNs);
        },
        [&](const auto &node) {
          return preparePermitOnReplica(node, item, req.userInfo, req.cacheProtocolVersion, nowNs);
        },
        [&](const auto &node) {
          return releasePermitOnReplica(node, item.permit, req.userInfo, req.cacheProtocolVersion, nowNs);
        });
    response.results.emplace_back(std::move(result));
  }
  co_return response;
}

CoTryTask<RenewCachePermitsRsp> StorageOperator::renewCachePermits(const RenewCachePermitsReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  const auto nowNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
  RenewCachePermitsRsp response;
  response.results.reserve(req.items.size());
  for (const auto &item : req.items) {
    bool localIsCoordinator = false;
    auto nodes = resolvePermitReplicaNodes(item.permit, localIsCoordinator, true);
    if (!nodes) {
      response.results.emplace_back(makeError(std::move(nodes.error())));
      continue;
    }
    if (!localIsCoordinator) {
      response.results.emplace_back(renewLocalPermit(item, nowNs));
      continue;
    }
    folly::coro::Baton baton;
    auto lock = permitCoordinatorLocks_.lock(baton, serde::serializeBytes(item.permit).toString());
    co_await lock.lock();
    auto result = co_await CachePermitCoordinator::renew<PermitReplicaNode>(*nodes, [&](const auto &node) {
      return renewPermitOnReplica(node, item, req.userInfo, req.cacheProtocolVersion, nowNs);
    });
    response.results.emplace_back(std::move(result));
  }
  co_return response;
}

CoTryTask<ReleaseCachePermitsRsp> StorageOperator::releaseCachePermits(const ReleaseCachePermitsReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  const auto nowNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
  ReleaseCachePermitsRsp response;
  response.results.reserve(req.permits.size());
  for (const auto &permit : req.permits) {
    bool localIsCoordinator = false;
    auto nodes = resolvePermitReplicaNodes(permit, localIsCoordinator, false);
    if (!nodes) {
      response.results.emplace_back(makeError(std::move(nodes.error())));
      continue;
    }
    if (!localIsCoordinator) {
      response.results.emplace_back(releaseLocalPermit(permit, nowNs));
      continue;
    }
    folly::coro::Baton baton;
    auto lock = permitCoordinatorLocks_.lock(baton, serde::serializeBytes(permit).toString());
    co_await lock.lock();
    auto result = co_await CachePermitCoordinator::release<PermitReplicaNode>(*nodes, [&](const auto &node) {
      return releasePermitOnReplica(node, permit, req.userInfo, req.cacheProtocolVersion, nowNs);
    });
    response.results.emplace_back(std::move(result));
  }
  co_return response;
}

CoTryTask<QueryCachePermitsRsp> StorageOperator::queryCachePermits(const QueryCachePermitsReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  const auto nowNs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) * 1000;
  QueryCachePermitsRsp response;
  response.results.reserve(req.permits.size());
  for (const auto &permit : req.permits) {
    bool localIsCoordinator = false;
    auto nodes = resolvePermitReplicaNodes(permit, localIsCoordinator, false);
    if (!nodes) {
      response.results.emplace_back(makeError(std::move(nodes.error())));
      continue;
    }
    if (!localIsCoordinator) {
      response.results.emplace_back(queryLocalPermit(permit, nowNs));
      continue;
    }
    folly::coro::Baton baton;
    auto lock = permitCoordinatorLocks_.lock(baton, serde::serializeBytes(permit).toString());
    co_await lock.lock();
    response.results.emplace_back(
        co_await CachePermitCoordinator::query<PermitReplicaNode>(*nodes, [&](const auto &node) {
          return queryPermitOnReplica(node, permit, req.userInfo, req.cacheProtocolVersion, nowNs);
        }));
  }
  co_return response;
}

CoTryTask<RetireCacheReplicasRsp> StorageOperator::retireCacheReplicas(const RetireCacheReplicasReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  RetireCacheReplicasRsp response;
  response.results.reserve(req.items.size());
  for (const auto &item : req.items) {
    auto targetResult = components_.targetMap.getByChainId(item.key.vChainId);
    if (targetResult.hasError()) {
      response.results.emplace_back(makeError(std::move(targetResult.error())));
      continue;
    }
    auto target = std::move(*targetResult);
    if (!target->cacheData || target->storageRole != StorageRole::CACHE_ONLY || target->storageTarget == nullptr) {
      response.results.emplace_back(makeError(CacheCode::kRoleMismatch, "replica retire target is not cache-only"));
      continue;
    }
    if (!std::binary_search(item.placement.expectedReplicaTargets.begin(),
                            item.placement.expectedReplicaTargets.end(),
                            target->targetId)) {
      response.results.emplace_back(
          makeError(CacheCode::kPlacementMismatch, "local retire target is outside the recorded placement"));
      continue;
    }

    folly::coro::Baton baton;
    auto lock = target->storageTarget->lockChunk(baton, item.key.chunkId, "retireCacheReplica");
    if (!lock.locked()) co_await lock.lock();
    auto current = target->storageTarget->queryCacheChunk(item.key.chunkId);
    if (current && current->cacheGeneration > item.expectedGeneration) {
      response.results.emplace_back(makeError(CacheCode::kGenerationAdvanced));
      continue;
    }
    if (current && current->cacheGeneration == item.expectedGeneration) {
      auto descriptor = target->storageTarget->queryCacheChunkDescriptor(item.key.chunkId);
      if (descriptor.hasError() && descriptor.error().code() != CacheCode::kNotFound) {
        response.results.emplace_back(makeError(std::move(descriptor.error())));
        continue;
      }
      if (descriptor && descriptor->has_value() &&
          ((*descriptor)->generation != item.expectedGeneration || (*descriptor)->placement != item.placement)) {
        response.results.emplace_back(
            makeError(CacheCode::kPlacementMismatch, "replica retire identity differs from cache descriptor"));
        continue;
      }
    } else if (current.hasError() && current.error().code() != CacheCode::kNotFound) {
      response.results.emplace_back(makeError(std::move(current.error())));
      continue;
    }

    RetireCacheChunkItem retire{item.key, item.expectedGeneration, item.operationId};
    auto retired = target->storageTarget->retireCacheChunkDurable(retire);
    if (retired.hasError()) {
      response.results.emplace_back(makeError(std::move(retired.error())));
      continue;
    }
    if (!retired->retired || retired->cacheGeneration != item.expectedGeneration) {
      response.results.emplace_back(
          makeError(CacheCode::kStateConflict, "replica retirement did not produce a durable tombstone"));
      continue;
    }
    storageCacheRetireCount.addSample(1);
    storageCacheTombstoneCount.addSample(1);
    cache::metrics::recordCount(cache::metrics::Event::STORAGE_TOMBSTONE, 1, {.reason = "durable_retire"});
    response.results.emplace_back(RetireCacheReplicaResult{item.operationId, true});
  }
  co_return response;
}

CoTryTask<CoordinateCacheRetiresRsp> StorageOperator::coordinateCacheRetires(const CoordinateCacheRetiresReq &req) {
  CO_RETURN_ON_ERROR(req.valid());
  CO_RETURN_ON_ERROR(cache::checkPhase2Capability(req.cacheProtocolVersion, config_.enable_cache_phase2()));
  co_return makeError(StatusCode::kNotImplemented, "coordinateCacheRetires is not implemented");
}

}  // namespace hf3fs::storage
