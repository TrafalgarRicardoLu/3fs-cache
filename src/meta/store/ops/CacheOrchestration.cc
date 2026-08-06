#include <limits>
#include <memory>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "meta/store/MetaStore.h"
#include "meta/store/Operation.h"
#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/PinStore.h"
#include "meta/store/cache/PrefetchCancellationStore.h"
#include "meta/store/cache/PrefetchJobStateMachine.h"
#include "meta/store/cache/PrefetchJobStore.h"
#include "meta/store/cache/PrefetchPlanStore.h"
#include "meta/store/cache/PrefetchReadyStore.h"

namespace hf3fs::meta::server {

class CreatePrefetchJobOp : public Operation<CreatePrefetchJobRsp> {
 public:
  CreatePrefetchJobOp(MetaStore &meta, const CreatePrefetchJobReq &req)
      : Operation<CreatePrefetchJobRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<CreatePrefetchJobRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto created = co_await PrefetchJobStore::create(txn, req_.job);
    CO_RETURN_ON_ERROR(created);
    CreatePrefetchJobRsp response;
    response.job = std::move(created->job);
    response.created = created->created;
    co_return response;
  }

 private:
  const CreatePrefetchJobReq &req_;
};

class GetPrefetchJobOp : public ReadOnlyOperation<GetPrefetchJobRsp> {
 public:
  GetPrefetchJobOp(MetaStore &meta, const GetPrefetchJobReq &req)
      : ReadOnlyOperation<GetPrefetchJobRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<GetPrefetchJobRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto job = co_await PrefetchJobStore::snapshotLoad(txn, req_.jobId);
    CO_RETURN_ON_ERROR(job);
    if (!job->has_value()) co_return makeError(CacheCode::kNotFound, "prefetch job not found");
    GetPrefetchJobRsp response;
    response.job = std::move(**job);
    co_return response;
  }

 private:
  const GetPrefetchJobReq &req_;
};

class ListPrefetchJobsOp : public ReadOnlyOperation<ListPrefetchJobsRsp> {
 public:
  ListPrefetchJobsOp(MetaStore &meta, const ListPrefetchJobsReq &req)
      : ReadOnlyOperation<ListPrefetchJobsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListPrefetchJobsRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await PrefetchJobStore::snapshotList(txn, req_.ownerUid, req_.after, req_.limit);
    CO_RETURN_ON_ERROR(page);
    ListPrefetchJobsRsp response;
    response.jobs = std::move(page->jobs);
    response.more = page->more;
    co_return response;
  }

 private:
  const ListPrefetchJobsReq &req_;
};

class UpdatePrefetchJobOp : public Operation<UpdatePrefetchJobRsp> {
 public:
  UpdatePrefetchJobOp(MetaStore &meta, const UpdatePrefetchJobReq &req)
      : Operation<UpdatePrefetchJobRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<UpdatePrefetchJobRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto job = co_await PrefetchJobStore::update(txn, req_.expectedStateVersion, req_.job);
    CO_RETURN_ON_ERROR(job);
    UpdatePrefetchJobRsp response;
    response.job = std::move(*job);
    co_return response;
  }

 private:
  const UpdatePrefetchJobReq &req_;
};

class AppendPrefetchPlanOp : public Operation<AppendPrefetchPlanRsp> {
 public:
  AppendPrefetchPlanOp(MetaStore &meta, const AppendPrefetchPlanReq &req)
      : Operation<AppendPrefetchPlanRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<AppendPrefetchPlanRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto appended = co_await PrefetchPlanStore::append(txn,
                                                       req_.jobId,
                                                       req_.entries,
                                                       req_.plannerSourceIndex,
                                                       req_.plannerCursor,
                                                       req_.planningComplete);
    CO_RETURN_ON_ERROR(appended);
    AppendPrefetchPlanRsp response;
    response.insertedBlocks = appended->insertedBlocks;
    response.insertedBytes = appended->insertedBytes;
    co_return response;
  }

 private:
  const AppendPrefetchPlanReq &req_;
};

class ListPrefetchPlanOp : public ReadOnlyOperation<ListPrefetchPlanRsp> {
 public:
  ListPrefetchPlanOp(MetaStore &meta, const ListPrefetchPlanReq &req)
      : ReadOnlyOperation<ListPrefetchPlanRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListPrefetchPlanRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await PrefetchPlanStore::snapshotList(txn, req_.jobId, req_.after, req_.limit);
    CO_RETURN_ON_ERROR(page);
    ListPrefetchPlanRsp response;
    response.entries = std::move(page->entries);
    response.more = page->more;
    co_return response;
  }

 private:
  const ListPrefetchPlanReq &req_;
};

class UpdatePrefetchPlanEntriesOp : public Operation<UpdatePrefetchPlanEntriesRsp> {
 public:
  UpdatePrefetchPlanEntriesOp(MetaStore &meta, const UpdatePrefetchPlanEntriesReq &req)
      : Operation<UpdatePrefetchPlanEntriesRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<UpdatePrefetchPlanEntriesRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    UpdatePrefetchPlanEntriesRsp response;
    response.results.reserve(req_.items.size());
    for (const auto &item : req_.items) {
      response.results.emplace_back(co_await PrefetchPlanStore::update(txn, item.expected, item.desired));
    }
    co_return response;
  }

 private:
  const UpdatePrefetchPlanEntriesReq &req_;
};

class TrackPrefetchReadyOp : public Operation<TrackPrefetchReadyRsp> {
 public:
  TrackPrefetchReadyOp(MetaStore &meta, const TrackPrefetchReadyReq &req)
      : Operation<TrackPrefetchReadyRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<TrackPrefetchReadyRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto tracked = co_await PrefetchReadyStore::track(txn, req_.jobId, req_.after, req_.limit);
    CO_RETURN_ON_ERROR(tracked);
    TrackPrefetchReadyRsp response;
    response.job = std::move(tracked->job);
    response.currentReadyBytes = tracked->currentReadyBytes;
    response.currentReadyBlocks = tracked->currentReadyBlocks;
    response.more = tracked->more;
    response.nextAfter = tracked->nextAfter;
    co_return response;
  }

 private:
  const TrackPrefetchReadyReq &req_;
};

class AdvancePrefetchJobStateOp : public Operation<AdvancePrefetchJobStateRsp> {
 public:
  AdvancePrefetchJobStateOp(MetaStore &meta, const AdvancePrefetchJobStateReq &req)
      : Operation<AdvancePrefetchJobStateRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<AdvancePrefetchJobStateRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto job = co_await PrefetchJobStateMachine::advance(txn, req_.jobId, req_.expectedStateVersion);
    CO_RETURN_ON_ERROR(job);
    AdvancePrefetchJobStateRsp response;
    response.achievedReadyBps = prefetchReadyRatioBps(job->readyBytes, job->plannedBytes);
    response.job = std::move(*job);
    co_return response;
  }

 private:
  const AdvancePrefetchJobStateReq &req_;
};

class CancelPrefetchJobOp : public Operation<CancelPrefetchJobRsp> {
 public:
  CancelPrefetchJobOp(MetaStore &meta, const CancelPrefetchJobReq &req)
      : Operation<CancelPrefetchJobRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<CancelPrefetchJobRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto job = co_await PrefetchCancellationStore::cancel(txn, req_.jobId);
    CO_RETURN_ON_ERROR(job);
    CancelPrefetchJobRsp response;
    response.job = std::move(*job);
    co_return response;
  }

 private:
  const CancelPrefetchJobReq &req_;
};

class UpsertCachePinsOp : public Operation<UpsertCachePinsRsp> {
 public:
  UpsertCachePinsOp(MetaStore &meta, const UpsertCachePinsReq &req)
      : Operation<UpsertCachePinsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<UpsertCachePinsRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    UpsertCachePinsRsp response;
    response.results.reserve(req_.pins.size());
    for (const auto &pin : req_.pins) response.results.emplace_back(co_await upsert(txn, pin));
    co_return response;
  }

 private:
  CoTryTask<cache::PinRecord> upsert(IReadWriteTransaction &txn, const cache::PinRecord &pin) {
    if (pin.cacheGeneration != cache::CacheGeneration{}) {
      auto block = co_await CacheBlockStore::load(txn, pin.key);
      CO_RETURN_ON_ERROR(block);
      if (!block->has_value() || (*block)->state != cache::CacheBlockState::READY ||
          (*block)->cacheGeneration != pin.cacheGeneration) {
        co_return makeError(CacheCode::kStateConflict, "cache pin generation fence changed");
      }
    }
    co_return co_await PinStore::upsert(txn, pin);
  }

  const UpsertCachePinsReq &req_;
};

class RemoveCachePinsOp : public Operation<RemoveCachePinsRsp> {
 public:
  RemoveCachePinsOp(MetaStore &meta, const RemoveCachePinsReq &req)
      : Operation<RemoveCachePinsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<RemoveCachePinsRsp> run(IReadWriteTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto removed = co_await PinStore::removeByOwner(txn, req_.owner, req_.keys);
    CO_RETURN_ON_ERROR(removed);
    RemoveCachePinsRsp response;
    response.removed = *removed;
    co_return response;
  }

 private:
  const RemoveCachePinsReq &req_;
};

class ListCachePinsByOwnerOp : public ReadOnlyOperation<ListCachePinsByOwnerRsp> {
 public:
  ListCachePinsByOwnerOp(MetaStore &meta, const ListCachePinsByOwnerReq &req)
      : ReadOnlyOperation<ListCachePinsByOwnerRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<ListCachePinsByOwnerRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto page = co_await PinStore::snapshotListByOwner(txn, req_.owner, req_.after, req_.limit);
    CO_RETURN_ON_ERROR(page);
    ListCachePinsByOwnerRsp response;
    response.pins = std::move(page->pins);
    response.more = page->more;
    co_return response;
  }

 private:
  const ListCachePinsByOwnerReq &req_;
};

class QueryCachePinsOp : public ReadOnlyOperation<QueryCachePinsRsp> {
 public:
  QueryCachePinsOp(MetaStore &meta, const QueryCachePinsReq &req)
      : ReadOnlyOperation<QueryCachePinsRsp>(meta),
        req_(req) {}

  OPERATION_TAGS(req_);

  CoTryTask<QueryCachePinsRsp> run(IReadOnlyTransaction &txn) override {
    CHECK_REQUEST(req_);
    auto nowUs = now().toMicroseconds();
    if (nowUs <= 0) co_return makeError(StatusCode::kInvalidArg, "metadata clock is invalid");
    auto nowMs = static_cast<uint64_t>(nowUs) / 1000;
    QueryCachePinsRsp response;
    response.results.reserve(req_.keys.size());
    for (const auto &key : req_.keys) {
      auto active = co_await PinStore::snapshotQueryActive(txn, key, nowMs);
      if (active.hasError()) {
        response.results.emplace_back(makeError(active.error()));
      } else if (active->size() > std::numeric_limits<uint32_t>::max()) {
        response.results.emplace_back(makeError(StatusCode::kDataCorruption, "cache pin owner count overflow"));
      } else {
        response.results.emplace_back(
            CachePinQueryResult{key, !active->empty(), static_cast<uint32_t>(active->size())});
      }
    }
    co_return response;
  }

 private:
  const QueryCachePinsReq &req_;
};

MetaStore::OpPtr<CreatePrefetchJobRsp> MetaStore::createPrefetchJob(const CreatePrefetchJobReq &req) {
  return std::make_unique<CreatePrefetchJobOp>(*this, req);
}

MetaStore::OpPtr<GetPrefetchJobRsp> MetaStore::getPrefetchJob(const GetPrefetchJobReq &req) {
  return std::make_unique<GetPrefetchJobOp>(*this, req);
}

MetaStore::OpPtr<ListPrefetchJobsRsp> MetaStore::listPrefetchJobs(const ListPrefetchJobsReq &req) {
  return std::make_unique<ListPrefetchJobsOp>(*this, req);
}

MetaStore::OpPtr<UpdatePrefetchJobRsp> MetaStore::updatePrefetchJob(const UpdatePrefetchJobReq &req) {
  return std::make_unique<UpdatePrefetchJobOp>(*this, req);
}

MetaStore::OpPtr<AppendPrefetchPlanRsp> MetaStore::appendPrefetchPlan(const AppendPrefetchPlanReq &req) {
  return std::make_unique<AppendPrefetchPlanOp>(*this, req);
}

MetaStore::OpPtr<ListPrefetchPlanRsp> MetaStore::listPrefetchPlan(const ListPrefetchPlanReq &req) {
  return std::make_unique<ListPrefetchPlanOp>(*this, req);
}

MetaStore::OpPtr<UpdatePrefetchPlanEntriesRsp> MetaStore::updatePrefetchPlanEntries(
    const UpdatePrefetchPlanEntriesReq &req) {
  return std::make_unique<UpdatePrefetchPlanEntriesOp>(*this, req);
}

MetaStore::OpPtr<TrackPrefetchReadyRsp> MetaStore::trackPrefetchReady(const TrackPrefetchReadyReq &req) {
  return std::make_unique<TrackPrefetchReadyOp>(*this, req);
}

MetaStore::OpPtr<AdvancePrefetchJobStateRsp> MetaStore::advancePrefetchJobState(const AdvancePrefetchJobStateReq &req) {
  return std::make_unique<AdvancePrefetchJobStateOp>(*this, req);
}

MetaStore::OpPtr<CancelPrefetchJobRsp> MetaStore::cancelPrefetchJob(const CancelPrefetchJobReq &req) {
  return std::make_unique<CancelPrefetchJobOp>(*this, req);
}

MetaStore::OpPtr<UpsertCachePinsRsp> MetaStore::upsertCachePins(const UpsertCachePinsReq &req) {
  return std::make_unique<UpsertCachePinsOp>(*this, req);
}

MetaStore::OpPtr<RemoveCachePinsRsp> MetaStore::removeCachePins(const RemoveCachePinsReq &req) {
  return std::make_unique<RemoveCachePinsOp>(*this, req);
}

MetaStore::OpPtr<ListCachePinsByOwnerRsp> MetaStore::listCachePinsByOwner(const ListCachePinsByOwnerReq &req) {
  return std::make_unique<ListCachePinsByOwnerOp>(*this, req);
}

MetaStore::OpPtr<QueryCachePinsRsp> MetaStore::queryCachePins(const QueryCachePinsReq &req) {
  return std::make_unique<QueryCachePinsOp>(*this, req);
}

}  // namespace hf3fs::meta::server
