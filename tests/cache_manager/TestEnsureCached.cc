#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "cache_manager/admission/SecondMissAdmissionPolicy.h"
#include "cache_manager/capacity/SpacePoller.h"
#include "cache_manager/service/EnsureCached.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

class EnsureBackend : public CacheManagerBackend {
 public:
  CoTryTask<meta::Inode> stat(meta::InodeId) final { co_return inode; }
  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase> items) override {
    seen = std::move(items);
    meta::EnqueueCacheBlocksRsp response;
    for (size_t i = 0; i < seen.size(); ++i) {
      if (capacityReject) {
        response.results.emplace_back(makeError(CacheCode::kCapacityExceeded));
      } else {
        response.results.emplace_back(meta::CacheBlockMutationResult{seen[i].key, state});
      }
    }
    co_return response;
  }
  CoTryTask<meta::CacheBlockLease> acquire(const meta::CacheBlockRequestBase &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<std::vector<uint8_t>> getRange(const cache::ImmutableObjectIdentity &, cache::ByteRange) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<storage::CacheChunkGenerationInfo> replace(const meta::Inode &,
                                                       cache::CacheBlockIndex,
                                                       const meta::CacheBlockLease &,
                                                       std::vector<uint8_t>) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> commit(const meta::CacheBlockRequestBase &,
                         const meta::CacheBlockLease &,
                         const storage::CacheChunkGenerationInfo &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  CoTryTask<void> fail(const cache::CacheBlockKey &, const meta::CacheBlockLease &) final {
    co_return makeError(StatusCode::kNotImplemented);
  }
  std::shared_ptr<client::RoutingInfo> routingInfo() override { return rolloutRouting; }

  meta::Inode inode = [] {
    auto object = cache::ImmutableObjectIdentity{cache::OriginId{1},
                                                 "bucket",
                                                 "key",
                                                 {cache::VersionSelectorType::VERSION_ID, "v1"}};
    return meta::Inode{
        meta::InodeId{9},
        meta::InodeData{
            meta::OriginFile{8193, meta::Layout::newEmpty(flat::ChainTableId{2}, 4096, 1), std::move(object)}}};
  }();
  cache::CacheBlockState state{cache::CacheBlockState::QUEUED};
  bool capacityReject{false};
  std::vector<meta::CacheBlockRequestBase> seen;
  std::shared_ptr<client::RoutingInfo> rolloutRouting;
};

EnsureCachedReq request() {
  EnsureCachedReq req;
  req.service = {"cache-manager", "token"};
  req.inode = meta::InodeId{9};
  req.beginBlock = cache::CacheBlockIndex{0};
  req.blockCount = 3;
  req.priority = 7;
  req.cacheProtocolVersion = cache::kCacheProtocolVersion;
  return req;
}

storage::PhysicalDiskId phase2Disk() { return storage::PhysicalDiskId{Uuid::from(0, 1)}; }

std::shared_ptr<client::RoutingInfo> phase2Routing() {
  auto raw = std::make_shared<flat::RoutingInfo>();
  flat::ChainInfo chain;
  chain.chainId = flat::ChainId{1};
  chain.chainVersion = flat::ChainVersion{3};
  flat::ChainTargetInfo replica;
  replica.targetId = flat::TargetId{1};
  replica.publicState = flat::PublicTargetState::SERVING;
  chain.targets.push_back(replica);
  raw->chains.emplace(chain.chainId, chain);
  flat::TargetInfo target;
  target.targetId = flat::TargetId{1};
  target.nodeId = flat::NodeId{10};
  target.physicalDiskId = phase2Disk();
  target.storageRole = storage::StorageRole::CACHE_ONLY;
  raw->targets.emplace(target.targetId, target);
  return std::make_shared<client::RoutingInfo>(std::move(raw), SteadyTime{});
}

storage::QueryCacheSpaceRsp phase2Space(uint64_t used = 100) {
  storage::CacheSpaceInfo space;
  space.physicalDiskId = phase2Disk();
  space.role = storage::StorageRole::CACHE_ONLY;
  space.targets = {flat::TargetId{1}};
  space.capacityBytes = 1000;
  space.physicalUsedBytes = used;
  space.allocatableBytes = 1000;
  space.enforcedHighWatermark = 0.9;
  storage::QueryCacheSpaceRsp response;
  response.results.emplace_back(space);
  return response;
}

class Phase2Backend : public EnsureBackend {
 public:
  enum class Outcome { CREATED, READY, QUEUED, LOADING, ERROR };

  CoTryTask<storage::PermitIdentity> makePermit(const meta::Inode &,
                                                cache::CacheBlockIndex,
                                                uint64_t,
                                                Uuid managerEpoch,
                                                Uuid attemptId,
                                                uint64_t generation) final {
    storage::PlacementIdentity placement{{flat::ChainId{1}, flat::ChainVersion{3}},
                                         {flat::TargetId{1}},
                                         flat::TargetId{1},
                                         attemptId};
    storage::PermitIdentity permit{managerEpoch, placement, generation, {{flat::TargetId{1}, 100}}};
    made.push_back(permit);
    co_return permit;
  }

  CoTryTask<storage::CachePermitResult> preparePermit(const storage::PermitIdentity &permit,
                                                      uint64_t expiresAtNs) final {
    prepared.push_back(permit);
    if (prepareError) co_return makeError(CacheCode::kCapacityExceeded);
    co_return storage::CachePermitResult{permit, cache::CachePermitState::RESERVED, expiresAtNs};
  }

  CoTryTask<storage::CachePermitResult> renewPermit(const storage::PermitIdentity &permit, uint64_t expiresAtNs) final {
    renewed.push_back(permit);
    co_return storage::CachePermitResult{permit, cache::CachePermitState::RESERVED, expiresAtNs};
  }

  CoTryTask<storage::CachePermitResult> queryPermit(const storage::PermitIdentity &permit) final {
    queried.push_back(permit);
    co_return storage::CachePermitResult{permit, queryState, queryExpiresAtNs};
  }

  CoTryTask<void> releasePermit(const storage::PermitIdentity &permit) final {
    released.push_back(permit);
    co_return Void{};
  }
  CoTryTask<void> cancelQueuedAdmission(const cache::CacheBlockKey &key, const storage::PermitIdentity &permit) final {
    cancelled.emplace_back(key, permit);
    co_return Void{};
  }

  CoTryTask<meta::EnqueueCacheBlocksRsp> enqueue(std::vector<meta::CacheBlockRequestBase> items) final {
    enqueueCalls.push_back(items);
    if (enqueueTimeouts > 0) {
      --enqueueTimeouts;
      co_return makeError(RPCCode::kTimeout);
    }
    meta::EnqueueCacheBlocksRsp response;
    const auto &item = items.front();
    if (item.expectedPermit.has_value()) {
      response.results.emplace_back(meta::CacheBlockMutationResult{item.key,
                                                                   cache::CacheBlockState::QUEUED,
                                                                   cache::CacheEnqueueOutcome::QUEUED,
                                                                   item.permit->placement});
      response.permits.emplace_back(item.permit);
      co_return response;
    }
    switch (outcome) {
      case Outcome::CREATED:
        response.results.emplace_back(meta::CacheBlockMutationResult{item.key,
                                                                     cache::CacheBlockState::QUEUED,
                                                                     cache::CacheEnqueueOutcome::CREATED,
                                                                     item.permit->placement});
        response.permits.emplace_back(item.permit);
        break;
      case Outcome::READY:
        response.results.emplace_back(
            meta::CacheBlockMutationResult{item.key, cache::CacheBlockState::READY, cache::CacheEnqueueOutcome::READY});
        response.permits.emplace_back(std::nullopt);
        break;
      case Outcome::QUEUED:
        response.results.emplace_back(meta::CacheBlockMutationResult{item.key,
                                                                     cache::CacheBlockState::QUEUED,
                                                                     cache::CacheEnqueueOutcome::QUEUED,
                                                                     existingPermit.placement});
        response.permits.emplace_back(existingPermit);
        break;
      case Outcome::LOADING:
        response.results.emplace_back(meta::CacheBlockMutationResult{item.key,
                                                                     cache::CacheBlockState::LOADING,
                                                                     cache::CacheEnqueueOutcome::LOADING,
                                                                     existingPermit.placement});
        response.permits.emplace_back(existingPermit);
        break;
      case Outcome::ERROR:
        response.results.emplace_back(makeError(CacheCode::kUnavailable));
        response.permits.emplace_back(std::nullopt);
        break;
    }
    co_return response;
  }

  storage::PermitIdentity existingPermit = [] {
    storage::PlacementIdentity placement{{flat::ChainId{1}, flat::ChainVersion{3}},
                                         {flat::TargetId{1}},
                                         flat::TargetId{1},
                                         Uuid::from(7, 7)};
    return storage::PermitIdentity{Uuid::from(8, 8), placement, 4, {{flat::TargetId{1}, 100}}};
  }();
  Outcome outcome{Outcome::CREATED};
  cache::CachePermitState queryState{cache::CachePermitState::RESERVED};
  uint64_t queryExpiresAtNs{5000};
  bool prepareError{false};
  int enqueueTimeouts{0};
  std::vector<storage::PermitIdentity> made;
  std::vector<storage::PermitIdentity> prepared;
  std::vector<storage::PermitIdentity> queried;
  std::vector<storage::PermitIdentity> renewed;
  std::vector<storage::PermitIdentity> released;
  std::vector<std::pair<cache::CacheBlockKey, storage::PermitIdentity>> cancelled;
  std::vector<std::vector<meta::CacheBlockRequestBase>> enqueueCalls;
};

class Phase2EnsureCachedTest : public ::testing::Test {
 protected:
  Phase2EnsureCachedTest()
      : preflight(topology, 15_s, 0.9) {
    topology.updateRouting(phase2Routing());
    EXPECT_FALSE(topology
                     .updateSpace(flat::NodeId{10},
                                  phase2Space(),
                                  SteadyTime{std::chrono::nanoseconds(1)},
                                  SteadyTime{std::chrono::nanoseconds(2)})
                     .hasError());
  }

  EnsureCached create() {
    return EnsureCached(
        backend,
        hints,
        nullptr,
        policy,
        preflight,
        Uuid::from(1, 1),
        1_s,
        [this] { return SteadyTime{std::chrono::nanoseconds(++steadyNs)}; },
        [] { return uint64_t{1000}; });
  }

  EnsureCachedReq oneBlockRequest() {
    auto req = request();
    req.blockCount = 1;
    return req;
  }

  std::shared_ptr<Phase2Backend> backend = std::make_shared<Phase2Backend>();
  HintCoalescer hints;
  SecondMissAdmissionPolicy policy{100, 16};
  PhysicalTopology topology;
  PhysicalPreflight preflight;
  uint64_t steadyNs{2};
};

TEST(TestEnsureCached, EnqueuesExactBlocksAndDeduplicatesRestartedQueue) {
  auto backend = std::make_shared<EnsureBackend>();
  HintCoalescer hints;
  EnsureCached ensure(backend, hints);
  auto first = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(first);
  ASSERT_EQ(first->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_EQ(first->bypassReason, BypassReason::NONE);
  ASSERT_EQ(backend->seen.size(), size_t{3});
  ASSERT_EQ(backend->seen[2].blockLength, uint64_t{1});
  ASSERT_EQ(hints.size(), size_t{3});

  auto repeated = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(repeated);
  ASSERT_EQ(repeated->status, EnsureCachedStatus::ATTACHED);
  ASSERT_EQ(repeated->bypassReason, BypassReason::NONE);
  ASSERT_EQ(hints.size(), size_t{3});
}

TEST(TestEnsureCached, AttachesExistingStatesAndBypassesCapacity) {
  auto backend = std::make_shared<EnsureBackend>();
  HintCoalescer hints;
  EnsureCached ensure(backend, hints);
  backend->state = cache::CacheBlockState::READY;
  auto ready = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(ready);
  ASSERT_EQ(ready->status, EnsureCachedStatus::ATTACHED);
  ASSERT_EQ(ready->bypassReason, BypassReason::NONE);
  ASSERT_EQ(hints.size(), size_t{0});

  backend->capacityReject = true;
  auto bypassed = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(bypassed);
  ASSERT_EQ(bypassed->status, EnsureCachedStatus::BYPASSED);
  ASSERT_EQ(bypassed->bypassReason, BypassReason::CAPACITY);
}

TEST(TestEnsureCached, DrainingStopsLegacyAdmission) {
  auto backend = std::make_shared<EnsureBackend>();
  backend->rolloutRouting = phase2Routing();
  backend->rolloutRouting->raw()->cachePhase2State = flat::CachePhase2RolloutState::DRAINING;
  HintCoalescer hints;
  EnsureCached ensure(backend, hints);

  auto result = folly::coro::blockingWait(ensure.run(request()));
  ASSERT_OK(result);
  EXPECT_EQ(result->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(result->bypassReason, BypassReason::ADMISSION_DISABLED);
  EXPECT_TRUE(backend->seen.empty());
}

TEST_F(Phase2EnsureCachedTest, FirstMissDoesNotReachMetadataAndSecondMissCreatesQueuedWork) {
  auto ensure = create();
  auto first = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(first);
  EXPECT_EQ(first->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(first->bypassReason, BypassReason::POLICY);
  EXPECT_TRUE(backend->enqueueCalls.empty());
  EXPECT_TRUE(backend->prepared.empty());

  auto second = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(second);
  EXPECT_EQ(second->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_EQ(backend->enqueueCalls.size(), 1);
  ASSERT_TRUE(backend->enqueueCalls.front().front().permit.has_value());
  EXPECT_EQ(backend->enqueueCalls.front().front().permit->placement.admissionAttemptId,
            backend->made.front().placement.admissionAttemptId);
  EXPECT_EQ(hints.size(), 1);
  EXPECT_TRUE(backend->released.empty());
}

TEST_F(Phase2EnsureCachedTest, ExplicitPrefetchAdmitsFirstRequestButStillHonorsPhysicalHighWatermark) {
  auto ensure = create();
  auto prefetch = oneBlockRequest();
  prefetch.reason = EnsureReason::PREFETCH;
  auto admitted = folly::coro::blockingWait(ensure.run(prefetch));
  ASSERT_OK(admitted);
  EXPECT_EQ(admitted->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_EQ(backend->enqueueCalls.size(), size_t{1});

  hints.pop();
  ASSERT_OK(topology.updateSpace(flat::NodeId{10},
                                 phase2Space(850),
                                 SteadyTime{std::chrono::nanoseconds(3)},
                                 SteadyTime{std::chrono::nanoseconds(4)}));
  auto rejected = folly::coro::blockingWait(ensure.run(prefetch));
  ASSERT_OK(rejected);
  EXPECT_EQ(rejected->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(rejected->bypassReason, BypassReason::CAPACITY);
  EXPECT_EQ(backend->enqueueCalls.size(), size_t{1});
}

TEST_F(Phase2EnsureCachedTest, DurablePrefetchUsesPersistedAttemptAndRegistersJobClaim) {
  auto ensure = create();
  cache::PrefetchPlanEntry planned{cache::PrefetchJobId{Uuid::from(3, 4)},
                                   {9, cache::CacheBlockIndex{0}},
                                   4096,
                                   7,
                                   cache::PrefetchPlanEntryState::ADMITTED,
                                   Uuid::from(5, 6)};
  bool completed = false;
  auto admitted =
      folly::coro::blockingWait(ensure.runPrefetch(planned, [&](const Status &status) { completed = status.isOK(); }));
  ASSERT_OK(admitted);
  EXPECT_EQ(admitted->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_EQ(backend->made.size(), 1u);
  EXPECT_EQ(backend->made.front().placement.admissionAttemptId, planned.admissionAttemptId);
  auto hint = hints.pop();
  ASSERT_TRUE(hint.has_value());
  ASSERT_EQ(hint->jobClaims.size(), 1u);
  EXPECT_EQ(hint->jobClaims.front().jobId, planned.jobId);
  hint->notify(Status::OK);
  EXPECT_TRUE(completed);
}

TEST_F(Phase2EnsureCachedTest, ClusterRolloutStateStopsPhase2Admission) {
  backend->rolloutRouting = phase2Routing();
  auto ensure = create();

  backend->rolloutRouting->raw()->cachePhase2State = flat::CachePhase2RolloutState::DISABLED;
  auto disabled = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(disabled);
  EXPECT_EQ(disabled->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(disabled->bypassReason, BypassReason::ADMISSION_DISABLED);

  backend->rolloutRouting->raw()->cachePhase2State = flat::CachePhase2RolloutState::DRAINING;
  auto draining = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(draining);
  EXPECT_EQ(draining->status, EnsureCachedStatus::BYPASSED);
  EXPECT_TRUE(backend->made.empty());
  EXPECT_TRUE(backend->enqueueCalls.empty());
}

TEST_F(Phase2EnsureCachedTest, ReadyAndLoadingReleaseFreshPermit) {
  auto ensure = create();
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->outcome = Phase2Backend::Outcome::READY;
  auto ready = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(ready);
  EXPECT_EQ(ready->status, EnsureCachedStatus::ATTACHED);
  ASSERT_EQ(backend->released.size(), 1);
  EXPECT_EQ(backend->released.back(), backend->made.back());

  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->outcome = Phase2Backend::Outcome::LOADING;
  auto loading = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(loading);
  EXPECT_EQ(loading->status, EnsureCachedStatus::ATTACHED);
  ASSERT_EQ(backend->released.size(), 2);
  EXPECT_EQ(backend->released.back(), backend->made.back());
}

TEST_F(Phase2EnsureCachedTest, PrepareAndEnqueueFailuresReleasePreparedIdentity) {
  auto ensure = create();
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->prepareError = true;
  auto prepareFailed = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(prepareFailed);
  EXPECT_EQ(prepareFailed->bypassReason, BypassReason::CAPACITY);
  ASSERT_EQ(backend->released.size(), 1);
  EXPECT_EQ(backend->released.back(), backend->made.back());

  backend->prepareError = false;
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->outcome = Phase2Backend::Outcome::ERROR;
  auto enqueueFailed = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(enqueueFailed);
  EXPECT_EQ(enqueueFailed->bypassReason, BypassReason::UNAVAILABLE);
  ASSERT_EQ(backend->released.size(), 2);
  EXPECT_EQ(backend->released.back(), backend->made.back());
}

TEST_F(Phase2EnsureCachedTest, ExistingQueuedPermitIsRenewedOrCasReplaced) {
  auto ensure = create();
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->outcome = Phase2Backend::Outcome::QUEUED;
  auto attached = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(attached);
  EXPECT_EQ(attached->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_EQ(backend->queried.size(), 1);
  ASSERT_EQ(backend->renewed.size(), 1);
  EXPECT_EQ(backend->renewed.front(), backend->existingPermit);
  ASSERT_EQ(backend->released.size(), 1);

  hints.pop();
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->queryState = cache::CachePermitState::INVALID;
  auto replaced = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(replaced);
  EXPECT_EQ(replaced->status, EnsureCachedStatus::ACCEPTED);
  ASSERT_GE(backend->enqueueCalls.size(), 3);
  const auto &replacement = backend->enqueueCalls.back().front();
  ASSERT_TRUE(replacement.expectedPermit.has_value());
  ASSERT_TRUE(replacement.permit.has_value());
  EXPECT_EQ(replacement.permit->placement, backend->existingPermit.placement);
  EXPECT_EQ(replacement.permit->permitGeneration, backend->existingPermit.permitGeneration + 1);
  EXPECT_EQ(replacement.expectedState, cache::CacheBlockState::QUEUED);
}

TEST_F(Phase2EnsureCachedTest, AmbiguousEnqueueRetriesWithSamePermitIdentity) {
  auto ensure = create();
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->enqueueTimeouts = 1;
  auto admitted = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(admitted);
  ASSERT_EQ(backend->enqueueCalls.size(), 2);
  ASSERT_TRUE(backend->enqueueCalls[0][0].permit.has_value());
  ASSERT_TRUE(backend->enqueueCalls[1][0].permit.has_value());
  EXPECT_EQ(backend->enqueueCalls[0][0].permit, backend->enqueueCalls[1][0].permit);
  EXPECT_EQ(admitted->status, EnsureCachedStatus::ACCEPTED);
}

TEST_F(Phase2EnsureCachedTest, FinalAmbiguousEnqueueLeavesPermitForRecovery) {
  auto ensure = create();
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  backend->enqueueTimeouts = 2;
  auto ambiguous = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(ambiguous);
  EXPECT_EQ(ambiguous->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(ambiguous->bypassReason, BypassReason::UNAVAILABLE);
  ASSERT_EQ(backend->enqueueCalls.size(), 2);
  EXPECT_EQ(backend->enqueueCalls[0][0].permit, backend->enqueueCalls[1][0].permit);
  EXPECT_TRUE(backend->released.empty());
}

TEST_F(Phase2EnsureCachedTest, SchedulerAttachFailureCancelsExactQueuedAdmission) {
  EnsureCached ensure(
      backend,
      hints,
      nullptr,
      policy,
      preflight,
      Uuid::from(1, 1),
      1_s,
      [this] { return SteadyTime{std::chrono::nanoseconds(++steadyNs)}; },
      [] { return uint64_t{1000}; },
      [](LoadHint) -> Result<bool> { return makeError(StatusCode::kQueueConflict, "scheduler rejected hint"); });
  ASSERT_OK(folly::coro::blockingWait(ensure.run(oneBlockRequest())));
  auto rejected = folly::coro::blockingWait(ensure.run(oneBlockRequest()));
  ASSERT_OK(rejected);
  EXPECT_EQ(rejected->status, EnsureCachedStatus::BYPASSED);
  EXPECT_EQ(rejected->bypassReason, BypassReason::UNAVAILABLE);
  ASSERT_EQ(backend->cancelled.size(), 1);
  EXPECT_EQ(backend->cancelled.front().first, backend->enqueueCalls.front().front().key);
  EXPECT_EQ(backend->cancelled.front().second, backend->made.front());
  EXPECT_EQ(backend->released, std::vector<storage::PermitIdentity>{backend->made.front()});
  EXPECT_EQ(hints.size(), 0);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
