#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <limits>

#include "meta/store/cache/CacheBlockStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr std::string_view kServiceName = "cache-manager";
constexpr std::string_view kServiceToken = "orchestration-test-token";

class TestCacheOrchestration : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster(bool phase3Enabled) {
    static MockCluster::Config enabled = config(true);
    static MockCluster::Config disabled = config(false);
    return createMockCluster(phase3Enabled ? enabled : disabled);
  }

  static void enableCacheFeature(MockCluster &cluster) {
    auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
    for (auto &[_, node] : routing->raw()->nodes) {
      if (node.type == flat::NodeType::META) {
        node.cacheSchemaVersion = cache::kCacheSchemaVersion;
        node.cacheProtocolVersion = cache::kCacheProtocolVersion;
      }
    }
    auto &table = routing->raw()->chainTables.at(flat::ChainTableId{2}).rbegin()->second;
    table.role = flat::ChainTableRole::CACHE_DATA;
    table.logicalCapacity = 1ULL << 30;
    table.checksumType = flat::ChainTableChecksumType::CRC32C;
    cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
  }

 private:
  static MockCluster::Config config(bool phase3Enabled) {
    MockCluster::Config value;
    value.set_num_meta(1);
    value.mock_meta().event_trace_log().set_enabled(false);
    value.mock_meta().set_cache_service_name(std::string{kServiceName});
    value.mock_meta().set_cache_service_token(std::string{kServiceToken});
    value.mock_meta().set_enable_cache_phase2(true);
    value.mock_meta().set_enable_cache_phase3(phase3Enabled);
    return value;
  }
};

CacheServiceIdentity service() { return {std::string{kServiceName}, std::string{kServiceToken}}; }

cache::PrefetchJobRecord job(uint64_t id) {
  cache::PrefetchJobRecord record;
  record.spec.jobId = cache::PrefetchJobId{Uuid::from(0, id)};
  record.spec.ownerUid = flat::Uid{1000};
  record.spec.sources.push_back(cache::DatasetSource{cache::NamespacePathSource{"/dataset", true}});
  record.state = cache::PrefetchJobState::PENDING;
  record.stateVersion = 1;
  record.createdAtMs = 100;
  record.updatedAtMs = 100;
  return record;
}

cache::PinOwner owner(uint64_t id) { return {cache::PinOwnerKind::ACTIVE_JOB, cache::PinOwnerId{Uuid::from(0, id)}}; }

TEST_F(TestCacheOrchestration, EnforcesIdentityProtocolAndDisabledByDefault) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster(false);
    enableCacheFeature(cluster);
    CreatePrefetchJobReq request;
    request.service = service();
    request.job = job(1);
    request.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    CO_ASSERT_ERROR(co_await cluster.meta().getOperator().createPrefetchJob(request), CacheCode::kFeatureDisabled);

    request.service.token = "wrong-token";
    CO_ASSERT_ERROR(co_await cluster.meta().getOperator().createPrefetchJob(request), MetaCode::kNoPermission);
    request.service = service();
    request.cacheProtocolVersion = cache::kCacheProtocolVersion;
    CO_ASSERT_ERROR(co_await cluster.meta().getOperator().createPrefetchJob(request), CacheCode::kUpgradeRequired);

    auto readable = request.service.serdeToReadable();
    CO_ASSERT_EQ(readable.find(std::string{kServiceToken}), std::string::npos);
  }());
}

TEST_F(TestCacheOrchestration, PersistsAndPagesJobsAndPlansWithCas) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster(true);
    enableCacheFeature(cluster);
    auto &meta = cluster.meta().getOperator();
    CreatePrefetchJobReq create;
    create.service = service();
    create.job = job(2);
    create.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto created = co_await meta.createPrefetchJob(create);
    CO_ASSERT_OK(created);
    CO_ASSERT_TRUE(created->created);
    auto retried = co_await meta.createPrefetchJob(create);
    CO_ASSERT_OK(retried);
    CO_ASSERT_FALSE(retried->created);

    AppendPrefetchPlanReq append;
    append.service = service();
    append.jobId = create.job.spec.jobId;
    append.entries.push_back(
        {append.jobId, {10, cache::CacheBlockIndex{0}}, 4096, 1, cache::PrefetchPlanEntryState::PLANNED, Uuid::zero()});
    append.plannerCursor = "next";
    append.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto appended = co_await meta.appendPrefetchPlan(append);
    CO_ASSERT_OK(appended);
    CO_ASSERT_EQ(appended->insertedBlocks, uint64_t{1});
    CO_ASSERT_EQ(appended->insertedBytes, uint64_t{4096});

    ListPrefetchPlanReq listPlan;
    listPlan.service = service();
    listPlan.jobId = append.jobId;
    listPlan.limit = 1;
    listPlan.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto plan = co_await meta.listPrefetchPlan(listPlan);
    CO_ASSERT_OK(plan);
    CO_ASSERT_EQ(plan->entries, append.entries);

    auto desired = create.job;
    desired.state = cache::PrefetchJobState::PLANNING;
    desired.stateVersion = 3;
    desired.updatedAtMs = 101;
    desired.plannedBlocks = 1;
    desired.plannedBytes = 4096;
    desired.plannerCursor = "next";
    UpdatePrefetchJobReq stale;
    stale.service = service();
    stale.expectedStateVersion = 2;
    stale.job = desired;
    stale.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto updated = co_await meta.updatePrefetchJob(stale);
    CO_ASSERT_OK(updated);
    CO_ASSERT_EQ(updated->job, desired);
    CO_ASSERT_ERROR(co_await meta.updatePrefetchJob(stale), CacheCode::kStateConflict);

    GetPrefetchJobReq get;
    get.service = service();
    get.jobId = desired.spec.jobId;
    get.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto fetched = co_await meta.getPrefetchJob(get);
    CO_ASSERT_OK(fetched);
    CO_ASSERT_EQ(fetched->job, desired);

    ListPrefetchJobsReq listJobs;
    listJobs.service = service();
    listJobs.ownerUid = flat::Uid{1000};
    listJobs.limit = 1;
    listJobs.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto jobs = co_await meta.listPrefetchJobs(listJobs);
    CO_ASSERT_OK(jobs);
    CO_ASSERT_EQ(jobs->jobs.size(), size_t{1});
    CO_ASSERT_EQ(jobs->jobs.front(), desired);
  }());
}

TEST_F(TestCacheOrchestration, PinBatchUsesGenerationFenceAndServerClock) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster(true);
    enableCacheFeature(cluster);
    auto &meta = cluster.meta().getOperator();
    const auto nowMs = static_cast<uint64_t>(UtcClock::now().toMicroseconds()) / 1000;
    cache::PinRecord valid{{20, cache::CacheBlockIndex{0}}, owner(3), nowMs, nowMs + 60'000, {}};
    auto fenced = valid;
    fenced.key.block = cache::CacheBlockIndex{1};
    fenced.cacheGeneration = cache::CacheGeneration{1};

    UpsertCachePinsReq upsert;
    upsert.service = service();
    upsert.pins = {valid, fenced};
    upsert.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto inserted = co_await meta.upsertCachePins(upsert);
    CO_ASSERT_OK(inserted);
    CO_ASSERT_EQ(inserted->results.size(), size_t{2});
    CO_ASSERT_OK(inserted->results[0]);
    CO_ASSERT_ERROR(inserted->results[1], CacheCode::kStateConflict);

    QueryCachePinsReq query;
    query.service = service();
    query.keys = {valid.key, fenced.key};
    query.nowMs = std::numeric_limits<uint64_t>::max();
    query.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto queried = co_await meta.queryCachePins(query);
    CO_ASSERT_OK(queried);
    CO_ASSERT_TRUE(queried->results[0].hasValue() && queried->results[0]->pinned);
    CO_ASSERT_EQ(queried->results[0]->activeOwners, uint32_t{1});
    CO_ASSERT_TRUE(queried->results[1].hasValue() && !queried->results[1]->pinned);

    ListCachePinsByOwnerReq list;
    list.service = service();
    list.owner = valid.owner;
    list.limit = 1;
    list.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto listed = co_await meta.listCachePinsByOwner(list);
    CO_ASSERT_OK(listed);
    CO_ASSERT_EQ(listed->pins, std::vector{valid});

    RemoveCachePinsReq remove;
    remove.service = service();
    remove.owner = valid.owner;
    remove.cacheProtocolVersion = cache::kCachePhase3ProtocolVersion;
    auto removed = co_await meta.removeCachePins(remove);
    CO_ASSERT_OK(removed);
    CO_ASSERT_EQ(removed->removed, uint64_t{1});
    removed = co_await meta.removeCachePins(remove);
    CO_ASSERT_OK(removed);
    CO_ASSERT_EQ(removed->removed, uint64_t{0});
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
