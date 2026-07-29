#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>
#include <string_view>

#include "meta/store/cache/CacheBlockStore.h"
#include "meta/store/cache/CacheCapacityStore.h"
#include "meta/store/cache/CleanupJobStore.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

constexpr std::string_view kServiceName = "cache-manager";
constexpr std::string_view kServiceToken = "cleanup-test-token";

class TestCacheCleanup : public MetaTestBase<kv::mem::MemKV> {
 protected:
  MockCluster createCluster() {
    static MockCluster::Config config = [] {
      MockCluster::Config value;
      value.set_num_meta(1);
      value.mock_meta().event_trace_log().set_enabled(false);
      value.mock_meta().set_cache_service_name(std::string{kServiceName});
      value.mock_meta().set_cache_service_token(std::string{kServiceToken});
      return value;
    }();
    return createMockCluster(config);
  }
};

CacheServiceIdentity service() { return {std::string{kServiceName}, std::string{kServiceToken}}; }

void enableCacheFeature(MockCluster &cluster) {
  auto routing = cluster.mgmtdClient()->cloneRoutingInfo();
  auto &raw = *routing->raw();
  for (auto &[_, node] : raw.nodes) {
    if (node.type == flat::NodeType::META) {
      node.cacheSchemaVersion = cache::kCacheSchemaVersion;
      node.cacheProtocolVersion = cache::kCacheProtocolVersion;
    }
  }
  auto &table = raw.chainTables.at(flat::ChainTableId{2}).rbegin()->second;
  table.role = flat::ChainTableRole::CACHE_DATA;
  table.logicalCapacity = 1ULL << 30;
  table.checksumType = flat::ChainTableChecksumType::CRC32C;
  cluster.mgmtdClient()->setRoutingInfo(std::move(routing));
}

CoTryTask<Inode> prepareOrigin(MockCluster &cluster, std::string path, uint64_t blocks = 4) {
  enableCacheFeature(cluster);
  auto txn = cluster.kvEngine()->createReadWriteTransaction();
  CO_RETURN_ON_ERROR(co_await CacheCapacityStore::setLogicalCapacity(*txn, 1ULL << 20));
  CO_RETURN_ON_ERROR(co_await txn->commit());

  ImportOriginFileReq request;
  request.user = SUPER_USER;
  request.entry.path = PathAt(std::move(path));
  request.entry.metadata.object = {cache::OriginId{1},
                                   "bucket",
                                   "key",
                                   cache::VersionSelector{cache::VersionSelectorType::VERSION_ID, "version-1"}};
  request.entry.metadata.objectSize = blocks * 4096;
  request.entry.metadata.tableId = flat::ChainTableId{2};
  request.entry.metadata.blockSize = 4096;
  request.entry.metadata.stripeSize = 1;
  request.entry.metadata.permission = Permission{0600};
  request.cacheProtocolVersion = cache::kCacheProtocolVersion;
  auto imported = co_await cluster.meta().getOperator().importOriginFile(request);
  CO_RETURN_ON_ERROR(imported);
  co_return Inode{imported->inode};
}

cache::CacheBlockKey key(const Inode &inode, uint32_t block) { return {inode.id.u64(), cache::CacheBlockIndex{block}}; }

TEST_F(TestCacheCleanup, BeginFinishFencesAndPreservesSingleCharge) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepareOrigin(cluster, "/cleanup");
    CO_ASSERT_OK(inode);

    cache::ReadyIdentity ready{1, cache::CacheGeneration{2}, 1, 1234, 4096};
    {
      auto txn = cluster.kvEngine()->createReadWriteTransaction();
      for (uint32_t block = 0; block < 3; ++block) {
        CO_ASSERT_OK(co_await CacheBlockStore::enqueue(*txn, key(*inode, block), flat::ChainId{1}, 4096));
      }
      auto loading = co_await CacheBlockStore::load(*txn, key(*inode, 1));
      CO_ASSERT_OK(loading);
      (**loading).state = cache::CacheBlockState::LOADING;
      (**loading).loaderId = Uuid::random();
      (**loading).loadEpoch = 7;
      (**loading).cacheGeneration = cache::CacheGeneration{2};
      (**loading).leaseExpiresAt = UtcClock::now() + (1_min).asUs();
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, **loading));

      auto committed = co_await CacheBlockStore::load(*txn, key(*inode, 2));
      CO_ASSERT_OK(committed);
      (**committed).state = cache::CacheBlockState::READY;
      (**committed).loadEpoch = 1;
      (**committed).cacheGeneration = cache::CacheGeneration{2};
      (**committed).ready = ready;
      CO_ASSERT_OK(co_await CacheBlockStore::commitCharge(*txn, **committed));

      CacheBlockRecord failed;
      failed.key = key(*inode, 3);
      failed.state = cache::CacheBlockState::FAILED;
      failed.chainId = flat::ChainId{1};
      failed.blockLength = 4096;
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, failed));
      CO_ASSERT_OK(co_await txn->commit());
    }

    BeginCleanCacheBlocksReq begin;
    begin.service = service();
    begin.items.push_back({key(*inode, 0), std::nullopt, std::nullopt, cache::CleanupTerminalState::NONE});
    begin.items.push_back(
        {key(*inode, 1), std::nullopt, cache::CacheGeneration{4}, cache::CleanupTerminalState::FAILED});
    begin.items.push_back({key(*inode, 2), ready, std::nullopt, cache::CleanupTerminalState::REENQUEUE});
    begin.items.push_back({key(*inode, 3), std::nullopt, std::nullopt, cache::CleanupTerminalState::NONE});
    auto begun = co_await cluster.meta().getOperator().beginCleanCacheBlocks(begin);
    CO_ASSERT_OK(begun);
    for (const auto &result : begun->results) CO_ASSERT_OK(result);
    CO_ASSERT_EQ(begun->results[0]->deleteGeneration, cache::CacheGeneration{});
    CO_ASSERT_EQ(begun->results[1]->deleteGeneration, cache::CacheGeneration{4});
    CO_ASSERT_EQ(begun->results[2]->deleteGeneration, cache::CacheGeneration{2});

    auto repeated = co_await cluster.meta().getOperator().beginCleanCacheBlocks(begin);
    CO_ASSERT_OK(repeated);
    CO_ASSERT_EQ(repeated->results[1]->cleanupEpoch, begun->results[1]->cleanupEpoch);

    FinishCleanCacheBlocksReq finish;
    finish.service = service();
    finish.items.push_back({key(*inode, 0), begun->results[0]->cleanupEpoch, std::nullopt});
    finish.items.push_back({key(*inode, 1), begun->results[1]->cleanupEpoch, cache::CacheGeneration{3}});
    finish.items.push_back({key(*inode, 2), begun->results[2]->cleanupEpoch, cache::CacheGeneration{2}});
    finish.items.push_back({key(*inode, 3), begun->results[3]->cleanupEpoch, std::nullopt});
    auto staleEpoch = finish;
    staleEpoch.items = {{key(*inode, 1),
                         cache::CleanupEpoch{begun->results[1]->cleanupEpoch.toUnderType() + 1},
                         cache::CacheGeneration{4}}};
    auto staleFinished = co_await cluster.meta().getOperator().finishCleanCacheBlocks(staleEpoch);
    CO_ASSERT_OK(staleFinished);
    CO_ASSERT_ERROR(staleFinished->results[0], CacheCode::kStateConflict);

    auto finished = co_await cluster.meta().getOperator().finishCleanCacheBlocks(finish);
    CO_ASSERT_OK(finished);
    CO_ASSERT_OK(finished->results[0]);
    CO_ASSERT_ERROR(finished->results[1], CacheCode::kStaleGeneration);
    CO_ASSERT_OK(finished->results[2]);
    CO_ASSERT_OK(finished->results[3]);

    finish.items = {{key(*inode, 1), begun->results[1]->cleanupEpoch, cache::CacheGeneration{4}}};
    finished = co_await cluster.meta().getOperator().finishCleanCacheBlocks(finish);
    CO_ASSERT_OK(finished);
    CO_ASSERT_OK(finished->results[0]);
    CO_ASSERT_EQ(finished->results[0]->state, cache::CacheBlockState::FAILED);
    auto idempotent = co_await cluster.meta().getOperator().finishCleanCacheBlocks(finish);
    CO_ASSERT_OK(idempotent);
    CO_ASSERT_OK(idempotent->results[0]);

    auto read = cluster.kvEngine()->createReadonlyTransaction();
    auto capacity = co_await CacheCapacityStore::snapshotLoad(*read);
    CO_ASSERT_OK(capacity);
    CO_ASSERT_EQ(capacity->usedBytes, uint64_t{4096});
    CO_ASSERT_EQ(capacity->reservedBytes, uint64_t{4096});
    CO_ASSERT_EQ(capacity->committedBytes, uint64_t{0});
  }());
}

TEST_F(TestCacheCleanup, CleanupCursorResumesAndRetriesPartialPass) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createCluster();
    auto inode = co_await prepareOrigin(cluster, "/cursor", 1001);
    CO_ASSERT_OK(inode);
    auto jobId = Uuid::random();
    {
      auto txn = cluster.kvEngine()->createReadWriteTransaction();
      OriginCleanupJobRecord job;
      job.jobId = jobId;
      job.inode = inode->id;
      job.endBlock = 1001;
      CO_ASSERT_OK(co_await CleanupJobStore::store(*txn, job));
      CacheBlockRecord cleaning;
      cleaning.key = key(*inode, 1000);
      cleaning.state = cache::CacheBlockState::CLEANING;
      cleaning.chainId = flat::ChainId{1};
      cleaning.blockLength = 4096;
      cleaning.cleanupEpoch = cache::CleanupEpoch{1};
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, cleaning));
      CO_ASSERT_OK(co_await txn->commit());
    }

    auto advance = [&](const Uuid &id) -> CoTryTask<OriginCleanupJobRecord> {
      auto txn = cluster.kvEngine()->createReadWriteTransaction();
      auto result = co_await CleanupJobStore::advance(*txn, id);
      CO_RETURN_ON_ERROR(result);
      CO_RETURN_ON_ERROR(co_await txn->commit());
      co_return *result;
    };
    auto first = co_await advance(jobId);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->cursor, uint64_t{1000});
    auto partial = co_await advance(jobId);
    CO_ASSERT_OK(partial);
    CO_ASSERT_EQ(partial->cursor, uint64_t{0});
    CO_ASSERT_EQ(partial->remainingNonTerminalBlocks, uint64_t{1});

    {
      auto txn = cluster.kvEngine()->createReadWriteTransaction();
      auto record = co_await CacheBlockStore::load(*txn, key(*inode, 1000));
      CO_ASSERT_OK(record);
      (**record).state = cache::CacheBlockState::FAILED;
      (**record).cleanupEpoch = cache::CleanupEpoch{};
      CO_ASSERT_OK(co_await CacheBlockStore::store(*txn, **record));
      CO_ASSERT_OK(co_await txn->commit());
    }
    auto resumed = co_await advance(jobId);
    CO_ASSERT_OK(resumed);
    CO_ASSERT_EQ(resumed->cursor, uint64_t{1000});
    auto complete = co_await advance(jobId);
    CO_ASSERT_OK(complete);
    CO_ASSERT_TRUE(complete->complete());
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
