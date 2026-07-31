#include <folly/test/TestUtils.h>
#include <gtest/gtest.h>
#include <thread>

#include "kv/MemDBStore.h"
#include "storage/cache/event/CacheEventOutbox.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::storage::test {
namespace {

PhysicalDiskId disk(uint64_t id) { return PhysicalDiskId{Uuid::from(0, id)}; }

CacheEventIntent intent(uint64_t id, cache::CacheStorageEventType type = cache::CacheStorageEventType::DELETED) {
  CacheEventIntent result;
  result.type = type;
  result.storageOperationId = Uuid::from(1, id);
  result.logicalKey = {id, cache::CacheBlockIndex{0}};
  result.storageKey = {{ChainId{1}, ChainVer{1}}, ChunkId{0xCA, id}};
  result.generation = cache::CacheGeneration{id};
  result.placement =
      *PlacementIdentity::create({ChainId{1}, ChainVer{1}}, {TargetId{1}}, TargetId{1}, Uuid::from(2, id));
  result.diskId = disk(7);
  result.timestamp = UtcTime::fromMicroseconds(static_cast<int64_t>(id));
  if (type == cache::CacheStorageEventType::DELETED) {
    result.logicalRetireOperationId = Uuid::from(3, id);
    result.evictionEpoch = cache::EvictionEpoch{id};
  }
  return result;
}

std::unique_ptr<CacheEventJournal> memoryJournal(const kv::KVStore::Config &config,
                                                 size_t maxRecords = 100,
                                                 uint64_t maxBytes = 1_MB) {
  auto journal = std::make_unique<CacheEventJournal>(std::make_unique<kv::MemDBStore>(config), maxRecords, maxBytes);
  EXPECT_TRUE(journal->init());
  return journal;
}

TEST(TestCacheEventJournal, PreparedDoesNotAllocateSequenceOrBlockLaterCompletion) {
  kv::KVStore::Config config;
  auto journal = memoryJournal(config);
  auto first = intent(1);
  auto second = intent(2);
  auto third = intent(3);
  ASSERT_OK(journal->prepare(first));
  ASSERT_OK(journal->prepare(second));
  ASSERT_OK(journal->prepare(third));
  EXPECT_EQ(journal->stats().nextSequence, 1);

  auto completedSecond = journal->markDeliverable(second.storageOperationId);
  auto completedThird = journal->markDeliverable(third.storageOperationId);
  ASSERT_OK(completedSecond);
  ASSERT_OK(completedThird);
  EXPECT_EQ(completedSecond->sequence, 1);
  EXPECT_EQ(completedThird->sequence, 2);
  auto prepared = journal->prepared();
  ASSERT_OK(prepared);
  ASSERT_EQ(prepared->size(), size_t{1});
  EXPECT_EQ(prepared->front().intent, first);

  CacheEventOutbox outbox(*journal);
  auto batch = outbox.next(10);
  ASSERT_OK(batch);
  ASSERT_EQ(batch->size(), size_t{2});
  EXPECT_EQ((*batch)[0].sequence, 1);
  EXPECT_EQ((*batch)[0].intent, second);
  EXPECT_EQ((*batch)[1].sequence, 2);
  EXPECT_EQ((*batch)[1].intent, third);
}

TEST(TestCacheEventJournal, OperationsAndAcknowledgementsAreIdempotent) {
  kv::KVStore::Config config;
  auto journal = memoryJournal(config);
  auto deletion = intent(1);
  auto prepared = journal->prepare(deletion);
  ASSERT_OK(prepared);
  ASSERT_EQ(journal->prepare(deletion), prepared);
  auto reused = deletion;
  reused.generation = cache::CacheGeneration{2};
  ASSERT_ERROR(journal->prepare(reused), CacheCode::kStateConflict);

  auto delivered = journal->markDeliverable(deletion.storageOperationId);
  ASSERT_OK(delivered);
  ASSERT_EQ(journal->markDeliverable(deletion.storageOperationId), delivered);
  CacheEventOutbox outbox(*journal);
  ASSERT_ERROR(outbox.acknowledge(disk(99), delivered->sequence), CacheCode::kStateConflict);
  ASSERT_OK(outbox.acknowledge(delivered->sourceId, delivered->sequence));
  ASSERT_OK(outbox.acknowledge(delivered->sourceId, delivered->sequence));
  ASSERT_TRUE(outbox.next(10)->empty());
  EXPECT_EQ(journal->stats().acknowledgedSequence, 1);
  EXPECT_EQ(journal->stats().accountedBytes, 0);
}

TEST(TestCacheEventJournal, RecoversPreparedDeliverableAndAckCrashBoundaries) {
  folly::test::TemporaryDirectory directory;
  kv::KVStore::Config config;
  config.set_type(kv::KVStore::Type::LevelDB);
  auto open = [&](bool create) {
    kv::KVStore::Options options;
    options.type = kv::KVStore::Type::LevelDB;
    options.path = directory.path() / "cache-events";
    options.createIfMissing = create;
    auto journal = std::make_unique<CacheEventJournal>(kv::KVStore::create(config, options), 100, 1_MB);
    EXPECT_TRUE(journal->init());
    return journal;
  };

  auto stuck = intent(1);
  auto completed = intent(2);
  PhysicalDiskId sourceId;
  {
    auto journal = open(true);
    sourceId = journal->stats().sourceId;
    ASSERT_OK(journal->prepare(stuck));
    ASSERT_OK(journal->prepare(completed));
  }
  {
    auto journal = open(false);
    EXPECT_EQ(journal->stats().sourceId, sourceId);
    ASSERT_EQ(journal->prepared()->size(), size_t{2});
    ASSERT_EQ(journal->markDeliverable(completed.storageOperationId)->sequence, 1);
  }
  {
    auto journal = open(false);
    ASSERT_EQ(journal->prepared()->size(), size_t{1});
    ASSERT_EQ(journal->deliveryBatch(10)->size(), size_t{1});
    ASSERT_EQ(journal->markDeliverable(stuck.storageOperationId)->sequence, 2);
  }
  {
    auto journal = open(false);
    CacheEventOutbox outbox(*journal);
    auto batch = outbox.next(10);
    ASSERT_OK(batch);
    ASSERT_EQ(batch->size(), size_t{2});
    ASSERT_OK(outbox.acknowledge(sourceId, 2));
  }
  {
    auto journal = open(false);
    EXPECT_TRUE(journal->prepared()->empty());
    EXPECT_TRUE(journal->deliveryBatch(10)->empty());
    EXPECT_EQ(journal->stats().acknowledgedSequence, 2);
    EXPECT_EQ(journal->stats().nextSequence, 3);
  }
}

TEST(TestCacheEventJournal, ReservesBoundedDeliverySpaceBeforeDeletion) {
  kv::KVStore::Config config;
  auto oneRecord = memoryJournal(config, 1, 1_MB);
  ASSERT_OK(oneRecord->prepare(intent(1)));
  ASSERT_ERROR(oneRecord->requireWritable(), CacheCode::kJournalFull);
  ASSERT_ERROR(oneRecord->prepare(intent(2)), CacheCode::kJournalFull);

  auto noSpace = memoryJournal(config, 100, 1);
  ASSERT_ERROR(noSpace->prepare(intent(1)), CacheCode::kJournalFull);
  ASSERT_ERROR(noSpace->requireWritable(), CacheCode::kJournalFull);

  auto reclaim = memoryJournal(config, 1, 1_MB);
  auto first = intent(3);
  auto envelope = reclaim->prepare(first);
  ASSERT_OK(envelope);
  auto delivered = reclaim->markDeliverable(first.storageOperationId);
  ASSERT_OK(delivered);
  ASSERT_OK(reclaim->acknowledge(delivered->sequence));
  ASSERT_OK(reclaim->requireWritable());
  ASSERT_OK(reclaim->prepare(intent(4)));
}

TEST(TestCacheEventJournal, ConcurrentCompletionAllocatesContiguousSequences) {
  kv::KVStore::Config config;
  auto journal = memoryJournal(config, 32, 1_MB);
  constexpr uint64_t kEvents = 16;
  for (uint64_t id = 1; id <= kEvents; ++id) ASSERT_OK(journal->prepare(intent(id)));

  std::vector<std::thread> threads;
  std::atomic<uint64_t> completed{0};
  for (uint64_t id = 1; id <= kEvents; ++id) {
    threads.emplace_back([&, id] {
      if (journal->markDeliverable(intent(id).storageOperationId)) ++completed;
    });
  }
  for (auto &thread : threads) thread.join();
  EXPECT_EQ(completed, kEvents);
  auto batch = journal->deliveryBatch(kEvents);
  ASSERT_OK(batch);
  ASSERT_EQ(batch->size(), kEvents);
  for (uint64_t index = 0; index < kEvents; ++index) EXPECT_EQ((*batch)[index].sequence, index + 1);
}

TEST(TestCacheEventJournal, ValidatesEventKindsAndBatchBounds) {
  auto local = intent(1, cache::CacheStorageEventType::EMERGENCY_EVICTED);
  ASSERT_OK(local.valid());
  local.logicalRetireOperationId = Uuid::random();
  ASSERT_ERROR(local.valid(), StatusCode::kInvalidArg);

  auto logical = intent(2);
  logical.evictionEpoch.reset();
  ASSERT_ERROR(logical.valid(), StatusCode::kInvalidArg);

  kv::KVStore::Config config;
  auto journal = memoryJournal(config);
  ASSERT_ERROR(journal->deliveryBatch(0), CacheCode::kRequestTooLarge);
  ASSERT_ERROR(journal->deliveryBatch(cache::kMaxPhase2BatchItems + 1), CacheCode::kRequestTooLarge);
  ASSERT_ERROR(journal->markDeliverable(Uuid::random()), CacheCode::kNotFound);
}

}  // namespace
}  // namespace hf3fs::storage::test
