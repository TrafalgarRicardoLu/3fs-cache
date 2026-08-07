#include <algorithm>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "common/kv/mem/MemKVEngine.h"
#include "common/serde/Serde.h"
#include "meta/store/cache/UploadJobKey.h"
#include "meta/store/cache/UploadJobStore.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::meta::server {
namespace {

class TestUploadJobStore : public ::testing::Test {
 protected:
  kv::MemKVEngine engine_;
};

cache::UploadJobRecord openJob(uint64_t id, uint32_t owner = 1) {
  cache::UploadJobRecord job;
  job.jobId = cache::UploadJobId{Uuid::from(0, id)};
  job.ownerUid = flat::Uid{owner};
  job.path = fmt::format("/staging/{}", id);
  job.stagingInode = 1000 + id;
  job.destination = {cache::OriginId{1}, "bucket", fmt::format("objects/{}", id)};
  job.state = cache::UploadJobState::OPEN;
  job.stateVersion = 1;
  job.createdAtMs = 100;
  job.updatedAtMs = 100;
  job.writerLeaseId = Uuid::from(9, id);
  job.writerLeaseExpiresAtMs = 200;
  return job;
}

cache::UploadJobRecord seal(cache::UploadJobRecord job, uint64_t length = 10) {
  job.stagingLength = length;
  job.state = cache::UploadJobState::SEALED;
  ++job.stateVersion;
  ++job.updatedAtMs;
  job.writerLeaseId = Uuid::zero();
  job.writerLeaseExpiresAtMs = 0;
  return job;
}

TEST_F(TestUploadJobStore, CreateIsIdempotentAndPersistsSerdeRecord) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto desired = openJob(1);
    auto txn = engine_.createReadWriteTransaction();
    auto created = co_await UploadJobStore::create(*txn, desired);
    CO_ASSERT_OK(created);
    CO_ASSERT_TRUE(created->created);
    CO_ASSERT_OK(co_await txn->commit());

    txn = engine_.createReadWriteTransaction();
    auto retry = co_await UploadJobStore::create(*txn, desired);
    CO_ASSERT_OK(retry);
    CO_ASSERT_FALSE(retry->created);
    CO_ASSERT_EQ(retry->job, desired);

    auto conflicting = desired;
    conflicting.destination.key = "another-object";
    CO_ASSERT_ERROR(co_await UploadJobStore::create(*txn, conflicting), CacheCode::kStateConflict);

    auto read = engine_.createReadonlyTransaction();
    auto loaded = co_await UploadJobStore::snapshotLoad(*read, desired.jobId);
    CO_ASSERT_OK(loaded);
    CO_ASSERT_TRUE(loaded->has_value());
    CO_ASSERT_EQ(**loaded, desired);
  }());
}

TEST_F(TestUploadJobStore, StateVersionProvidesCasAndValidatesTransitions) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto initial = openJob(2);
    auto setup = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await UploadJobStore::create(*setup, initial));
    CO_ASSERT_OK(co_await setup->commit());

    auto invalid = seal(initial);
    invalid.state = cache::UploadJobState::UPLOADING;
    invalid.multipartId = "multipart";
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*txn, 1, invalid), CacheCode::kStateConflict);

    auto desired = seal(initial);
    auto first = engine_.createReadWriteTransaction();
    auto second = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await UploadJobStore::update(*first, 1, desired));
    CO_ASSERT_OK(co_await UploadJobStore::update(*second, 1, desired));
    CO_ASSERT_OK(co_await first->commit());
    CO_ASSERT_ERROR(co_await second->commit(), TransactionCode::kConflict);

    auto stale = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*stale, 1, desired), CacheCode::kStateConflict);
  }());
}

TEST_F(TestUploadJobStore, ListsWithStableCursorAndOwnerFilter) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    std::vector<cache::UploadJobRecord> records{openJob(11, 1), openJob(12, 2), openJob(13, 1)};
    std::sort(records.begin(), records.end(), [](const auto &left, const auto &right) {
      return UploadJobKey::job(left.jobId) < UploadJobKey::job(right.jobId);
    });
    auto txn = engine_.createReadWriteTransaction();
    for (const auto &record : records) CO_ASSERT_OK(co_await UploadJobStore::create(*txn, record));
    CO_ASSERT_OK(co_await txn->commit());

    auto read = engine_.createReadonlyTransaction();
    auto first = co_await UploadJobStore::snapshotList(*read, std::nullopt, std::nullopt, 2);
    CO_ASSERT_OK(first);
    CO_ASSERT_EQ(first->jobs.size(), size_t{2});
    CO_ASSERT_TRUE(first->more);
    auto second = co_await UploadJobStore::snapshotList(*read, std::nullopt, first->jobs.back().jobId, 2);
    CO_ASSERT_OK(second);
    CO_ASSERT_EQ(second->jobs.size(), size_t{1});
    CO_ASSERT_FALSE(second->more);

    auto owned = co_await UploadJobStore::snapshotList(*read, flat::Uid{1}, std::nullopt, 2);
    CO_ASSERT_OK(owned);
    CO_ASSERT_EQ(owned->jobs.size(), size_t{2});
    CO_ASSERT_FALSE(owned->more);
    for (const auto &record : owned->jobs) CO_ASSERT_EQ(record.ownerUid, flat::Uid{1});
  }());
}

TEST_F(TestUploadJobStore, RejectsPartBoundsOversizedValuesAndProgressRollback) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto initial = openJob(3);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await UploadJobStore::create(*txn, initial));
    CO_ASSERT_OK(co_await txn->commit());

    auto invalidParts = seal(initial, cache::kMaxUploadParts + 1);
    invalidParts.state = cache::UploadJobState::UPLOADING;
    invalidParts.multipartId = "multipart";
    invalidParts.parts.resize(cache::kMaxUploadParts + 1);
    invalidParts.nextPartNumber = cache::kMaxUploadParts + 1;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*txn, 1, invalidParts), StatusCode::kInvalidArg);

    auto sealed = seal(initial, 100);
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await UploadJobStore::update(*txn, 1, sealed));
    CO_ASSERT_OK(co_await txn->commit());

    auto uploading = sealed;
    uploading.state = cache::UploadJobState::UPLOADING;
    uploading.stateVersion = 3;
    uploading.updatedAtMs = 102;
    uploading.multipartId = "multipart";
    uploading.parts.reserve(100);
    for (uint32_t part = 1; part <= 100; ++part) {
      uploading.parts.push_back({part, 1, std::string(cache::kMaxCompletedPartTagBytes, 'e'), {}});
    }
    uploading.stagingLength = 100;
    uploading.nextPartNumber = 101;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*txn, 2, uploading), CacheCode::kRequestTooLarge);

    uploading.parts = {{1, 100, "etag", "checksum"}};
    uploading.nextPartNumber = 2;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await UploadJobStore::update(*txn, 2, uploading));
    CO_ASSERT_OK(co_await txn->commit());

    auto rolledBackPart = uploading;
    rolledBackPart.stateVersion = 4;
    rolledBackPart.updatedAtMs = 103;
    rolledBackPart.parts.clear();
    rolledBackPart.nextPartNumber = 1;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*txn, 3, rolledBackPart), CacheCode::kStateConflict);

    auto changedPart = uploading;
    changedPart.stateVersion = 4;
    changedPart.updatedAtMs = 103;
    changedPart.parts.front().etag = "different";
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*txn, 3, changedPart), CacheCode::kStateConflict);

    auto changedLength = sealed;
    changedLength.state = cache::UploadJobState::UPLOADING;
    changedLength.stateVersion = 4;
    changedLength.updatedAtMs = 103;
    changedLength.multipartId = "multipart";
    changedLength.parts = uploading.parts;
    changedLength.nextPartNumber = uploading.nextPartNumber;
    changedLength.stagingLength = 101;
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::update(*txn, 3, changedLength), CacheCode::kStateConflict);
  }());
}

TEST_F(TestUploadJobStore, RejectsCorruptAndMismatchedRecords) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto expected = openJob(4);
    auto txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await txn->set(UploadJobKey::job(expected.jobId), "corrupt"));
    CO_ASSERT_OK(co_await txn->commit());
    auto read = engine_.createReadonlyTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::snapshotLoad(*read, expected.jobId), StatusCode::kDataCorruption);

    auto other = openJob(5);
    txn = engine_.createReadWriteTransaction();
    CO_ASSERT_OK(co_await txn->set(UploadJobKey::job(other.jobId), serde::serialize(expected)));
    CO_ASSERT_OK(co_await txn->commit());
    read = engine_.createReadonlyTransaction();
    CO_ASSERT_ERROR(co_await UploadJobStore::snapshotLoad(*read, other.jobId), StatusCode::kDataCorruption);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
