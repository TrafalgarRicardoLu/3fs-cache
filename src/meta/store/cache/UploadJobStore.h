#pragma once

#include <optional>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"
#include "fbs/meta/Service.h"

namespace hf3fs::meta::server {

struct CreateUploadJobResult {
  cache::UploadJobRecord job;
  bool created{false};
};

struct UploadJobPage {
  std::vector<cache::UploadJobRecord> jobs;
  bool more{false};
};

struct ExpiredOpenUploadPage {
  std::vector<cache::UploadJobRecord> jobs;
  bool more{false};
};

class UploadJobStore {
 public:
  static CoTryTask<CreateUploadJobResult> create(kv::IReadWriteTransaction &txn, const cache::UploadJobRecord &job);
  static CoTryTask<std::optional<cache::UploadJobRecord>> snapshotLoad(kv::IReadOnlyTransaction &txn,
                                                                       cache::UploadJobId jobId);
  static CoTryTask<std::optional<cache::UploadJobRecord>> load(kv::IReadWriteTransaction &txn,
                                                               cache::UploadJobId jobId);
  static CoTryTask<UploadJobPage> snapshotList(kv::IReadOnlyTransaction &txn,
                                               std::optional<flat::Uid> ownerUid,
                                               std::optional<cache::UploadJobId> after,
                                               uint32_t limit,
                                               bool includeTerminal = true);
  static CoTryTask<ExpiredOpenUploadPage> snapshotListExpiredOpen(
      kv::IReadOnlyTransaction &txn,
      uint64_t expiresBeforeMs,
      std::optional<meta::UploadOpenLeaseCursor> after,
      uint32_t limit);
  static CoTryTask<cache::UploadJobRecord> update(kv::IReadWriteTransaction &txn,
                                                  uint64_t expectedStateVersion,
                                                  const cache::UploadJobRecord &job);
  static CoTryTask<void> removeActive(kv::IReadWriteTransaction &txn, cache::UploadJobId jobId);
  static CoTryTask<void> indexActive(kv::IReadWriteTransaction &txn, const cache::UploadJobRecord &job);
  static CoTryTask<void> finishStateIndexMigration(kv::IReadWriteTransaction &txn);
};

}  // namespace hf3fs::meta::server
