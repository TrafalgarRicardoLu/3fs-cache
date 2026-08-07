#pragma once

#include <optional>
#include <vector>

#include "common/kv/ITransaction.h"
#include "common/utils/Coroutine.h"
#include "fbs/cache/Common.h"

namespace hf3fs::meta::server {

struct CreateUploadJobResult {
  cache::UploadJobRecord job;
  bool created{false};
};

struct UploadJobPage {
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
                                               uint32_t limit);
  static CoTryTask<cache::UploadJobRecord> update(kv::IReadWriteTransaction &txn,
                                                  uint64_t expectedStateVersion,
                                                  const cache::UploadJobRecord &job);
};

}  // namespace hf3fs::meta::server
