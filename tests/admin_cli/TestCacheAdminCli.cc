#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "client/cli/admin/CacheOriginCli.h"
#include "client/cli/admin/registerAdminCommands.h"

namespace hf3fs::client::cli::test {
namespace {

TEST(CacheAdminCli, RegistersLifecycleCommands) {
  Dispatcher dispatcher;
  auto result = folly::coro::blockingWait(registerAdminCommands(dispatcher));
  ASSERT_FALSE(result.hasError()) << result.error().describe();

  const auto usages = dispatcher.getUsages();
  for (const auto *command :
       {"cache-import", "cache-refresh-origin", "cache-status", "cache-list-blocks", "cache-cleanup"}) {
    EXPECT_TRUE(usages.contains(command)) << command;
  }
}

TEST(CacheAdminCli, RefreshRequestIdIsStableAndIdentitySensitive) {
  const meta::PathAt path{meta::InodeId::root(), Path{"data/model"}};
  const cache::ImmutableObjectIdentity first{cache::OriginId{1},
                                             "bucket",
                                             "model",
                                             {cache::VersionSelectorType::VERSION_ID, "version-1"}};
  auto same = cache_admin::stableRefreshRequestId(path, meta::InodeId{101}, first);
  EXPECT_EQ(same, cache_admin::stableRefreshRequestId(path, meta::InodeId{101}, first));

  auto changed = first;
  changed.version.value = "version-2";
  EXPECT_NE(same, cache_admin::stableRefreshRequestId(path, meta::InodeId{101}, changed));
  EXPECT_NE(same, cache_admin::stableRefreshRequestId(path, meta::InodeId{102}, first));
}

}  // namespace
}  // namespace hf3fs::client::cli::test
