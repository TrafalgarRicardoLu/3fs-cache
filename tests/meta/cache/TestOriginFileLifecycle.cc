#include <fcntl.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "meta/store/DirEntry.h"
#include "meta/store/Inode.h"
#include "tests/GtestHelpers.h"
#include "tests/meta/MetaTestBase.h"

namespace hf3fs::meta::server {
namespace {

class TestOriginFileLifecycle : public MetaTestBase<mem::MemKV> {};

TEST_F(TestOriginFileLifecycle, ReadOnlyNamespaceBoundaries) {
  folly::coro::blockingWait([&]() -> CoTask<void> {
    auto cluster = createMockCluster();
    auto &meta = cluster.meta().getOperator();

    auto created = co_await meta.create({SUPER_USER, "origin", {}, O_RDONLY, p644});
    CO_ASSERT_OK(created);
    auto object = cache::ImmutableObjectIdentity{
        cache::OriginId{1},
        "bucket",
        "key",
        cache::VersionSelector{cache::VersionSelectorType::STRONG_ETAG, "etag"},
    };
    auto origin = Inode::newOriginFile(created->stat.id,
                                       created->stat.acl,
                                       8192,
                                       created->stat.asFile().layout,
                                       std::move(object),
                                       UtcClock::now());
    auto entry = DirEntry::newOriginFile(InodeId::root(), "origin", origin.id);
    READ_WRITE_TRANSACTION_OK({
      CO_ASSERT_OK(co_await origin.store(*txn));
      CO_ASSERT_OK(co_await entry.store(*txn));
    });

    auto opened = co_await meta.open({SUPER_USER, "origin", {}, O_RDONLY});
    CO_ASSERT_OK(opened);
    CO_ASSERT_TRUE(opened->stat.isOriginFile());
    CO_ASSERT_EQ(opened->stat.fileLength(), uint64_t{8192});

    auto session = MetaTestHelper::randomSession();
    CO_ASSERT_ERROR(co_await meta.open({SUPER_USER, "origin", session, O_WRONLY}), CacheCode::kReadOnlyOriginFile);
    CO_ASSERT_ERROR(co_await meta.open({SUPER_USER, "origin", session, O_RDWR}), CacheCode::kReadOnlyOriginFile);
    CO_ASSERT_ERROR(co_await meta.open({SUPER_USER, "origin", session, O_TRUNC | O_RDWR}),
                    CacheCode::kReadOnlyOriginFile);
    CO_ASSERT_ERROR(co_await meta.remove({SUPER_USER, "origin", AtFlags{}, false}), CacheCode::kReadOnlyOriginFile);
    CO_ASSERT_ERROR(co_await meta.rename({SUPER_USER, "origin", "renamed"}), CacheCode::kReadOnlyOriginFile);
    CO_ASSERT_ERROR(co_await meta.rename({SUPER_USER, "origin", "origin"}), CacheCode::kReadOnlyOriginFile);
    CO_ASSERT_ERROR(co_await meta.hardLink({SUPER_USER, "origin", "linked", AtFlags{}}),
                    CacheCode::kReadOnlyOriginFile);

    CO_ASSERT_OK(co_await meta.create({SUPER_USER, "ordinary", {}, O_RDONLY, p644}));
    CO_ASSERT_ERROR(co_await meta.rename({SUPER_USER, "ordinary", "origin"}), CacheCode::kReadOnlyOriginFile);
  }());
}

}  // namespace
}  // namespace hf3fs::meta::server
