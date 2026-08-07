# Cache Phase 4 baseline

Record date: 2026-08-07

## Scope and revision

This record freezes the reliability-recovery and write-through baseline before Phase 4 implementation starts.

- Branch: `ljh/dev`
- Phase 3 acceptance commit: `dc9529eb2ccdc0c6f634e63a99ffb37344d335d2`
- Phase 4 design: `docs/superpowers/specs/2026-08-07-phase-4-reliability-write-design.md`
- Phase 4 implementation plan: `docs/superpowers/plans/2026-08-07-phase-4-reliability-write-implementation.md`

The user worktree contains modified `third_party/folly` and `third_party/rocksdb` submodules and the untracked
`build-clang/` directory. They are not Phase-4 source changes and must not be staged. The build directory also contains
a local generated include-order workaround needed by the available Clang 14 environment; it is not a repository patch.

## Existing reliability boundary

At this revision the project already provides generation-fenced cache load/commit/fail, expired-lease takeover on a
new miss, QUEUED permit recovery, READY invalidation, durable Storage events, EVICTING/CLEANING replay, Phase-3 job
recovery, pins and physical-capacity admission. The following deliberate gaps define the Phase-4 starting point:

- startup permit recovery skips a LOADING record when its permit is absent or expired;
- Metadata recovery scans use an in-memory full snapshot rather than stable FDB pages;
- Storage can query specified cache keys but cannot page all persisted cache descriptors for reverse reconciliation;
- no bidirectional Reconciler or unified startup recovery barrier exists;
- ObjectStore supports head/range-get/list only, with no multipart mutation API;
- OriginFile is fail-closed read-only and no WRITE_STAGING role, UploadJob or atomic staged publish operation exists.

## Build and runtime environment

`build-clang/` is a Clang 14 `RelWithDebInfo` incremental tree configured with `HF3FS_ENABLE_CACHE=ON` and
`HF3FS_ENABLE_CACHE_INTEGRATION_TESTS=ON`. It links AWS SDK for C++ 1.10.55 from a temporary local prefix. CTest
registers the seven focused targets `test_cache`, `test_cache_minio`, `test_cache_manager`, `test_admin_cli`,
`test_meta`, `test_storage`, and `test_storage_store`.

The Phase-3 closure on this revision built the cache-enabled AWS executor and `test_cache_minio`, then passed 3/3 real
MinIO tests against MinIO `RELEASE.2025-09-07T16-13-09Z`. The preceding focused qualification passed 118 Cache Manager,
37 cache/common, 49 Metadata, 10 mgmtd and 6 Admin CLI cases. These counts are lower bounds for Phase-4 regression;
new passing tests may only increase them.

FoundationDB, RDMA, io_uring, external AWS S3 and process-crash scenarios count only when their service/device/runtime
prerequisites are available. A missing prerequisite is an environment limitation. Once setup succeeds, an assertion,
unexpected timeout, crash, sanitizer report or changed focused result is a functional failure.

## Frozen focused matrix

Use the existing incremental tree and build once per completed milestone:

```bash
cmake --build build-clang --target \
  test_cache test_cache_manager test_meta test_storage test_storage_store test_admin_cli test_cache_minio -j 2

ctest --test-dir build-clang -R \
  '^(test_cache|test_cache_minio|test_cache_manager|test_admin_cli|test_meta|test_storage|test_storage_store)$' \
  --output-on-failure

build-clang/tests/test_cache_manager --gtest_filter='*Recovery*:*Reconcile*:*Permit*:*Evict*:*Job*'
build-clang/tests/test_meta --gtest_filter='*Cache*:*Upload*:*Origin*'
build-clang/tests/test_storage --gtest_filter='*Cache*:*Inventory*:*Retire*'
build-clang/tests/test_storage_store --gtest_filter='*Cache*:*LocalSafety*:*Staging*'
build-clang/tests/test_cache --gtest_filter='*Origin*:*ObjectStore*:*Multipart*:*ServiceContracts*'
build-clang/tests/test_admin_cli --gtest_filter='*Cache*:*Upload*:*Reconcile*'
```

Real MinIO qualification must use an isolated disposable bucket and explicit `HF3FS_CACHE_MINIO_*` environment
variables. It must cover multipart create/part/complete/abort and read-after-publish before Phase 4 can close. A skipped
MinIO or process-restart scenario is never reported as passed.

## T0 exit condition

The design assumptions, known gaps, dirty-worktree exclusions, environment boundary and regression matrix are frozen.
Phase 4 remains disabled and T0 introduces no runtime mutation.
