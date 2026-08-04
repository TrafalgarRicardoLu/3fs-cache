# Cache Phase 3 baseline

Record date: 2026-08-04

## Scope and revision

This record freezes the data-orchestration baseline before Phase 3 implementation starts. It is comparison evidence
for T0, not a replacement for deployment qualification.

- Branch: `ljh/dev`
- Phase 2 acceptance commit: `b8a87a7deccaf6c49f394a585dfe9a4d3ad27b51`
- Phase 3 design: `docs/superpowers/specs/2026-08-04-phase-3-data-orchestration-design.md`
- Phase 3 implementation plan: `docs/superpowers/plans/2026-08-04-phase-3-data-orchestration-implementation.md`

The worktree already contains unrelated deleted reading guides and an earlier design document, modified `folly` and
`rocksdb` submodules, the untracked `build-clang/` directory, and the untracked `docs/dev/dev.md`. They are user work
and must not be staged with Phase 3 changes.

## Build and runtime environment

The clean Phase 2 qualification build remains at `/home/trafalgarlu/.cache/hf3fs-phase2-build`. It was configured as
`RelWithDebInfo` with Clang 14, cache support enabled, AWS SDK for C++ 1.10.55, and MinIO
`RELEASE.2025-09-07T16-13-09Z`. The build tree and its CTest registration were present when this baseline was recorded.

External MinIO, FoundationDB, RDMA, io_uring, and AWS S3 tests only count as functional evidence when their required
services, devices, credentials, and runtime libraries are available. A missing prerequisite is an environment failure;
an assertion, sanitizer, crash, timeout after successful setup, or changed focused-test result is a candidate functional
failure.

## Frozen focused matrix

Run the following groups after each applicable milestone. Filters are intentionally based on the Phase 2 acceptance
labels so Phase 3 cannot silently weaken the existing cache safety coverage.

```bash
phase3_build=/home/trafalgarlu/.cache/hf3fs-phase2-build

ctest --test-dir "${phase3_build}" -R \
  '^(test_cache|test_cache_minio|test_cache_manager|test_admin_cli)$' \
  --output-on-failure

"${phase3_build}/tests/test_cache" --gtest_filter='*Cache*:*Origin*:*ServiceContracts*'
"${phase3_build}/tests/test_cache_manager" --gtest_filter='*'
"${phase3_build}/tests/test_mgmtd" --gtest_filter='*CachePhase2*:*CacheChain*'
"${phase3_build}/tests/test_meta" --gtest_filter='*Cache*'
"${phase3_build}/tests/test_storage" --gtest_filter='*Cache*:*Retire*'
"${phase3_build}/tests/test_storage_store" --gtest_filter='*Cache*:*LocalSafety*'
```

For object-store work, create a disposable MinIO instance and run `test_cache_minio` with
`HF3FS_CACHE_MINIO_ENDPOINT`, `HF3FS_CACHE_MINIO_ACCESS_KEY`, and `HF3FS_CACHE_MINIO_SECRET_KEY`. Do not report a
skipped scenario as passed.

## Starting results

The final Phase 2 qualification at the baseline commit passed:

- 4/4 registered CTest targets: cache, MinIO, Cache Manager, and Admin CLI;
- 29/29 cache protocol, metrics, and serde cases;
- 68/68 Cache Manager admission, permit, capacity, and eviction cases;
- 7/7 mgmtd capability and rollout cases;
- 31/31 Metadata cache state, capacity, event, and read-plan cases;
- 23/23 Storage cache/event/local-safety/retirement cases;
- 2/2 real MinIO scenarios;
- the Rust release build and Clang 14 formatting validation.

Phase 3 starts disabled and has no runtime mutation path at T0. A milestone is acceptable only if its new narrow tests
pass and all applicable counts above remain unchanged or increase solely through new passing tests. Full-repository
CTest failures that require unavailable infrastructure must be recorded separately with their exact prerequisite; they
must not conceal a focused cache regression.
