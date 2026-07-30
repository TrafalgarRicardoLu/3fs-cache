# Phase-two capacity-management baseline

Record date: 2026-07-30

## Scope and revision

This record freezes the validation baseline before phase-two implementation starts. It is intended for milestone
comparison, not as new release qualification evidence.

- Branch: `ljh/dev`
- Baseline commit: `c2a7ac405f02f3f447f8a187a16ecd7312c0a93a`
- Approved design commits: `46e57f6`, `7515317`, `a843848`, and `e8f9d95`
- Phase-one acceptance commit: `3b35097`
- Phase-two implementation-plan commit: `c2a7ac4`

The worktree already contained unrelated deleted reading guides, a deleted earlier design document, modified `folly`
and `rocksdb` submodules, the untracked `build-clang/` directory, and the untracked `docs/dev/dev.md`. They are not
part of T0 and must not be staged with phase-two work.

## Build and runtime environment

The reusable `build-clang` tree is configured as follows:

| Setting | Value |
| --- | --- |
| Build type | `RelWithDebInfo` |
| C/C++ compiler | Clang 14 (`clang++-14`) |
| Shuffle method | `g++11` |
| Cache implementation | `HF3FS_ENABLE_CACHE=ON` |
| Cache integration tests | `HF3FS_ENABLE_CACHE_INTEGRATION_TESTS=ON` |
| AWS SDK for C++ | Isolated 1.10.55 installation under `/tmp/hf3fs-aws-sdk-install` |

On this workstation the test executables need the isolated dependency directories in `LD_LIBRARY_PATH`:

```bash
export LD_LIBRARY_PATH=/tmp/hf3fs-aws-sdk-install/lib:\
/tmp/hf3fs-deps.Suskkg/root/usr/lib:\
/tmp/hf3fs-deps.Suskkg/root/usr/lib/x86_64-linux-gnu
```

An initial run without the last two paths failed at dynamic loading and is deliberately excluded from the functional
baseline.

## Frozen test matrix

The cache-enabled configuration registers 17 real CTest targets. Phase two uses the following fixed groups; it does not
add placeholder tests or change the existing CMake test registrations.

### Core, locally reproducible targets

```bash
ctest --test-dir build-clang -R \
  '^(test_analytics|test_meta|test_kv|test_mgmtd|test_cache|test_cache_aws_s3|test_cache_manager|test_admin_cli)$' \
  --output-on-failure
```

Result: **8/8 CTest targets passed** in 195.29 seconds. `test_cache_aws_s3` is registration evidence only in this
environment: both GoogleTest scenarios explicitly skipped because AWS qualification was not enabled.

### Cache-specific Client read path

```bash
cache_client_filter='TestCacheHitReader.*:TestCacheReadPipelineMiss.*:TestMixedRead.*:TestOriginRangePlanner.*:TestBufferAssembler.*:TestLocalMissSingleflight.*:TestOriginMissReader.*:TestReadPlanBatcher.*:TestReadPlanner.*'
build-clang/tests/test_client --gtest_filter="${cache_client_filter}"
```

Result: **16/16 tests passed** across nine suites.

### Storage generation fencing

```bash
build-clang/tests/test_storage_store --gtest_filter='TestCacheGeneration.*'
```

Result: **2/2 tests passed**.

### External MinIO qualification

`test_cache_minio` is part of the matrix only when the following variables are injected by a disposable MinIO test
environment:

```text
HF3FS_CACHE_MINIO_ENDPOINT
HF3FS_CACHE_MINIO_ACCESS_KEY
HF3FS_CACHE_MINIO_SECRET_KEY
```

They were absent on 2026-07-30, so the current run failed its environment precondition. The last qualified Phase-one
run passed 2/2 MinIO tests on 2026-07-29; that evidence is recorded in `docs/dev/cache-phase-1-acceptance.md`.

## Full-suite result and classification

With the complete runtime library path, the full command was:

```bash
ctest --test-dir build-clang --output-on-failure
```

It ran all 17 registered targets in 254.62 seconds: eight CTest targets passed and nine failed. No failure is in a
phase-two implementation because T0 precedes all phase-two code changes.

| Target | Baseline classification | Evidence or constraint |
| --- | --- | --- |
| `test_analytics` | Pass | Full target passed. |
| `test_meta` | Pass | Full target passed; also passed in the frozen core group. |
| `test_kv` | Pass | Full target passed. |
| `test_mgmtd` | Pass | Full target passed. |
| `test_cache` | Pass | Full target passed. |
| `test_cache_manager` | Pass | Full target passed. |
| `test_admin_cli` | Pass | Full target passed. |
| `test_cache_aws_s3` | Explicit external skip | CTest registration passed; both qualification scenarios skipped without explicit AWS enablement. |
| `test_common` | Known environment failure | No RDMA device; initialization reports `RPC::IBInitFailed`. |
| `test_client` | Known baseline failure | 60/61 pass; `MgmtdClientTest.testRetryUnknownAddrs` observes one additional old-address request. Cache-focused Client tests pass 16/16. |
| `test_storage` | Known environment failure | Requires RDMA and/or locally unavailable io_uring registered-buffer resources. |
| `test_storage_client` | Known environment failure | Requires an available RDMA device. |
| `test_storage_service` | Known environment failure | Requires an available RDMA device. |
| `test_storage_store` | Known environment failure | 12/14 pass; `TestBufferPool.Normal` and `TestChunkEngine.ReadWrite` fail when `io_uring_register_buffers` returns `-12`. Cache generation tests pass 2/2. |
| `test_storage_sync` | Known environment failure | Requires an available RDMA device. |
| `test_migration` | Known environment failure | Requires an available RDMA device. |
| `test_cache_minio` | Missing external test environment | The three required MinIO variables were not present in this shell. |

The first eight failing targets after excluding MinIO are the same environment/baseline set documented at the end of
Phase one. MinIO changes the local count from the previously qualified **9 pass / 8 fail** to **8 pass / 9 fail** only
because its disposable endpoint and credentials are absent.

## Milestone comparison rules

- Always run the core group plus the narrow tests for the subsystem changed by the task.
- Treat any new failure in `test_cache`, cache-focused Client tests, `TestCacheGeneration`, Metadata, mgmtd, Cache
  Manager, analytics, KV, or Admin CLI as a candidate regression.
- Do not count AWS or MinIO as passed unless their internal scenarios execute rather than skip or fail preconditions.
- Keep RDMA, io_uring, FDB, external-object-store, and the existing Client retry assertion separate from functional
  regressions. If one changes, record the exact environment and compare its focused test output.
- At each milestone, record both CTest target counts and GoogleTest case counts for any filtered binary.

This baseline is sufficient to start T1: all locally reproducible cache control/data-path checks pass, and every
remaining failure has an explicit pre-existing or external-environment classification.
