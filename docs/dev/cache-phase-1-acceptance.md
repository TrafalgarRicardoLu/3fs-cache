# Phase-one read-cache acceptance record

Record date: 2026-07-29

## Delivery status

All 24 implementation tasks, T0 through T23, are represented by focused commits and repository tests. The implementation
is complete for the approved MVP scope. The mandatory MinIO environment qualification has passed. Formal M5 release
qualification is still pending because this workstation does not provide AWS credentials or the two disposable AWS
buckets. The AWS suite and its manual workflow are present so that evidence can be attached without code changes.

| Task | Delivered capability | Commit |
| --- | --- | --- |
| T0 | Cache test targets and baseline | `a7724c6` |
| T1 | Shared cache identities, ranges, generations, and errors | `98bb6aa` |
| T2 | Fail-closed `OriginFile` inode variant | `a245921` |
| T3 | Read-only namespace and GC lifecycle boundaries | `fa0f3e5` |
| T4 | `CACHE_DATA` role and capability gate | `c7657e5` |
| T5 | Metadata, Storage, and Manager service contracts | `59e7458` |
| T6 | Cache block persistence and single capacity charge | `390cb57` |
| T7 | Version-fenced S3 object-store adapter | `fcfa1b7` |
| T8 | Import, batch import, and idempotent refresh | `0823b05` |
| T9 | Shared pipeline skeleton and miss-only native reads | `8014bea` |
| T10 | Cache-aware read plans | `21d040d` |
| T11 | Fenced cache state transitions | `a19afc9` |
| T12 | Generation-fenced Storage replace, read, and tombstones | `3afeef4` |
| T13 | Persistent, resumable cleanup jobs | `f9291e7` |
| T14 | Cache Manager service and scheduler skeleton | `253b58f` |
| T15 | Admission, scheduling, and background loader | `2ecc104` |
| T16 | Invalid-block and administrative cleanup workflow | `f14eecc` |
| T17 | Native mixed hit/miss cache pipeline | `4adbed3` |
| T18 | FUSE routing through the shared pipeline | `1c46290` |
| T19 | Import, refresh, status, list, and cleanup Admin CLI | `e061d8b` |
| T20 | Cross-component metrics and safe structured identity tags | `ba88869` |
| T21 | Real MinIO integration suite and workflow | `1f6b697` |
| T22 | VersionId/If-Match AWS qualification suite | `05ad959` |
| T23 | Benchmark, fault matrix, runbook, and final acceptance | `e0ad965` |

## Fault-injection evidence

The following focused tests passed in the local cache-off build. Cache-off affects only the AWS transport factory; these
tests exercise the relevant state machines and adapters directly.

| Required fault | Test evidence | Expected behavior |
| --- | --- | --- |
| S3 timeout, 429, and 500 | `S3ObjectStore.RecoversFromTimeoutThrottleAndServerFaults` | Retries only transient failures within the configured budget and recovers on the next valid response. |
| Cache Manager stop/unavailable | `TestMixedRead.CacheManagerFailureDoesNotAffectColdRead`; `TestCacheManagerLifecycle.StartStopAndRepeatedStop` | Foreground origin reads remain successful and lifecycle stop is idempotent. |
| Storage stale generation | `TestCacheGeneration.ReplaceRetireAndFence` | A retired or newer generation fences delayed writes from reviving stale data. |
| Commit timeout/error | `TestCacheLoader.FailsFenceOnOriginWriteAndCommitErrors` | Loader reports fenced failure; it does not publish READY after an ambiguous/failed commit. |

## Component benchmark baseline

Command:

```bash
build-clang/bin/cache_bench \
  --iterations=50 \
  --object_size=4194304 \
  --read_size=262144 \
  --block_size=65536 \
  --output=/tmp/hf3fs-cache-bench.csv
```

Recorded output:

| Scenario | MiB/s | P50 us | P99 us | S3 requests | Hit ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| cold_direct_s3 | 1745.660 | 121.900 | 200.054 | 50 | 0.000 |
| foreground_miss | 4454.007 | 44.795 | 124.263 | 200 | 0.000 |
| warm_hit | 7527.060 | 25.813 | 71.185 | 0 | 1.000 |
| mixed | 8008.175 | 29.942 | 48.917 | 100 | 0.500 |
| native_3fs | 33616.609 | 7.277 | 10.493 | 0 | 1.000 |

These are synthetic, component-level in-memory measurements. They are not deployed-cluster or external-S3 performance
claims, and no phase-one threshold is applied.

## External qualification status

| Suite | Local result | Required release evidence |
| --- | --- | --- |
| MinIO | **Passed 2026-07-29.** Built with AWS SDK for C++ 1.10.55 and ran against MinIO `RELEASE.2025-09-07T16-13-09Z` on an isolated loopback endpoint. `test_cache_minio` passed both tests in 0.21 seconds; the test bucket was removed and only MinIO internal metadata remained in the data root. | Complete. The suite covered the boundary matrix, cold/fill/warm/mixed/refresh/version/capacity/cleanup cases and asserted zero origin requests on the all-hit read. |
| AWS S3 | The cache-enabled AWS SDK 1.10.55 executable builds and registers normally. Both scenarios explicitly reported `SKIPPED` because qualification was not enabled; no credentials or disposable buckets were supplied. | Run `test_cache_aws_s3` with explicit qualification enablement; archive CTest output and the sanitized log with both scenarios passing and `cleanup_failures=0`. |

An external release must not mark M5 qualified until both rows have real evidence. See the linked procedures in the
[phase-one runbook](cache-phase-1-runbook.md).

## Known local validation constraints

- The cache-enabled qualification build uses isolated AWS SDK for C++ 1.10.55, autoconf 2.71, and the repository CI's
  pinned libfuse 3.16.2 under `/tmp`; these dependencies are not installed system-wide. With them, the cache-enabled
  all-target build completes successfully, including FUSE, services, tests, benchmarks, and Python bindings.
- Full `test_client` currently fails the unrelated `MgmtdClientTest.testRetryUnknownAddrs` baseline assertion because an
  additional old-address probe appears in the observed call sequence. Cache-specific Client tests pass.
- The complete cache-enabled CTest run executes all 17 registered targets in 253.74 seconds: nine pass, including
  analytics, Metadata, KV, mgmtd, cache, MinIO, AWS qualification registration, Cache Manager, and Admin CLI. Both AWS
  cases skip as designed. Eight baseline/environment targets fail: Common and Migration require an available RDMA
  device; Client retains the unrelated retry-sequence assertion; and the Storage suites require RDMA or available
  `io_uring_register_buffers` resources. Both cache-generation Storage cases pass.
- Repository-wide formatting currently reports unrelated pre-existing failures outside the cache task. Every modified
  cache source is checked separately with the repository clang-format configuration.

## Deferred backlog

The following remain explicitly outside phase one:

- automatic eviction, low-watermark reclamation, and victim selection;
- full reconcile, Storage event journal/inventory scan, emergency eviction, and persistent Manager task recovery;
- prefetch jobs, manifests, prefix planning, pinning, and frequency-based admission;
- write-through, `WRITE_STAGING`, multipart upload, and any writable `OriginFile` path;
- multi-Manager high availability and performance tuning/hard thresholds.

These items require later designs and must not weaken the phase-one generation, cleanup, capacity, or fail-closed
invariants.
