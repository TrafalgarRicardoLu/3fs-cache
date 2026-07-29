# Phase-one read-cache acceptance record

Record date: 2026-07-28

## Delivery status

All 24 implementation tasks, T0 through T23, are represented by focused commits and repository tests. The implementation
is complete for the approved MVP scope. Formal M5 environment qualification is still pending because this workstation
does not provide an installed AWS SDK CMake package, a provisioned MinIO endpoint, or AWS qualification credentials.
The two suites and their manual workflows are present so those results can be attached without code changes.

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
| T23 | Benchmark, fault matrix, runbook, and final acceptance | current delivery commit |

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
| MinIO | Not run: cache-enabled AWS SDK package and provisioned endpoint are unavailable on this workstation. Source was syntax-checked against AWS SDK for C++ 1.10.55 headers. | Run `test_cache_minio`; archive CTest output proving boundary, cold/fill/warm/mixed/refresh/version/capacity/cleanup cases and zero origin requests on the all-hit read. |
| AWS S3 | Ordinary cache-off registration reports explicit `SKIPPED`; no credentials or disposable buckets were supplied. Source was syntax-checked against AWS SDK for C++ 1.10.55 headers. | Run `test_cache_aws_s3` with explicit qualification enablement; archive CTest output and the sanitized log with both scenarios passing and `cleanup_failures=0`. |

An external release must not mark M5 qualified until both rows have real evidence. See the linked procedures in the
[phase-one runbook](cache-phase-1-runbook.md).

## Known local validation constraints

- The ordinary build keeps `HF3FS_ENABLE_CACHE=OFF` because the installed environment has no AWS SDK CMake package.
- The final all-target build stops in the pre-existing jemalloc external project because this workstation does not
  provide `autoconf`; focused cache, Manager, Client, Metadata, Storage, Admin, and benchmark targets build separately.
- The full FUSE target is constrained by the workstation's older fuse3 headers, which do not expose the
  `fuse_loop_cfg_*` API used by the repository. Modified FUSE translation units were compiled individually.
- Full `test_client` currently fails the unrelated `MgmtdClientTest.testRetryUnknownAddrs` baseline assertion because an
  additional old-address probe appears in the observed call sequence. Cache-specific Client tests pass.
- The all-CTest invocation has 16 registered targets: Metadata, cache, Manager, and Admin pass; AWS is skipped as
  designed; eight binaries are unavailable after the all-target build stops; and the existing Client plus three Storage
  suites report environment/baseline failures. The focused cache-generation Storage test passes.
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
