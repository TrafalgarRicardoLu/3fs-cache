# Cache Phase 4 acceptance

Record date: 2026-08-10

## Result

Phase 4 reliability recovery and write-through implementation is complete on branch `ljh/dev`. Tasks T0 through T35
provide stable recovery and inventory contracts, bidirectional reconciliation, startup admission fencing, staged writes,
durable multipart checkpoints, atomic OriginFile publish, post-publish warming, admin operations, metrics, fault
injection, and real MinIO qualification.

The feature remains fail-closed and disabled by default. Cache Manager requires Phase 2 and Phase 3 before Phase 4 can
be enabled; Metadata and FUSE have independent Phase 4/write-through gates so a rolling deployment can install the
protocol without routing writes.

## Qualification environment

The local qualification used the existing Clang 14 `RelWithDebInfo` incremental tree with
`HF3FS_ENABLE_CACHE=ON` and `HF3FS_ENABLE_CACHE_INTEGRATION_TESTS=ON`. Changed objects were rebuilt with
`-Wall -Wextra -Werror -Wpedantic`; static archives and focused executables were relinked after protocol or config
header changes. An initially stale Cache Manager test object was force-rebuilt before the final run, so the reported
results do not mix pre-Phase-4 config layouts.

Real object-store qualification used MinIO `RELEASE.2025-09-07T16-13-09Z` on an isolated loopback endpoint with a
process-scoped bucket. The downloaded official binary was SHA-256 verified, and the server, bucket and temporary data
directory were removed after the run.

## Automated evidence

The final focused matrix passed:

| Area | Selection | Result |
| --- | --- | ---: |
| Cache common/origin/contracts/metrics | complete `test_cache` | 51/51 |
| Cache Manager | complete `test_cache_manager` | 169/169 |
| Metadata | `*Cache*:*Upload*:*Origin*:*WriteStaging*` | 69/69 |
| Storage service | `*Cache*:*Inventory*:*Retire*:*Staging*` | 40/40 |
| Storage store | `*Cache*:*Inventory*:*Retire*:*LocalSafety*:*Staging*` | 18/18 |
| Client/FUSE write route | `WriteStagingRoute.*:UploadJobWaiter.*:*Cache*` | 14/14 |
| Management rollout | `*Cache*:*Phase4*:*Rollout*` | 15/15 |
| Admin CLI | complete `test_admin_cli` | 9/9 |
| Real MinIO | complete credentialed `test_cache_minio` | 4/4 |

The T34 failure matrix additionally passed 42/42 Loader, inventory, reconciler, multipart uploader/finalizer and
publish-controller cases. It covers S3 GET, 3FS replace, Metadata commit, inventory epoch/target failure, crash before
every multipart checkpoint, ambiguous Complete with HEAD recovery, Abort retry, and loss of the publish response after
the atomic Metadata commit. Restart resumes from durable state and does not republish an already published inode.

The MinIO Phase 4 case performs a real two-part upload with an S3-compliant 5 MiB non-final part, completes it, recovers
identity with HEAD, rereads and compares the complete object, releases staging data, aborts a second upload, verifies
the upload ID is absent from `ListMultipartUploads`, and confirms no object was published by the aborted upload.

## Safety and compatibility checks

- `configs/cache_manager_main.toml` keeps `enable_phase4 = false`.
- `configs/meta_main.toml` keeps `enable_cache_phase4 = false`.
- `configs/hf3fs_fuse_main.toml` keeps `[write_through].enabled = false`.
- Cache Manager config validation rejects Phase 4 without earlier capabilities, invalid part sizes, invalid
  concurrency relationships, and invalid publish priority.
- Missing MinIO credentials report each scenario as `SKIPPED`; skipped scenarios are not accepted as qualification.
- `clang-format --dry-run --Werror` passed for all Phase 4 C++ changes, and `git diff --check` passed.
- Existing modified dependency submodules and local build/event artifacts were excluded from every commit.

## Deployment boundary

This record qualifies the repository-native and local real-MinIO paths. Production deployment must still repeat the
runbook's dry-run reconcile, staged rollout, drain and rollback checks with its actual FoundationDB cluster, RDMA and
io_uring devices, service identities, object-store credentials, and monitoring backend. External AWS S3 qualification
is separate from the MinIO compatibility run and was not claimed here.

## Post-acceptance recovery hardening

Commit `6206384` closes the review findings discovered after T35 without changing the default-off rollout boundary. It
adds durable Cache Block and Upload Job state indexes with restart-safe migration markers, an OPEN writer-lease index,
service-authenticated expired-OPEN recovery, terminal staging cleanup, owner-level pin leases, immediate recovery when
a LOADING permit is missing, global priority scheduling, and bounded upload/reconcile scans. Recovery policy remains
physical-capacity based; the logical counters are query and audit data rather than an admission limit.

The hardened wire contract appends Metadata RPC IDs 73 through 76. Existing user-authenticated
`recoverExpiredWriteStaging` retains its original request layout and method ID; Cache Manager uses the separate
service-authenticated `recoverExpiredOpenUpload` method so mixed-version deployments do not reinterpret an old payload.

The 2026-08-10 qualification also found and fixed a migration-page boundary before release: a canonical `CBLK` range
larger than the reconcile response page could otherwise publish `CMIG` before every fetched record had been indexed.
Migration now indexes the complete fetched range before writing the completion marker, and the regression test uses a
response limit smaller than the legacy range.

The post-hardening matrix passed:

| Area | Selection | Result |
| --- | --- | ---: |
| Cache common/origin/contracts/metrics | complete `test_cache` | 51/51 |
| Cache Manager | complete `test_cache_manager` | 174/174 |
| Metadata | complete `test_meta` using MemKV | 211 passed |
| Metadata FoundationDB variants | complete `test_meta` | 94 skipped: `FDB_UNITTEST_CLUSTER` unavailable |
| Storage service | `*Cache*:*Inventory*:*Retire*:*Staging*` | 40/40 |
| Storage store | `*Cache*:*Inventory*:*Retire*:*LocalSafety*:*Staging*` | 18/18 |
| Client/FUSE | `WriteStagingRoute.*:UploadJobWaiter.*:*Cache*` | 15/15 |
| Management rollout | `*Cache*:*Phase4*:*Rollout*` | 15/15 |
| Admin CLI | complete `test_admin_cli` | 9/9 |
| Real MinIO | complete credentialed `test_cache_minio` | 4/4 |

The MinIO rerun used the same pinned `RELEASE.2025-09-07T16-13-09Z` binary. Its official SHA-256 manifest verified,
and the isolated server, process-scoped bucket, and temporary data directory were removed after the run. The 94 skipped
FoundationDB variants remain deployment-environment qualification work and are not reported as passes.
