# Cache Phase 4 acceptance

Record date: 2026-08-08

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
