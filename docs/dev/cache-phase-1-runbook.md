# Phase-one read-cache runbook

This runbook covers rollout, validation, routine administration, and rollback for the S3-backed, read-only cache MVP.
The origin object remains authoritative. Cached chunks are replaceable data, and an `OriginFile` is never writable.

## Build and prerequisites

Initialize the repository dependencies and configure with cache support explicitly enabled:

```bash
git submodule update --init --recursive
./patches/apply.sh
cmake -S . -B build \
  -DCMAKE_CXX_COMPILER=clang++-14 \
  -DCMAKE_C_COMPILER=clang-14 \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DSHUFFLE_METHOD=g++11 \
  -DHF3FS_ENABLE_CACHE=ON
cmake --build build -j 32
```

`HF3FS_ENABLE_CACHE=ON` requires the AWS SDK for C++ S3 component and its CMake package. Add
`HF3FS_ENABLE_CACHE_INTEGRATION_TESTS=ON` only in an environment that provisions MinIO; that switch deliberately makes
missing endpoint or credentials a test failure.

Credentials must come from the local AWS provider chain or protected process configuration. Do not place access keys,
secret keys, session tokens, signed URLs, production bucket names, or account identifiers in repository files or logs.

## Prepare and roll out

1. Create a dedicated `CACHE_DATA` chain table with its logical capacity and checksum type. Do not reuse a normal data
   chain table.
2. Deploy mgmtd and all Metadata writers first. Confirm every active Metadata node advertises the required cache schema
   and protocol capability; import remains fail-closed until the feature gate is satisfied.
3. Configure a non-empty, dedicated service identity in `cache_manager_main.toml`, map every allowed `origin_id` to its
   endpoint and region, and start `cache_manager_main`. The checked-in config is an example and its empty token is not a
   deployable secret.
4. Configure FUSE `read_cache` with the same origin mappings, the Cache Manager address, and a least-privilege client
   service identity. Set `read_cache.enabled = true` only after the manager and Metadata gate are healthy.
5. Configure `cache_manager_address` for `admin_cli`. Cache import, refresh, cleanup, and global status require a user
   authorized server-side as cache admin (root or `UserAttr.admin`).

Keep endpoint, TLS, path-style, region, and `origin_id` mappings consistent across Admin, Cache Manager, and clients.
An inode stores only the immutable object identity, never connection details or credentials.

## Smoke test

Use `admin_cli` interactively, or pass the following commands through its normal command input. First import a disposable
object using the dedicated cache table:

```text
cache-import /cache/smoke objects/smoke --origin-id 1 --bucket disposable-bucket --table-id 100 --block-size 65536
```

Then validate this sequence:

1. `stat` identifies the path as an `OriginFile` with the expected immutable VersionId or strong ETag.
2. The first read returns the exact source bytes. It may be entirely or partly an origin miss.
3. `cache-status` shows queued/loading work and eventually committed capacity.
4. `cache-list-blocks --inode <inode> --begin 0 --limit 1000` eventually shows READY blocks with generation, length,
   and checksum.
5. A repeated read returns identical bytes. Metrics should show cache-hit bytes and no origin requests for an all-hit
   range.
6. Write, truncate, writable open, fallocate, unlink, and rename attempts fail with the read-only/lifecycle error.

Capacity exhaustion is a supported state: existing READY blocks remain readable, foreground misses continue to read the
origin, and new admissions are reported as bypassed.

## Refresh and cleanup

When the authoritative object identity changes, do not overwrite or rename the existing `OriginFile`. Refresh it:

```text
cache-refresh-origin /cache/smoke objects/smoke --origin-id 1 --bucket disposable-bucket
```

Save the printed `RequestId` and `CleanupJobId`. Retrying the same request ID is idempotent. Attach or resume cleanup with:

```text
cache-cleanup --job-id <CleanupJobId>
```

If the command reaches its batch limit, rerun it with the same job ID. For an explicit range cleanup use:

```text
cache-cleanup --inode <inode> --begin 0 --count 1000
```

Never remove cache chunks through an unfenced Storage delete. Cleanup must pass through Metadata `CLEANING`, the Cache
Manager worker, and the Storage generation tombstone before capacity is released.

## Observe and diagnose

- `cache-status [--inode <inode>]` reports logical, used, reserved, committed and free capacity; block-state counts;
  manager queue/loading/inflight/ready/cleaning state; and the last admission-bypass reason.
- `cache-list-blocks --inode <inode> ...` provides stable pagination. Continue from the `NEXT` block when present.
- Client metrics cover plan, hit/miss/origin bytes, Storage/origin latency, hint, and invalid-report results.
- Metadata, Manager, and Storage metrics cover state transitions and charges, scheduling/loading/cleanup, and generation
  replacement/stale/tombstone outcomes.

For a failed READY read, the client falls back only that segment to the origin and reports the observed READY identity.
The expected recovery is READY to CLEANING, a fenced tombstone, then optional re-enqueue. A stopped Cache Manager must
not prevent cold origin reads or existing cache hits; queued work reattaches on a later miss.

## Rollback

1. Stop new imports and refreshes.
2. Set client/FUSE `read_cache.enabled = false` and roll clients back to their normal 3FS-file path. Existing
   `OriginFile` paths require a cache-capable reader; never deploy a binary that interprets an unknown inode variant as a
   normal file.
3. Drain or explicitly clean cache jobs, retaining Storage tombstones. Stop Cache Manager only after foreground cache
   routing is disabled.
4. Keep Metadata/mgmtd writers at a cache-schema-capable version while any `OriginFile`, cleanup job, cache record, or
   generation tombstone remains. The feature gate intentionally forbids an unsafe schema rollback.

Rollback does not convert an `OriginFile` to a writable 3FS file. If the namespace mapping must be retired, finish its
cleanup lifecycle and remove it through the cache-aware administrative/GC path.

## Qualification

- Run the mandatory MinIO suite as described in [cache-minio-integration.md](cache-minio-integration.md).
- Run the credentialed AWS release gate and archive its sanitized evidence as described in
  [cache-aws-qualification.md](cache-aws-qualification.md).
- Record the non-gating component baseline with `cache_bench`; see
  [the benchmark README](../../benchmarks/cache_bench/README.md).
