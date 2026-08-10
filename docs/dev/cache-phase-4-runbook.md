# Phase 4 write-through operations

Phase 4 is disabled by default on Cache Manager, Metadata, and FUSE. Enabling only one component is unsupported. Keep
the Cache Manager service token and object-store credentials in the deployment secret store; never put them in CLI
output, archived status tables, or this runbook.

## Configuration checklist

- Cache Manager: set `enable_phase2`, `enable_phase3`, and `enable_phase4`; verify upload interval/page/concurrency,
  cache table/layout, multipart part size/retry limit, reconcile interval/page/runtime, and lease recovery interval.
- Metadata: configure the matching cache service identity, keep `enable_cache_phase4 = false` until its binary and
  schema are deployed, and choose `write_staging_expired_action = 'recover'` (default) or `'cancel'`.
- FUSE: configure `[write_through]` origin, bucket, deterministic key prefix, WRITE_STAGING table, writer lease, publish
  timeout, and poll interval. Keep `write_through.enabled = false` until the cluster rollout is enabled.
- Routing: the destination table must have role `CACHE_DATA`; the staging table must have role `WRITE_STAGING` and is
  not counted as ordinary cache capacity.

The checked-in examples in `configs/cache_manager_main.toml`, `configs/meta_main.toml`, and
`configs/hf3fs_fuse_main.toml` contain all Phase 4 fields with the feature gates off.

## Online index migration

The hardened Metadata binary introduces rebuildable Cache Block state indexes (`CBST`/`CMIG`), upload active/lease/state
indexes (`UPAJ`/`UPOL`/`UPSJ`/`UPMG`), and owner-level pin leases (`PNOL`). Deploy Metadata before Cache Manager so no
new worker depends on an index that the serving Metadata binary does not understand.

Do not create migration markers or copy FDB keys manually. Before `CMIG` exists, the first repairing or dry-run
reconcile pages through canonical `CBLK` records and backfills the state index transactionally. Upload startup recovery
similarly pages through canonical `UPJB` records and writes `UPMG` after the final page. Mutations maintain canonical
records and indexes in the same transaction, so interrupted migration is safely repeatable.

Keep admission and FUSE write-through disabled until one complete startup recovery and `cache-reconcile dry-run` have
succeeded. A missing marker means migration is incomplete, not corrupt; a present marker with a missing or mismatched
index entry is data corruption and must fail closed. Preserve the canonical records and all index prefixes during
rollback. Older binaries ignore the new prefixes, while a later hardened binary can resume or rebuild them.

## Upgrade and enable

1. Deploy Storage inventory/schema support, then the hardened Metadata operations, then Cache Manager workers, and
   finally FUSE.
2. Enable the Phase 2 and Phase 3 capabilities first. Confirm their rollout status is healthy.
3. Enable Phase 4 in Metadata and Cache Manager configuration, but leave FUSE write routing disabled.
4. Allow startup upload recovery to finish, then run `cache-reconcile dry-run --timeout-ms 300000`. Run
   `cache-reconcile status`; require `HEALTHY`, zero conflicts and retryable items, and `DryRun=true`. Investigate rather
   than suppressing unknown/newer generations.
5. Run `cache-phase4-rollout enable`. Only after that succeeds, enable FUSE `[write_through]` on a canary and perform a
   create, sequential write, `fsync`, close, reopen, and read verification.
6. Expand the FUSE rollout while monitoring the metrics below and `cache-upload status --active-only --limit 100`.

A repairing pass is destructive and always requires explicit acknowledgement:

```text
cache-reconcile run --confirm --timeout-ms 300000
```

## Upload administration

`cache-upload status` returns one bounded page (default 100); continue with the last job ID using `--after`. Filter
with `--owner-uid`, `--active-only`, or query a single `--job-id`. Output contains only stable job state, progress,
timestamps, and a `HasError` flag. It intentionally omits multipart IDs, credentials, and raw backend errors.

Cancellation and retry are fenced by the latest durable `stateVersion` and require confirmation:

```text
cache-upload cancel --job-id <uuid> --confirm
cache-upload retry --job-id <uuid> --confirm
```

Cancel rejects publishing/published jobs. Multipart jobs enter `ABORTING` and remain retained until abort is confirmed.
Retry accepts only `FAILED` jobs and resumes from the durable multipart checkpoints; it never creates a new destination
key or discards completed parts.

## Drain and rollback

1. Disable new FUSE write-through creates, while keeping existing handles and Cache Manager upload workers alive.
2. Run `cache-phase4-rollout drain`. Inspect `cache-upload status --active-only`; wait for zero non-terminal uploads or
   explicitly cancel selected jobs. Do not delete staging chunks, multipart uploads, or FDB keys by hand.
3. Run a final `cache-reconcile dry-run`; require healthy, conflict-free output and zero non-terminal recovery work.
4. Disable Phase 4 rollout, then disable Cache Manager and Metadata Phase 4 gates. Roll back FUSE last.
5. Preserve Phase 4 keyspaces and published OriginFile metadata. Older readers ignore append-only fields; removing the
   durable records would make recovery and audit impossible.

If drain times out, leave the rollout in `DRAINING`, keep workers running, archive the status tables, and repair the
specific failed jobs. Re-enabling admission or force-changing universal tags is not a safe timeout response.

## Metrics and alerts

- Recovery/reconcile: `cache.manager.lease_recovery`, `cache.manager.reconcile.*`, and reconcile last start/success.
- Upload: `cache.manager.upload.run|scanned|scheduled|completed|failed`.
- Publish: `cache.manager.publish.result`, tagged only with origin and stable outcome.
- Staging cleanup: `cache.meta.staging_gc` records successfully committed GC queueing; ordinary Metadata GC success,
  failure, busy-session, chunk-count, and latency metrics show completion and retries.

Alert on failed/degraded upload runs, publish failures, reconcile conflicts/retryable work, stale last-success time,
Metadata GC failures, or sustained active upload backlog. Tags are bounded and never contain object keys or credentials.

## Publish completion semantics

For a write-through file, FUSE seals the staging inode and then waits for its durable upload job to reach a terminal
state. `fsync` and `release` use the same job ID, so retrying `fsync` after a timeout observes the original upload rather
than creating a second object. A `PUBLISHED` job is success, `FAILED` is reported as `EIO`, cancellation is reported as
`EINTR`, and expiration of `write_through.publish_timeout` is reported as `ETIMEDOUT`.

A timeout or disconnected client does not cancel the durable upload job. The cache manager continues processing it,
and an authorized client can query the job by ID after reconnecting. The polling interval is controlled by
`write_through.publish_poll_interval`.

Applications that require a reliably observable publish result must call `fsync` before closing the file. Although
FUSE `release` waits for publication and returns an error when the kernel accepts one, POSIX `close(2)` errors are not
reliably propagated by all kernels, runtimes, or application wrappers. Process exit is not a publish barrier; use
`fsync` or query the durable job explicitly before treating the object as published.

Job error details remain server-side operational data. Clients receive stable error categories and must not depend on
backend-specific object-store error text.

## Post-publish warming and cleanup

Publishing atomically places the unlinked staging inode on Metadata's ordinary GC queue. GC retains its chunks while
any file session is open, retries after transient cleanup failures or a Metadata restart, and removes the inode after
the last handle closes. Upload-job queries report the staging state as `WAITING_FOR_HANDLES`, `QUEUED`, or `COMPLETE`.
Repeated cleanup is idempotent.

Cache Manager submits a deterministic, high-priority namespace prefetch job after publication. The durable prefetch
record is its own checkpoint: a crash between publish and prefetch creation is safe, and restart repeats the same
idempotent request without republishing the origin file. Submission failure is retried, but it never rolls back a
published file or changes successful `fsync` results. Upload queries report `PENDING` or `SUBMITTED` warming state and
the deterministic prefetch job ID once it exists.

Before publication, staging data is retained until the upload reaches a terminal state. A completed object belonging
to a failed upload is retained for explicit operator inspection and repair; it is not deleted automatically. The
upload query exposes this as `RETAIN_ORPHAN_FOR_OPERATOR`, while successfully published jobs report
`DELETE_STAGING_AFTER_LAST_HANDLE`.
