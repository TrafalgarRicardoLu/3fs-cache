# Phase 4 write-through operations

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
