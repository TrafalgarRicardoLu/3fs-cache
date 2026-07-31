# Cache Phase 2 acceptance

Phase 2 is releasable only when the following matrix passes with foreground
reads retaining safe origin fallback and never returning partial cache data.

## Functional and capacity matrix

- MinIO end to end: first miss bypasses, second miss admits, READY hits,
  access heat is reported, normal eviction converges to NONE, and the next read
  refetches origin data.
- Physical accounting: short blocks, engine allocation units, two replicas on
  one disk, concurrent writes, and Manager crash with an inflight write.
- Watermarks: normal and local-safety high/low hysteresis, independent physical
  disks, and chain isolation.
- Policy boundary: default LRU, fake policy, invalid/malicious output, and
  unknown configuration rejection.
- Placement: route change after READY; Metadata cannot release while any
  replica recorded in READY placement remains ACTIVE.
- Permits: prepare, rollback, renew, expiry, consume, enqueue-before-schedule
  crash, and recovery CAS races.
- Retirement: crash before/after each durable replica ACK, coordinator restart,
  partial timeout, and idempotent replay.
- Events: crash at PREPARED, physical deletion, DELIVERABLE, report, Metadata
  commit, ACK, and reclaim boundaries. A permanently failing operation must not
  prevent completed later operations from reporting.
- State races: duplicate and reordered events against READY, EVICTING,
  CLEANING, LOADING, FAILED, and NONE.
- Restart: queued permit and EVICTING recovery. Arbitrary LOADING recovery is
  explicitly outside the guarantee.
- Dedicated disks: ordinary write, truncate, remove, and mixed routing are
  rejected on `CACHE_ONLY` targets.

## Required invariants

1. If event PREPARE fails, the physical chunk is unchanged.
2. If any recorded replica is ACTIVE, Metadata and logical statistics remain.
3. Logical release implies every recorded replica has a durable generation
   tombstone.
4. Duplicate events never release statistics twice.
5. Storage capacity admission uses physical `used + reserved`; logical bytes
   are query-only.
6. CLEANING events are ACKed but cannot take EVICTING completion ownership.

## Regression and evidence

Run focused cache-manager, Metadata cache, Storage cache, client cache, and
mgmtd tests, then the complete CTest suite and Rust workspace tests. Run the
MinIO qualification scenario with injected component restarts. Archive test
commands, versions, routing inventory, phase-two status before/after, event
backlog/dead-letter counts, and the result of each invariant above.
