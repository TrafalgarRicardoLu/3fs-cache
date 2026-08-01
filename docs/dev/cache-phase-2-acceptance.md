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

## Automated evidence map

The acceptance cases are implemented by the following focused suites; these
names are stable evidence labels rather than a replacement for the full CTest
run:

- admission, permit crash/recovery, foreground fallback, and Manager restart:
  `Phase2EnsureCachedTest`, `TestPermitRecovery`, `TestCacheLoader`, and
  `TestEvictingWorker`;
- physical accounting, chain isolation, watermarks, policies, malicious
  selections, and route changes: `TestPhysicalCapacity`,
  `TestEvictionController`, `TestEvictionPolicy`, `TestEvictionCandidates`,
  `TestLocalEvictionPolicy`, and `TestLocalSafetyEvictor`;
- durable replica retirement and crash replay: `TestReplicaRetireOperation`
  and `TestRetireCoordinator`;
- PREPARED through ACK/reclaim crash windows and non-blocking event ordering:
  `TestCacheEventJournal`;
- duplicate/reordered event state matrix and exactly-once logical release:
  `TestCacheStorageEvents` and `TestCacheStateMachine`;
- rollout, half-upgrade, retry, and rollback gates: `CachePhase2Rollout` and
  `CacheChainTable`;
- MinIO miss, second-miss admission, warm hit, access aggregation, LRU
  eviction, and origin fallback:
  `MinIOIntegration.ColdFillWarmMixedRefreshCapacityAndCleanup`.

`cache-phase2-rollout enable` additionally requires live inventory evidence:
zero Metadata records, zero Storage ACTIVE generations, zero EVICTING/event
backlog/dead letters, fresh CACHE_ONLY disk snapshots, and matching component
capabilities. This prevents a unit-test-only qualification from enabling a
real cluster.

## Local qualification evidence

The 2026-08-01 clean `RelWithDebInfo` qualification used Clang 14, AWS SDK C++
1.10.55, and MinIO `RELEASE.2025-09-07T16-13-09Z`. The following final runs
passed:

- registered CTest targets `test_cache`, `test_cache_minio`,
  `test_cache_manager`, and `test_admin_cli`: 4/4;
- cache protocol, metrics, and serde contracts: 29/29;
- Manager admission, permits, physical capacity, and eviction: 68/68;
- mgmtd capability and rollout transitions: 7/7;
- Metadata cache capacity, state, event, and read-plan cases: 31/31;
- Storage event journal, local access/LRU, local safety, and retirement cases:
  23/23;
- real MinIO boundary and cold-fill/warm-hit/eviction/fallback scenarios: 2/2;
- `cargo build --release` and Clang 14 format validation.

The complete repository CTest registration also contains services that need
additional executables, FoundationDB/RDMA/io_uring setup, or external AWS S3
credentials. Those environment-specific suites are not replaced by this local
qualification and remain required in the deployment CI environment.
