# 3FS Cache Phase 2 Capacity Management Design

Date: 2026-07-30

Status: Approved in conversation; revised after independent review

## 1. Purpose

Phase 2 turns the phase-1 read-cache path into an automatic physical-capacity loop:

```text
access collection
  -> second-miss admission
  -> physical-space gating
  -> policy-selected eviction
  -> generation-fenced deletion
  -> durable Storage Event
  -> idempotent Metadata convergence
```

The selected architecture is manager-centric: one Cache Manager makes normal admission and eviction decisions, while
Storage has a local safety trigger that reuses the same eviction and event protocol.

## 2. Scope

Phase 2 delivers:

- asynchronous Client access reports;
- bounded second-miss admission;
- physical-disk-based admission and watermarks;
- configurable eviction policies, with LRU as the first implementation;
- normal manager-triggered eviction;
- Storage-local safety eviction;
- a durable, at-least-once Storage Event outbox;
- idempotent Metadata event consumption;
- limited recovery of `EVICTING` work after Cache Manager restart;
- physical-capacity, admission, eviction, and event observability.

Phase 2 does not deliver:

- prefetch, pinning, job priority, or per-Origin quota;
- multiple active Cache Managers or manager leader election;
- full recovery of every `LOADING` task or orphan chunk;
- a bidirectional Metadata/Storage inventory reconciler;
- write caching or Origin write-back;
- a throughput or latency qualification target.

## 3. Deployment Assumption and Invariants

The design depends on one strong deployment constraint:

> A physical disk and every Storage Target on it are assigned one persistent role: `CACHE_ONLY` or `USER_DATA`. A
> `CACHE_ONLY` disk accepts only cache chains and never contains ordinary 3FS user data.

The role is persisted when a disk and its Targets are initialized and cannot change while data exists. Mgmtd rejects a
chain or routing update that mixes CACHE_DATA and USER_DATA Targets on one physical disk. Storage rejects an incompatible
routing update and checks the role on every ordinary and cache I/O entry point. Phase-2 enablement requires an inventory
scan proving that existing data matches the persisted role.

The system maintains these invariants:

1. Physical disk space is the authority for admission and eviction.
2. Metadata logical byte counters are informational and never reject admission.
3. Cache Manager space checks are preflight optimization. A block is admitted only after Storage atomically reserves its
   calculated physical footprint on every replica Target and disk.
4. Missing, stale, or incomplete routing or space information fails admission closed, without affecting foreground
   Origin reads.
5. Normal and Storage-local eviction share the policy abstraction, generation fence, `EVICTING` state, and event
   convergence protocol.
6. For EVICTING records, Metadata keeps the block's logical bytes until a durable logical `DELETED` event proves every
   replica recorded by that eviction operation retired the relevant generation. Phase-1 CLEANING retains its existing
   synchronous fenced completion ownership in Phase 2.
7. Access reports may be duplicated, delayed, reordered, or dropped without affecting correctness.
8. Storage Events are delivered at least once and Metadata consumes them idempotently.
9. Phase 2 runs one Cache Manager. Its in-memory admission window and physical reservations are not distributed state.
10. Cache deletion must never affect an ordinary 3FS chain or a newer cache generation.

## 4. Component Responsibilities

### 4.1 Client

- Record successful READY hits and successful Origin misses.
- Buffer and send access reports asynchronously.
- Drop reports under bounded-buffer pressure instead of blocking foreground reads.
- Preserve existing Origin fallback behavior for cache bypass, `EVICTING`, `NotFound`, and transient failures.

### 4.2 Cache Manager

- Aggregate access reports.
- Maintain the bounded second-miss admission window.
- Resolve chains to replica targets, nodes, and physical disks.
- Poll Storage-reported cache-allocatable space and maintain in-memory preflight reservations.
- Obtain durable Storage permits for every replica before creating new QUEUED work.
- Stop admission for chains that touch pressured or unknown disks.
- Select normal eviction candidates through an `EvictionPolicy`.
- Begin Metadata eviction and request generation-fenced chain retirement.
- Rediscover and resume `EVICTING` work after restart.

### 4.3 Metadata

- Remain authoritative for cache block state and generation.
- Store READY and last-access timestamps used by normal eviction.
- Atomically transition matching READY records to `EVICTING`.
- Keep logical reserved/committed/used-byte statistics for queries.
- Consume Storage Events and their acknowledgement cursors in one transaction.
- Remove a matching EVICTING record only after a logical `DELETED` event.

### 4.4 Storage

- Persist cache chunk identity, generation, and local access metadata.
- Expose stable physical-disk identity, cache physical usage, cache-allocatable bytes, and footprint estimates.
- Persist disk/Target role and enforce it on routing changes and every ordinary/cache I/O entry point.
- Atomically gate cache writes with per-disk durable, expiring permits.
- Perform generation-fenced deletion.
- Trigger local safety eviction when the physical disk reaches its safety high watermark.
- Persist delete intents before deletion and report completed events until acknowledged.

### 4.5 Mgmtd

- Provide Cache Manager with the chain-to-target-to-node routing relationship.
- Advertise cache protocol and schema compatibility.
- Preserve the distinction between cache and user-data chain tables.
- Reject create, attach, upload, and routing operations that place CACHE_DATA and USER_DATA Targets on the same disk.

Storage heartbeat includes each Target's persistent disk ID and role. Mgmtd persists this mapping and refuses a chain
mutation when any Target has an unknown role/disk or when the resulting physical disk would be mixed-role.

## 5. Access Reports and Admission

### 5.1 Wire model

Add a bounded Cache Manager request equivalent to:

```cpp
enum class CacheAccessType : uint8_t {
  READY_HIT,
  ORIGIN_MISS,
};

struct CacheAccessReportItem {
  CacheBlockKey key;
  CacheAccessType type;
  UtcTime clientAccessTime;
  std::optional<CacheGeneration> observedGeneration;
};

struct ReportCacheAccessReq {
  std::vector<CacheAccessReportItem> items;
  uint32_t cacheProtocolVersion;
};
```

`READY_HIT` requires an observed generation. `ORIGIN_MISS` does not. Client time is diagnostic; Cache Manager receive
time is authoritative for global LRU ordering because Client clocks are not trusted.

### 5.2 Client buffering

The Client flushes when either the batch threshold or flush interval is reached. The buffer is bounded. A full buffer
or failed asynchronous send increments a dropped-report metric and discards reports. It must not retry on the foreground
read path.

### 5.3 Second-miss admission

Cache Manager keeps an in-memory bounded map:

```cpp
struct AdmissionWindowEntry {
  UtcTime firstMissAt;
  UtcTime lastMissAt;
  uint32_t missCount;
};
```

The first miss records an entry and bypasses caching. A second miss inside `admission_window` proceeds to physical
capacity checking. Expired entries restart at the first miss. The map evicts expired or oldest entries when it reaches
`admission_max_entries`.

The existing foreground `EnsureCached` call becomes the only miss signal in Phase 2 and is changed to
`reportMissAndMaybeAdmit`: the first call only updates this window; the second may admit. Client must not additionally
send `ORIGIN_MISS` through `ReportCacheAccess`, preventing double counting. `ReportCacheAccess` carries READY hits; its
ORIGIN_MISS enum value is reserved for a future replacement of `EnsureCached`.

Each admission attempt has a stable `admissionAttemptId`. Metadata enqueue stores its complete PermitIdentity and
returns whether the call created work or found READY, QUEUED, or LOADING.

- READY releases the newly prepared permit immediately.
- Newly created QUEUED transfers its permit to the scheduler.
- QUEUED with a valid permit is attached and renewed. QUEUED with an invalid permit prepares a new-epoch permit and uses
  Metadata CAS to replace the PermitIdentity; a loser releases its new permit.
- LOADING with an executing write retains the original permit and cannot be attached or replaced. LOADING that has not
  started replace may replace an invalid permit only after Storage proves no write is executing and Metadata CAS matches
  the loader lease and old permit generation.

An ambiguous enqueue timeout is retried with the same attempt ID. Scheduler attachment failure calls a fenced
`CancelQueuedAdmission` that atomically changes that exact QUEUED attempt to FAILED/NONE and releases its logical
reservation, then releases the Storage permit. If cancellation is ambiguous, restart recovery resolves the persisted
QUEUED/PermitIdentity pair; it must never leave an ordinary QUEUED record that cannot acquire a permit.

READY hits do not trigger admission. Prefetch and pin exceptions are deferred to Phase 3.

## 6. Physical Capacity Model

### 6.1 Stable disk identity

Extend Storage space information with a stable disk UUID persisted when the Storage disk is initialized:

```cpp
struct SpaceInfo {
  PhysicalDiskId diskId;
  StorageRole role;
  uint64_t cacheCapacityBytes;
  uint64_t cachePhysicalUsedBytes;
  uint64_t cacheAllocatableBytes;
  uint64_t cacheReservedBytes;
  double enforcedAdmissionHighWatermark;
  std::vector<TargetId> targetIds;
  UtcTime sampledAt;
  // Existing diagnostic fields may remain.
};
```

The path is diagnostic and must not be used as persistent identity. `cachePhysicalUsedBytes` and
`cacheAllocatableBytes` are calculated by Storage from the real ChunkEngine/ChunkStore allocation model, including
preallocation, reserved regions, recyclable-but-not-yet-recycled chunks, and filesystem availability. Cache Manager must
not reconstruct them from raw filesystem `available`.

### 6.2 Manager snapshot

Cache Manager combines RoutingInfo and `querySpaceInfo` into the in-memory mapping:

```text
ChainId -> replica TargetId -> NodeId -> PhysicalDiskId -> SpaceSnapshot
```

A snapshot is usable only when every serving replica resolves to one fresh CACHE_ONLY disk entry. Missing targets,
incomplete routing, failed queries, and responses whose Manager request/receive monotonic age exceeds
`space_snapshot_max_age` make the affected chain inadmissible. Remote `sampledAt` is diagnostic only.

### 6.3 Physical footprint

Storage exposes or computes `physicalFootprint(target, chunkSize, payloadLength)` using the actual engine allocation
rules. A short final block may therefore reserve a full allocation unit. If two replicas of one chain are on different
Targets sharing a disk, their footprints are added twice on that disk.

Cache Manager groups replica footprints by disk and uses this only for preflight:

```text
projected_used[disk] =
  cachePhysicalUsedBytes
  + cacheReservedBytes
  + managerPreflightReservedBytes
  + sum(replicaFootprint on disk)

projected_ratio[disk] = projected_used[disk] / cacheCapacityBytes
```

The projected post-write usage must stay below the normal high watermark on every disk.

### 6.4 Storage physical permits

The immutable placement and replace authorization are:

```cpp
struct PlacementIdentity {
  VersionedChainId versionedChain;
  std::vector<TargetId> expectedReplicaTargets;  // sorted and unique
  Uuid admissionAttemptId;
};

struct PermitIdentity {
  Uuid managerEpoch;
  PlacementIdentity placement;
  uint64_t permitGeneration;
  std::map<TargetId, uint64_t> footprintByTarget;
};
```

The final authority is a Storage-side per-disk `CacheSpaceGate`. Before Metadata enqueue, Cache Manager submits a stable
`managerEpoch` and `admissionAttemptId` to a chain-level prepare operation. The coordinator requests one permit per
replica Target; each Target atomically checks its CACHE_ONLY role and current allocatable space, reserves the calculated
footprint under its disk gate, and persists an expiring permit.

The gate enforces the Storage-configured normal high watermark, not a caller-supplied value. SpaceInfo advertises that
value and Cache Manager fails the disk closed if it differs from `capacity_high_watermark`.

The prepare succeeds only after every replica permit succeeds. Partial success is released with the same attempt ID.
Cache replace must carry the permit identity; Storage rejects missing, expired, wrong-epoch, wrong-target, or
wrong-footprint permits. A permit remains counted while a write is executing, is consumed on commit, and is released on
failure or cancellation.

While newly created work remains QUEUED or LOADING, Cache Manager renews the permit with the same attempt identity. If
renewal fails or the permit expires before replace begins, it follows the CAS replacement rules in Section 5.3. If safe
replacement cannot be proven, the attempt is cancelled or failed through the fenced cleanup path. It may never write
without a current permit persisted on the same Metadata attempt.

Metadata persists PermitIdentity in QUEUED and LOADING. Replacing it requires CAS on block state, load lease when
present, and old permit generation. Storage's prepare and release calls are idempotent by PermitIdentity. An executing
replace pins its permit so expiry cannot free bytes underneath the write.

Manager preflight reservations prevent avoidable RPCs but are not a correctness mechanism. After Manager restart, the
new epoch stops admission until Storage space responses include all active permits from prior epochs and any executing
writes. Expired permits fence new replace calls; an already executing write remains reserved until it commits or aborts.
The new Manager may resume only after this inventory is complete, not merely after clearing local memory.

### 6.5 Logical counters

The existing Metadata `reservedBytes`, `committedBytes`, and `usedBytes` remain useful for answering how much logical
data is queued and READY. `CacheCapacityStore::reserve()` must stop rejecting a request based on `logicalCapacity`; it
only updates counters with overflow and consistency checks. `CacheCapacityRecord::valid()` likewise removes
`usedBytes <= logicalCapacity`, and setting the reference value may not fail because current usage is larger.

The configured logical capacity becomes a deprecated informational/reference field. It must not be presented as the
remaining admission capacity.

## 7. Access State in Metadata

Extend `CacheBlockRecord` with:

```cpp
UtcTime readyAt;
UtcTime lastAccessAt;
std::optional<PermitIdentity> permit;       // QUEUED or LOADING
std::optional<PlacementIdentity> placement; // READY and later physical states
```

Replace and Commit carry the same PlacementIdentity. Commit validates it against the LOADING PermitIdentity, clears the
permit, persists the immutable placement on READY, and initializes both time fields. Descriptor creation persists that
same complete placement on every replica. RoutingInfo changes do not rewrite READY placement. A placement may change
only through a future protocol that proves migration of the generation on every old and new replica; Phase 2 has no such
protocol and therefore fails eviction closed if the recorded placement cannot be reached.

Cache Manager periodically sends coalesced access updates containing block key, observed generation, and Manager receive
time. Metadata applies:

```text
record.state == READY
and record.cacheGeneration == observedGeneration
=> lastAccessAt = max(lastAccessAt, receivedAt)
```

Reports for old generations or non-READY states are ignored. A READY block with no later hit uses `readyAt` for policy
input.

## 8. Pluggable Eviction Policy

LRU is a configured implementation, not controller behavior.

### 8.1 Manager policy

```cpp
struct EvictionCandidate {
  CacheBlockKey key;
  ReadyIdentity ready;
  ChainId chainId;
  uint64_t logicalBytes;
  UtcTime readyAt;
  UtcTime lastAccessAt;
  std::map<PhysicalDiskId, uint64_t> physicalFootprintByDisk;
};

struct EvictionContext {
  std::map<PhysicalDiskId, uint64_t> bytesToReleaseByDisk;
  UtcTime now;
  Duration protectionPeriod;
};

class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;
  virtual Result<std::vector<size_t>> select(
      std::span<const EvictionCandidate> candidates,
      const EvictionContext &context) = 0;
};
```

Policies return indexes, not mutable candidate copies. Before any state mutation, the controller verifies that every
index is in range and unique, re-reads the selected READY identity, enforces the hard protection period and batch limit,
and recomputes the footprint released on every pressured disk. Invalid output fails the whole policy invocation without
calling BeginEvict. A policy may return fewer bytes than requested, but it may not redirect deletion outside its input.

The first registered implementation is `LRUEvictionPolicy`. It sorts effective access time oldest first and selects a
set that addresses the per-disk deficits.

### 8.2 Storage-local policy

Storage has a parallel local policy interface over cache chunk descriptors. It is independently configurable so future
local algorithms need not have the same inputs as global algorithms. The first implementation is local LRU.

Controllers select a policy through a validated factory. Unknown policy names fail startup. Controller tests use a fake
policy to prove execution does not contain LRU-specific ordering.

## 9. Eviction State

Add a dedicated Metadata mutation equivalent to:

```cpp
struct BeginEvictCacheBlockItem {
  CacheBlockKey key;
  ReadyIdentity expectedReady;
  EvictionReason reason;
};
```

It atomically performs:

```text
READY with matching ReadyIdentity -> EVICTING
```

BeginEvict captures the exact versioned chain and atomically allocates and returns:

```cpp
ReadyIdentity expectedReady;
VersionedChainId versionedChain;
std::vector<TargetId> expectedReplicaTargets;
EvictionEpoch evictionEpoch;
Uuid retireOperationId;
EvictionReason reason;
```

BeginEvict copies versioned chain and replica set only from the immutable READY PlacementIdentity; it must never rebuild
them from current RoutingInfo. For local safety, EMERGENCY_EVICTED carries the descriptor's complete placement. Metadata
requires it to equal the READY placement, then generates a new eviction epoch and logical retire operation ID. The local
delete operation ID remains only the Storage event identity and cannot be reused as the logical retire operation ID.

All fields persist on the EVICTING record. Eviction epoch is monotonic per block; overflow fails closed. Repeating
BeginEvict with the same expected identity returns the existing operation, while a different identity conflicts.
`EVICTING` retains the READY identity and committed logical bytes. New read plans do not advertise EVICTING as a hit.
Old plans may race with deletion; `NotFound` falls back to the Origin.

Normal capacity eviction does not reuse `CLEANING`. `CLEANING` remains the invalidation/refresh cleanup workflow, while
`EVICTING` represents capacity-policy removal and Storage Event completion.

BeginEvict and BeginClean are mutually exclusive CAS transitions from READY. BeginClean against EVICTING and BeginEvict
against CLEANING return state conflict. The operation that wins owns completion and logical-counter release.

## 10. Storage Cache Descriptor and Local LRU

Extend cache replace requests with the neutral logical `CacheBlockKey`. Storage persists the following beside cache
chunk metadata:

```cpp
struct CacheChunkDescriptor {
  CacheBlockKey logicalKey;
  CacheGeneration generation;
  PlacementIdentity placement;
  TargetId targetId;
  UtcTime createdAt;
  UtcTime lastAccessAt;
};
```

Successful cache reads update local `lastAccessAt`. Updates may be coalesced so one block is persisted at most once per
`local_access_persist_interval`. Updates are monotonic under clock rollback:

```text
lastAccessAt = max(previousLastAccessAt, now)
```

The explicit logical key lets local safety eviction generate a Metadata-addressable event without coupling Storage to
Metadata's packed ChunkId representation.

Phase 2 does not attempt to infer descriptors for Phase-1 chunks. Enablement requires draining all Phase-1 cache data
with the existing fenced cleanup path, verifying Metadata has no nonterminal cache records or logical bytes, and querying
Storage to prove no ACTIVE cache generation remains. Only then may descriptor-required writes and local safety eviction
be enabled.

## 11. Normal Eviction Flow

When a physical disk reaches `capacity_high_watermark`, Cache Manager:

1. Stops new admission for chains touching that disk.
2. Calculates bytes required to return the disk to `capacity_low_watermark`.
3. Scans Metadata READY/COMMITTED blocks in bounded pages.
4. Resolves their chains and keeps blocks occupying a pressured disk.
5. Passes candidates and the release target to the configured `EvictionPolicy`.
6. Calls `BeginEvictCacheBlocks` for selected blocks.
7. Skips state-conflicting candidates and continues the batch.
8. Sends the persisted eviction identity to the new coordinated chain-retire RPC.
9. Waits for the durable logical `DELETED` event to finalize Metadata.
10. Repeats using fresh space snapshots until usage is below the low watermark.

A synchronous retire response reports the persisted RetireOperation state. It does not directly remove Metadata state;
only the durable event does. The existing Cache Manager helper that independently sends retire RPCs to every Target is
not a valid implementation of this step and remains only for the Phase-1 CLEANING owner until that path is migrated.

## 12. Storage-Local Safety Flow

Normal and local safety eviction are one mechanism with two trigger entries. When a disk reaches
`local_safety_high_watermark`, Storage:

1. Enumerates only cache descriptors on that disk.
2. Excludes chunks being written or without a stable active generation.
3. Calls the configured local eviction policy.
4. Prepares the durable event intent.
5. Generation-fenced deletes the selected local replica.
6. Marks `EMERGENCY_EVICTED` deliverable.
7. Continues until the disk is below `local_safety_low_watermark`.

Both normal and local ratios use Storage's cache capacity basis and physical footprint counters, not raw filesystem
usage reconstructed by Cache Manager.

Metadata consumes the event as follows:

- matching READY becomes EVICTING;
- matching EVICTING is an idempotent no-op;
- a stale generation is acknowledged without modifying the current record.

Cache Manager periodically lists or claims EVICTING records and retires the rest of the chain. The eventual chain-level
`DELETED` event removes Metadata state.

## 13. Durable Storage Event Protocol

### 13.1 Event model

```cpp
enum class CacheStorageEventType : uint8_t {
  DELETED,
  EMERGENCY_EVICTED,
  LOST,       // reserved for later use
  CORRUPTED,  // reserved for later use
};

struct CacheStorageEvent {
  Uuid sourceId;
  uint64_t sequence;
  CacheStorageEventType type;
  Uuid storageOperationId;
  std::optional<Uuid> logicalRetireOperationId;
  CacheBlockKey logicalKey;
  CacheChunkKey storageKey;
  CacheGeneration generation;
  PlacementIdentity placement;
  std::optional<EvictionEpoch> evictionEpoch;
  PhysicalDiskId diskId;
  UtcTime timestamp;
};
```

`sourceId` is persisted by the Storage event journal. Sequence numbers are monotonic within a source.

### 13.2 Delete outbox

Every Phase-2 event-producing deletion uses an operation journal plus a separate delivery outbox:

```text
persist PREPARED intent
  -> execute generation-fenced delete
  -> verify the generation is retired
  -> atomically allocate the next delivery sequence and create DELIVERABLE
  -> report until Metadata ACKs
  -> reclaim the journal record
```

PREPARED operations do not own a delivery sequence. The Storage operation ID is stable across retries. On Storage
restart, PREPARED records re-run or query the fenced deletion; DELIVERABLE records resume reporting. A permanently stuck
PREPARED operation therefore cannot create a sequence gap or block later completed operations from delivery.

This protocol does not change Phase-1 CLEANING retirement, which produces no Phase-2 logical DELETED event.

The outbox must use reserved metadata space or another bounded mechanism that remains writable at the local safety
watermark. If the outbox cannot prepare an intent, Storage must not start a new cache deletion. It stops new cache
writes and raises a critical alert rather than silently deleting data without an event.

### 13.3 Coordinated replicated retirement

Phase 2 adds one chain-level RPC and replaces the current per-Target fan-out helper for EVICTING work. The preferred
Target of the recorded chain version is the deterministic coordinator. Before any replica deletion, it persists:

```cpp
struct RetireOperation {
  Uuid operationId;
  CacheBlockKey logicalKey;
  ReadyIdentity expectedReady;
  PlacementIdentity placement;
  EvictionEpoch evictionEpoch;
  std::set<TargetId> retiredReplicaTargets;
  RetireOperationState state;  // PREPARED or DELIVERABLE
};
```

The expected replica set is copied from the exact recorded RoutingInfo version and never silently replaced by a newer
route. The coordinator sends the stable operation ID and generation to each recorded Target. A Target counts as retired
only after its tombstone is durable and a query proves that generation cannot be read or replaced. Partial success stays
PREPARED and is retried. Coordinator restart reloads the operation and its per-replica progress.

Only after every expected Target is durably retired may the coordinator, in one local transaction, allocate the next
contiguous source sequence, create the logical DELETED event in the delivery outbox, and mark the RetireOperation
DELIVERABLE. Local emergency intents allocate their sequence at the same transition. No synchronous response, quorum,
preferred replica, or single local delete substitutes for this condition. An independently deleted local replica
produces EMERGENCY_EVICTED, never DELETED.

Phase-1 CLEANING deliberately retains its current owner in Phase 2: `CacheCleanupWorker` fans out fenced retires, verifies
the synchronous per-Target results, and calls FinishClean. Those retires do not create a logical DELETED operation.
Local safety may still delete a CLEANING replica and emit EMERGENCY_EVICTED; Metadata ACKs it without taking completion
ownership from the cleanup worker.

### 13.4 Reporting and ACK

Storage sends contiguous event batches to a Metadata internal RPC. Metadata handles each source in sequence order and,
in one transaction:

1. validates the expected next sequence;
2. validates block identity and generation;
3. applies or safely ignores the state change;
4. advances the persisted acknowledged sequence.

It returns an ACK only after the transaction commits. Duplicate events return the persisted ACK. A gap does not advance
the cursor. Storage retries from the first unacknowledged sequence.

For DELETED, Metadata must match block key, generation, complete PlacementIdentity, eviction epoch, and retire operation
ID before releasing logical counters. Semantic errors never block later events from the same source: they are persisted
to a dead-letter record, alerted, and ACKed without an unsafe state mutation. Only a transport-level sequence gap
prevents cursor advancement.

The state/event matrix for a matching generation is:

| Metadata state | DELETED | EMERGENCY_EVICTED |
| --- | --- | --- |
| READY | Unexpected: enter CLEANING and enqueue verification; ACK | Atomically allocate eviction identity and enter EVICTING; ACK |
| EVICTING | Exact operation match: remove and decrement once; mismatch: dead-letter; ACK | No-op; ACK |
| CLEANING | No-op because cleanup worker owns completion; ACK | No-op because cleanup worker owns completion; ACK |
| LOADING | Enter existing fenced failure/cleanup workflow; ACK | Enter existing fenced failure/cleanup workflow; ACK |
| QUEUED | No physical generation expected; dead-letter; ACK | No physical generation expected; dead-letter; ACK |
| INVALID | Existing cleanup owns completion; ACK | Existing cleanup owns completion; ACK |
| FAILED or NONE | Stale no-op; ACK | Stale no-op; ACK |

An event generation older than the record is a stale no-op and is ACKed. A generation newer than the record is
dead-lettered and ACKed without modifying the newer/unknown state. This favors bounded event progress while preserving
the inconsistent record for explicit verification or the Phase-4 Reconciler.

## 14. Failure Behavior

### 14.1 Cache Manager restart

On restart Cache Manager allocates a new persistent-process epoch, discards admission and preflight memory,
force-refreshes RoutingInfo, and queries every related Storage disk for old-epoch permits and executing writes. It keeps
admission stopped until all are included in cacheReservedBytes or have durably completed/expired. It then scans
QUEUED records to renew, CAS-replace, or cancel their persisted permits; it never replaces a permit for an executing
LOADING write. It also scans EVICTING records in stable key order and resumes their exact persisted RetireOperation. Scan
cursors are ephemeral and may restart from the beginning because one Cache Manager and stable identities make replay
idempotent; no distributed claim lease is required. Access history for second-miss admission starts empty.

This is deliberately limited recovery. Full LOADING lease recovery, orphan inventory cleanup, and cross-version repair
remain Phase 4 work.

### 14.2 Storage and network failures

- Retire timeout leaves EVICTING and is retried with bounded backoff.
- Permit-prepare timeout is retried with the same admission attempt ID; partial replica permits are not forgotten.
- Event report failure retains DELIVERABLE records and retries.
- Metadata outage does not discard events. Storage may continue local safety deletion only while it can prepare outbox
  entries.
- Partial replica failure cannot produce logical DELETED, so logical state remains conservative.
- Storage no-space during load fails the cache load and uses the existing fenced cleanup path.

### 14.3 Routing changes

READY, Eviction work, RetireOperation, descriptor, and Events carry the same immutable PlacementIdentity from the actual
write. A route change must not be interpreted as proof that the old generation disappeared, and current RoutingInfo may
not replace this identity. Cache Manager resumes the persisted coordinator operation against the recorded replica set.
If a recorded replica or coordinator cannot be reached, the block remains EVICTING and its logical bytes remain counted.
Cross-version inventory repair is deferred to Phase 4.

### 14.4 Resource exhaustion

Client reports, admission history, access aggregation, policy candidates, and RPC batches are bounded and may degrade by
dropping approximate information. Storage Events may never be silently dropped. Event journal exhaustion stops new cache
deletion and cache writes and emits a critical alert.

## 15. Configuration

Representative Cache Manager configuration:

```toml
admission_policy = "second_miss"
admission_window = "5m"
admission_max_entries = 100000

eviction_policy = "lru"
capacity_high_watermark = 0.90
capacity_low_watermark = 0.80
space_poll_interval = "5s"
space_snapshot_max_age = "15s"
storage_permit_ttl = "60s"
eviction_protection_period = "10m"
eviction_batch_size = 256
```

Representative Storage configuration:

```toml
local_eviction_policy = "lru"
cache_write_high_watermark = 0.90
local_safety_high_watermark = 0.95
local_safety_low_watermark = 0.90
local_access_persist_interval = "30s"

cache_event_batch_size = 256
cache_event_report_interval = "1s"
cache_event_journal_max_bytes = "1GiB"
cache_space_permit_max_bytes = "4GiB"
```

Validation requires:

```text
0 < capacity_low < capacity_high < 1
0 < local_safety_low < local_safety_high < 1
capacity_high < local_safety_high
positive intervals, limits, and batch sizes
registered policy names
Manager capacity_high == every Storage enforced cache_write_high
```

Configuration changes may require restart in Phase 2. Hot-reload semantics are not required.

## 16. Observability

Cache status must expose:

- physical disk identity and role, cache capacity basis, physical used, allocatable, reserved bytes, and snapshot age;
- Node, Target, and cache-chain associations;
- normal and local safety watermarks;
- admission-paused state and reason;
- per-disk Manager preflight reservation and Storage durable permit bytes by Manager epoch;
- admission-window size and decision counts;
- READY, EVICTING, and logical cached bytes;
- current global and local policy names;
- event backlog, oldest event age, and ACK position.

Core metrics include:

```text
cache.access_report.received/dropped/stale
cache.admission.first_miss/accepted/bypassed
cache.admission.bypass_by_reason
cache.physical_space.usage_ratio/snapshot_age
cache.physical_reservation.bytes
cache.storage_permit.active/expired/rejected
cache.eviction.triggered/selected/completed/retried
cache.eviction.bytes_selected/bytes_completed
cache.eviction.policy_errors
cache.storage_event.prepared/deliverable/acked/retried
cache.storage_event.backlog/oldest_age
cache.local_safety.active
```

State-transition logs include block key, generation, versioned chain, disk ID, eviction reason, policy name, event source,
and sequence. They must not include tokens or Origin credentials.

## 17. Verification

### 17.1 Unit tests

- Client report threshold, periodic flush, bounded dropping, and foreground non-blocking behavior.
- First miss, second miss, expiry, and bounded admission history.
- Replica-disk resolution, same-disk replica footprint accumulation, short-block allocation units, preallocated engines,
  stale snapshots measured by Manager monotonic time, and per-disk preflight reservation.
- Storage gate atomicity, partial multi-replica permit rollback, permit consumption/expiry, old Manager epoch, and an
  executing write that crosses Manager restart.
- QUEUED permit renew/CAS replacement/cancellation, scheduler failure after enqueue, expiry before scheduling, and two
  recovery attempts racing to replace one permit.
- Persistent CACHE_ONLY/USER_DATA roles; old data mismatch; routing-role change; same-disk mixed Target rejection; and
  ordinary write/truncate/remove attempts against cache Targets.
- LRU selection, protection period, per-disk deficits, factory validation, and fake/malicious policy indexes that are
  duplicated, out of range, or insufficient.
- READY access generation CAS and READY-to-EVICTING state transitions.
- Logical counters update correctly without enforcing logical capacity.
- Storage descriptor persistence and coalesced local access updates.
- READY placement persistence across route changes and exact equality among permit, replace, descriptor, Commit, and
  EVICTING identity.
- Event sequence allocation only at DELIVERABLE, contiguous ACK, compaction, restart recovery, and journal bound. A
  permanently PREPARED operation A must not prevent completed operations B and C from receiving consecutive sequences.
- Duplicate, delayed, stale-generation, future-generation, DELETED, and EMERGENCY_EVICTED consumption across every row
  of the READY/EVICTING/CLEANING/LOADING/QUEUED/INVALID/FAILED/NONE matrix.
- All configuration relations and boundary values.

### 17.2 Integration tests

1. The existing CacheReadPipeline and EnsureCached path leave Metadata NONE after the first Origin miss; the second miss
   within the window reaches READY.
2. One pressured replica disk stops only affected-chain admission.
3. Normal eviction invokes the configured policy and returns the disk to the low watermark.
4. EVICTING is no longer returned as a cache hit.
5. Retire success with delayed Event keeps Metadata EVICTING.
6. DELETED Event removes the record and decrements logical bytes.
7. Local safety eviction emits EMERGENCY_EVICTED and converges through EVICTING to no record.
8. Cache Manager restart resumes EVICTING work.
9. Duplicate, delayed, and network-interrupted Events eventually converge once.
10. Multiple replicas on different Targets sharing one disk reserve multiple footprints without multiplying disk
    capacity.
11. Cache Targets and their physical disks reject ordinary user-data chains and every ordinary I/O entry point.
12. BeginEvict versus BeginClean and local safety versus CLEANING preserve one completion owner and ACK all events.
13. Existing Phase-1 READY data is drained and verified empty before Phase 2 can enable.
14. After a READY block's route changes, eviction uses its original placement; while any original replica remains ACTIVE,
    no DELETED event or logical release is possible.
15. MinIO Origin miss, admission, hit, eviction, and later Origin fallback form a complete loop.

### 17.3 Crash injection

Inject a crash after each of these points:

- each replica permit before and after durable reservation;
- Manager crash after permit prepare, after enqueue before schedule, during replace, and before/after Metadata enqueue
  result resolution;
- coordinator RetireOperation PREPARED, before the first replica retire;
- before and after every replica's durable tombstone acknowledgement;
- coordinator crash after the last replica retires but before logical DELIVERABLE;
- PREPARED persisted, before delete;
- delete completed, before DELIVERABLE;
- DELIVERABLE persisted, before report;
- before Metadata event transaction commit;
- after commit, before ACK response;
- after ACK response, before Storage journal reclamation.

Every case must preserve generation fencing, retain all required events, avoid premature or duplicate logical-counter
release, and eventually converge. The suite explicitly asserts that an outbox PREPARED failure leaves the physical chunk
unchanged, Metadata cannot release while any recorded replica remains ACTIVE, and after logical release every recorded
replica is generation-retired.

Performance is not a Phase-2 acceptance gate. Tests still verify that access reporting, admission, and eviction do not
block foreground Origin fallback.

## 18. Upgrade and Rollback

Bump the cache protocol and schema capability for Phase 2. Upgrade in this order:

```text
Storage -> Metadata -> Cache Manager -> Client -> enable Phase 2
```

New behavior is disabled until all components advertise support. Rolling wire compatibility is fail-closed: a Phase-1
Manager cannot create new cache data once the drain begins, and a Phase-2 descriptor/permit write is rejected until the
cluster enable flag is committed.

Before enabling:

1. Stop Phase-1 admission.
2. Drain every Phase-1 cache record through fenced CLEANING.
3. Verify Metadata has no nonterminal cache record and zero logical bytes.
4. Query Storage and verify no ACTIVE cache generation remains.
5. Persist and inventory-check CACHE_ONLY disk/Target roles.
6. Verify stable disk IDs, permits, event journals, routing, and watermarks.
7. Enable Phase 2 and only then resume admission.

Rollback also drains Phase-2 cache data: stop new admission, finish or cancel durable permits, finish EVICTING work, wait
for DELIVERABLE events to drain, CLEAN all remaining READY records, verify Storage has no ACTIVE generation, and only then
disable Phase 2. Do not expose descriptor-bearing data to Phase-1 binaries.

## 19. Acceptance Criteria

Phase 2 is complete when:

- Storage-calculated physical footprint and atomic per-disk permits, not logical capacity or Manager snapshots, control
  admission;
- CACHE_ONLY roles are persisted and enforced by Mgmtd, Storage routing, and every I/O entry point;
- READY persists the exact write placement and eviction never substitutes current routing;
- normal and local high/low watermarks are configurable and provide hysteresis;
- eviction policy selection is pluggable and LRU is the default implementation;
- normal and local safety triggers converge through one fenced delete and event protocol;
- logical DELETED is impossible until every replica recorded by the exact RetireOperation is durably retired;
- stuck PREPARED operations do not consume event sequence numbers or block completed-event delivery;
- Storage Events survive the defined crash windows and are delivered at least once;
- Metadata applies every logical deletion exactly once;
- Cache Manager restart resumes EVICTING work;
- foreground reads retain safe Origin fallback under all capacity and event failures;
- Phase-1 cache, Metadata, Storage, analytics, and MinIO acceptance tests do not regress.
