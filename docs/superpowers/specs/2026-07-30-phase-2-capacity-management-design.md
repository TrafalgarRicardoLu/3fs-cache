# 3FS Cache Phase 2 Capacity Management Design

Date: 2026-07-30

Status: Approved in conversation; pending written-spec review

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

The design depends on one strong deployment assumption:

> A cache Storage Target is dedicated to cache data and accepts only cache chains. It never contains ordinary 3FS
> user data.

This must be validated at configuration and runtime boundaries, not treated only as an operator convention.

The system maintains these invariants:

1. Physical disk space is the authority for admission and eviction.
2. Metadata logical byte counters are informational and never reject admission.
3. A block is admitted only when every replica disk has sufficient projected space below the normal high watermark.
4. Missing, stale, or incomplete routing or space information fails admission closed, without affecting foreground
   Origin reads.
5. Normal and Storage-local eviction share the policy abstraction, generation fence, `EVICTING` state, and event
   convergence protocol.
6. Metadata keeps the block's logical bytes until deletion of the relevant generation is confirmed by a durable
   logical `DELETED` event.
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
- Poll physical space and maintain in-memory per-disk inflight reservations.
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
- Expose stable physical-disk identity and current space information.
- Enforce that cache Targets contain only cache chains.
- Perform generation-fenced deletion.
- Trigger local safety eviction when the physical disk reaches its safety high watermark.
- Persist delete intents before deletion and report completed events until acknowledged.

### 4.5 Mgmtd

- Provide Cache Manager with the chain-to-target-to-node routing relationship.
- Advertise cache protocol and schema compatibility.
- Preserve the distinction between cache and user-data chain tables.

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

READY hits do not trigger admission. Prefetch and pin exceptions are deferred to Phase 3.

## 6. Physical Capacity Model

### 6.1 Stable disk identity

Extend Storage space information with a stable disk UUID persisted when the Storage disk is initialized:

```cpp
struct SpaceInfo {
  PhysicalDiskId diskId;
  uint64_t capacity;
  uint64_t free;
  uint64_t available;
  std::vector<TargetId> targetIds;
  UtcTime sampledAt;
  // Existing diagnostic fields may remain.
};
```

The path is diagnostic and must not be used as persistent identity. Multiple Targets on the same disk map to one
`PhysicalDiskId`; their capacity must not be counted multiple times.

### 6.2 Manager snapshot

Cache Manager combines RoutingInfo and `querySpaceInfo` into the in-memory mapping:

```text
ChainId -> replica TargetId -> NodeId -> PhysicalDiskId -> SpaceSnapshot
```

A snapshot is usable only when every serving replica resolves to one fresh physical disk entry. Missing targets,
incomplete routing, failed queries, and snapshots older than `space_snapshot_max_age` make the affected chain
inadmissible.

### 6.3 Projected usage and inflight reservation

Before admitting a block, Cache Manager evaluates every distinct replica disk:

```text
projected_used = capacity - available + manager_inflight_reserved + block_length
projected_ratio = projected_used / capacity
```

Admission succeeds only when every projected ratio remains below `capacity_high_watermark`. On success, Cache Manager
adds `block_length` to the inflight reservation for every distinct replica disk. It releases those reservations on load
success or failure.

These reservations serialize admission within the single Cache Manager. Storage remains the final authority and may
reject a write whose real space no longer satisfies its local checks.

### 6.4 Logical counters

The existing Metadata `reservedBytes`, `committedBytes`, and `usedBytes` remain useful for answering how much logical
data is queued and READY. `CacheCapacityStore::reserve()` must stop rejecting a request based on `logicalCapacity`; it
only updates counters with overflow and consistency checks.

The configured logical capacity becomes a deprecated informational/reference field. It must not be presented as the
remaining admission capacity.

## 7. Access State in Metadata

Extend `CacheBlockRecord` with:

```cpp
UtcTime readyAt;
UtcTime lastAccessAt;
```

Commit to READY initializes both fields. Cache Manager periodically sends coalesced access updates containing block key,
observed generation, and Manager receive time. Metadata applies:

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
  uint64_t bytes;
  UtcTime readyAt;
  UtcTime lastAccessAt;
  std::vector<PhysicalDiskId> replicaDisks;
};

struct EvictionContext {
  std::set<PhysicalDiskId> pressuredDisks;
  uint64_t bytesToRelease;
  UtcTime now;
  Duration protectionPeriod;
};

class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;
  virtual Result<std::vector<EvictionCandidate>> select(
      std::span<const EvictionCandidate> candidates,
      const EvictionContext &context) = 0;
};
```

The first registered implementation is `LRUEvictionPolicy`. It filters candidates that do not touch a pressured disk,
excludes blocks still in the READY protection period, sorts effective access time oldest first, and selects enough
candidates to meet the release target.

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

Extend the block record with an `EvictionEpoch` and reason. `EVICTING` retains the READY identity and committed logical
bytes. New read plans do not advertise EVICTING as a hit. Old plans may race with deletion; `NotFound` falls back to the
Origin.

Normal capacity eviction does not reuse `CLEANING`. `CLEANING` remains the invalidation/refresh cleanup workflow, while
`EVICTING` represents capacity-policy removal and Storage Event completion.

## 10. Storage Cache Descriptor and Local LRU

Extend cache replace requests with the neutral logical `CacheBlockKey`. Storage persists the following beside cache
chunk metadata:

```cpp
struct CacheChunkDescriptor {
  CacheBlockKey logicalKey;
  CacheGeneration generation;
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

## 11. Normal Eviction Flow

When a physical disk reaches `capacity_high_watermark`, Cache Manager:

1. Stops new admission for chains touching that disk.
2. Calculates bytes required to return the disk to `capacity_low_watermark`.
3. Scans Metadata READY/COMMITTED blocks in bounded pages.
4. Resolves their chains and keeps blocks occupying a pressured disk.
5. Passes candidates and the release target to the configured `EvictionPolicy`.
6. Calls `BeginEvictCacheBlocks` for selected blocks.
7. Skips state-conflicting candidates and continues the batch.
8. Requests generation-fenced chain retirement for successful EVICTING blocks.
9. Waits for the durable logical `DELETED` event to finalize Metadata.
10. Repeats using fresh space snapshots until usage is below the low watermark.

A synchronous retire response means the request reached the Storage protocol's completion point. It does not directly
remove Metadata state; only the durable event does.

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
  CacheBlockKey logicalKey;
  CacheChunkKey storageKey;
  CacheGeneration generation;
  PhysicalDiskId diskId;
  UtcTime timestamp;
};
```

`sourceId` is persisted by the Storage event journal. Sequence numbers are monotonic within a source.

### 13.2 Delete outbox

Deletion uses this local protocol:

```text
persist PREPARED intent
  -> execute generation-fenced delete
  -> verify the generation is retired
  -> mark DELIVERABLE
  -> report until Metadata ACKs
  -> reclaim the journal record
```

The operation ID is stable across retries. On Storage restart, PREPARED records re-run or query the fenced deletion;
DELIVERABLE records resume reporting.

The outbox must use reserved metadata space or another bounded mechanism that remains writable at the local safety
watermark. If the outbox cannot prepare an intent, Storage must not start a new cache deletion. It stops new cache
writes and raises a critical alert rather than silently deleting data without an event.

### 13.3 Logical versus local deletion events

For normal replicated retirement, only the Storage coordinator may make a logical `DELETED` event DELIVERABLE, and only
after the existing chain replication protocol reaches its commit point. An independently deleted local replica produces
`EMERGENCY_EVICTED`, never `DELETED`.

### 13.4 Reporting and ACK

Storage sends contiguous event batches to a Metadata internal RPC. Metadata handles each source in sequence order and,
in one transaction:

1. validates the expected next sequence;
2. validates block identity and generation;
3. applies or safely ignores the state change;
4. advances the persisted acknowledged sequence.

It returns an ACK only after the transaction commits. Duplicate events return the persisted ACK. A gap does not advance
the cursor. Storage retries from the first unacknowledged sequence.

Event effects are:

- matching `DELETED + EVICTING`: remove the record and decrement logical counters exactly once;
- matching `EMERGENCY_EVICTED + READY`: enter EVICTING;
- matching `EMERGENCY_EVICTED + EVICTING`: no-op;
- old generation: no state mutation, but ACK;
- impossible future generation: reject without ACK and alert.

## 14. Failure Behavior

### 14.1 Cache Manager restart

On restart Cache Manager discards admission and reservation memory, force-refreshes RoutingInfo and all related space
snapshots, and keeps admission stopped until the view is complete. It then scans EVICTING records and resumes chain
retirement. Access history for second-miss admission starts empty.

This is deliberately limited recovery. Full LOADING lease recovery, orphan inventory cleanup, and cross-version repair
remain Phase 4 work.

### 14.2 Storage and network failures

- Retire timeout leaves EVICTING and is retried with bounded backoff.
- Event report failure retains DELIVERABLE records and retries.
- Metadata outage does not discard events. Storage may continue local safety deletion only while it can prepare outbox
  entries.
- Partial replica failure cannot produce logical DELETED, so logical state remains conservative.
- Storage no-space during load fails the cache load and uses the existing fenced cleanup path.

### 14.3 Routing changes

Eviction work and events carry `VersionedChainId`. A route change must not be interpreted as proof that the old
generation disappeared. Cache Manager attempts retirement using the recorded version. If it cannot prove deletion, the
block remains EVICTING and its logical bytes remain counted. Cross-version inventory repair is deferred to Phase 4.

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
eviction_protection_period = "10m"
eviction_batch_size = 256
```

Representative Storage configuration:

```toml
local_eviction_policy = "lru"
local_safety_high_watermark = 0.95
local_safety_low_watermark = 0.90
local_access_persist_interval = "30s"

cache_event_batch_size = 256
cache_event_report_interval = "1s"
cache_event_journal_max_bytes = "1GiB"
```

Validation requires:

```text
0 < capacity_low < capacity_high < 1
0 < local_safety_low < local_safety_high < 1
capacity_high < local_safety_high
positive intervals, limits, and batch sizes
registered policy names
```

Configuration changes may require restart in Phase 2. Hot-reload semantics are not required.

## 16. Observability

Cache status must expose:

- physical disk identity, capacity, available bytes, usage, and snapshot age;
- Node, Target, and cache-chain associations;
- normal and local safety watermarks;
- admission-paused state and reason;
- per-disk inflight Manager reservation;
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
- Replica-disk resolution, shared-disk deduplication, stale snapshots, and per-disk reservation.
- LRU selection, protection period, pressured-disk filtering, release target, factory validation, and fake policy use.
- READY access generation CAS and READY-to-EVICTING state transitions.
- Logical counters update correctly without enforcing logical capacity.
- Storage descriptor persistence and coalesced local access updates.
- Event sequence, contiguous ACK, compaction, restart recovery, and journal bound.
- Duplicate, delayed, stale-generation, future-generation, DELETED, and EMERGENCY_EVICTED consumption.
- All configuration relations and boundary values.

### 17.2 Integration tests

1. First Origin miss does not cache; the second miss within the window reaches READY.
2. One pressured replica disk stops only affected-chain admission.
3. Normal eviction invokes the configured policy and returns the disk to the low watermark.
4. EVICTING is no longer returned as a cache hit.
5. Retire success with delayed Event keeps Metadata EVICTING.
6. DELETED Event removes the record and decrements logical bytes.
7. Local safety eviction emits EMERGENCY_EVICTED and converges through EVICTING to no record.
8. Cache Manager restart resumes EVICTING work.
9. Duplicate, delayed, and network-interrupted Events eventually converge once.
10. Multiple Targets sharing a disk do not multiply its capacity.
11. Cache Targets reject ordinary user-data chains and writes.
12. MinIO Origin miss, admission, hit, eviction, and later Origin fallback form a complete loop.

### 17.3 Crash injection

Inject a crash after each of these points:

- PREPARED persisted, before delete;
- delete completed, before DELIVERABLE;
- DELIVERABLE persisted, before report;
- before Metadata event transaction commit;
- after commit, before ACK response;
- after ACK response, before Storage journal reclamation.

Every case must preserve generation fencing, retain all required events, avoid premature or duplicate logical-counter
release, and eventually converge.

Performance is not a Phase-2 acceptance gate. Tests still verify that access reporting, admission, and eviction do not
block foreground Origin fallback.

## 18. Upgrade and Rollback

Bump the cache protocol and schema capability for Phase 2. Upgrade in this order:

```text
Storage -> Metadata -> Cache Manager -> Client -> enable Phase 2
```

New behavior is disabled until all components advertise support. Before enabling, verify that cache Targets are
dedicated, stable disk IDs exist, event journals are writable, routing resolves to physical disks, and watermark
configuration is valid.

For rollback, first stop new admission and eviction, wait for the DELIVERABLE event backlog to drain, and then disable
Phase 2. Do not disable event consumption while unacknowledged delete events remain.

## 19. Acceptance Criteria

Phase 2 is complete when:

- physical disk space, not logical capacity, controls admission;
- normal and local high/low watermarks are configurable and provide hysteresis;
- eviction policy selection is pluggable and LRU is the default implementation;
- normal and local safety triggers converge through one fenced delete and event protocol;
- Storage Events survive the defined crash windows and are delivered at least once;
- Metadata applies every logical deletion exactly once;
- Cache Manager restart resumes EVICTING work;
- foreground reads retain safe Origin fallback under all capacity and event failures;
- Phase-1 cache, Metadata, Storage, analytics, and MinIO acceptance tests do not regress.
