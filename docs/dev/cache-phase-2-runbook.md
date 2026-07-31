# Cache Phase 2 rollout and rollback runbook

Phase 2 must only be enabled after all cache disks are dedicated `CACHE_ONLY`
disks and every participating binary advertises the Phase 2 protocol
capability. Capacity admission is based on physical disk usage; logical cache
usage is diagnostic only.

## Inventory checks

Before rollout, record the routing generation and verify:

- every cache-chain target has role `CACHE_ONLY`, a stable non-zero physical
  disk ID, and no target on the disk serves ordinary 3FS data;
- Storage can open its permit store and event journal, the journal is writable,
  and configured local safety low/high watermarks satisfy
  `0 < low < high < 1`;
- Cache Manager receives fresh space snapshots from every replica and its
  normal high watermark exactly matches Storage admission enforcement;
- Metadata, Cache Manager, Storage, clients, and mgmtd all advertise the same
  Phase 2 protocol capability;
- there are no dead letters or unexplained event sequence gaps.

Do not enable on a partial inventory. A missing/stale snapshot, unknown policy,
unwritable journal, mixed-role disk, or watermark mismatch is a fail-closed
condition.

## Upgrade

1. Stop Phase 1 admission while reads continue to fall back to origin.
2. Fenced-clean all Phase 1 READY records and wait for Metadata READY,
   LOADING, CLEANING, and EVICTING counts to reach zero.
3. Query every cache target and verify there is no ACTIVE cache generation.
   Resolve incomplete LOADING records manually; Phase 2 does not promise
   recovery of arbitrary cross-version LOADING work.
4. Upgrade Metadata and Storage, then Cache Manager and clients. Keep Phase 2
   disabled.
5. Run the inventory checks above. Enable the cluster capability first, then
   enable Storage and Cache Manager. Enabling is retryable once the complete
   inventory is healthy.
6. Confirm fresh disk snapshots, a non-zero Manager epoch, zero event backlog,
   and successful permit prepare/consume before restoring admission.

## Drain and rollback

1. Stop new admission and let queued permits expire or cancel them by their
   exact persisted identity.
2. Wait for all EVICTING operations to converge and for Storage event journals
   to be ACKed. A recorded replica that is still ACTIVE blocks rollback.
3. Fenced-clean remaining READY records and verify Metadata nonterminal counts
   and Storage ACTIVE generations are both zero.
4. Disable Phase 2 in Cache Manager, Storage, and cluster capability order.
5. Only then deploy Phase 1 binaries. Phase 1 binaries must never see Phase 2
   descriptor/event data.

If an old replica cannot be contacted, keep the operation EVICTING and the
cluster drained. Do not erase Metadata or logical statistics by hand. Preserve
the operation ID, placement, generation, event source, and sequence for repair.

## Operational diagnosis

Admission pauses on stale/missing snapshots, physical high watermark,
outstanding reservations, pressured disks, routing/placement mismatch, or an
unwritable journal. Inspect `GetPhase2CacheStatus` for disk capacity, used,
allocatable, reserved, snapshot age, policy, Manager epoch, and EVICTING count.
Correlate logs with block key, cache generation, placement, eviction epoch,
retire operation ID, event source, and sequence. Never log service tokens or
origin credentials.
