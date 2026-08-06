# Cache Phase 3 rollout and rollback

Phase 3 is disabled by default and requires Phase 2 to remain `ENABLED`. The cluster rollout state is stored in the
`cache.phase3.rollout` universal tag; local `enable_phase3=true` alone does not authorize orchestration requests.

## Upgrade and enable

1. Upgrade Metadata, Cache Manager, and `admin_cli` to protocol 3. Keep Phase 3 disabled.
2. Confirm `cache-status` reports healthy Phase-2 disks and no Phase-3 inventory.
3. Run `cache-phase3-rollout drain`. This publishes capabilities without admitting new work.
4. Run `cache-phase3-rollout enable`. Mgmtd rejects the transition unless Phase 2 is enabled, every live Metadata node
   advertises the current schema/protocol, Manager/Meta/CLI capabilities are 3, and the inventory is empty.
5. Set `enable_phase3=true` on Cache Manager and Metadata and restart them. Verify `cache-status`, then create a small
   path Job before enabling larger manifest or prefix imports.

## Drain and rollback

1. Run `cache-phase3-rollout drain`. Planner and runner stop admitting new work; tracker and pin renewal remain active.
2. Use `cache-prefetch list`, then cancel every non-terminal Job. Remove explicit pins with `cache-pin remove`.
3. Repeat `cache-status` until `phase3.active_jobs`, `phase3.pinned_bytes`, and
   `phase3.exclusive_queued_claims` are all zero.
4. Run `cache-phase3-rollout disable`. The CLI and Mgmtd both fail closed if the inventory is not empty or the previous
   state was not `DRAINING`.
5. Set `enable_phase3=false` and restart Cache Manager/Metadata if rolling binaries back. Phase-1/2 foreground Origin
   fallback remains available throughout.

Do not remove orchestration FDB prefixes during rollback. Schema and wire fields are append-only so old readers ignore
the persisted Phase-3 records and a later re-enable can inspect them safely.

## Failure handling

- A Planner, admission, or pin renewal error is retried by its independent worker and never blocks foreground fallback.
- If the Manager restarts, ACTIVE_JOB pins and non-terminal Jobs are rebuilt from Metadata before periodic work resumes.
- If all eviction candidates are pinned, admission remains paused and eviction reports `no_candidates`; do not bypass
  the pin fence. Cancel/unpin owners or add physical capacity.
- Never force the universal tag directly. Unknown fields, direct ENABLED-to-DISABLED transitions, half-upgraded nodes,
  and incomplete drains are intentionally rejected.
