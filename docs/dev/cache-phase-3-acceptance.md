# Cache Phase 3 acceptance

Record date: 2026-08-07

## Scope and revision

This record closes the Phase 3 data-orchestration implementation tasks T0..T30 on branch `ljh/dev`. The frozen Phase 2
acceptance revision is `b8a87a7deccaf6c49f394a585dfe9a4d3ad27b51`; the Phase 3 baseline and plan start at
`dffa068`. Phase 3 remains disabled by default.

The implementation chain is:

- T1..T7, identities, wire contracts, keyspace, durable jobs/plans/pins, and Metadata operations: `689e40d` through
  `17f4c6f`;
- T8..T13, planner boundary, namespace/path-list/manifest planners, paged object listing, and prefix import:
  `c7848f7` through `f645ca7`;
- T14..T22, explicit admission, shared claims, strict priority, per-job quota, durable runner/lifecycle/tracker, READY
  transition, and cancellation: `a8460da` through `4d65fac`;
- T23..T29, active and retained pins, eviction fences, public API/CLI, observability, and rollout gates: `7014214`
  through `c578cc4`;
- T30, focused regression qualification, contract/config coverage, and this record: the commit containing this file.

## Functional evidence map

- path, path-list, manifest, and prefix planning: `TestNamespaceFilePlanner`, `TestNamespaceDatasetPlanner`,
  `TestManifestPlanner`, and `TestS3PrefixPlanner`;
- page commit replay, list-token progress, and import retry: `TestJobPlanner`,
  `TestS3PrefixPlanner.ReplaysSameObjectAfterImportOrPageCommitTimeout`, and the exact-retry/empty-final-page cases in
  `TestPrefetchPlanStore`;
- priority, per-job concurrency/bandwidth, physical-capacity admission, and shared ownership: `TestJobQueue`,
  `TestJobQuota`, `Phase2EnsureCachedTest`, and `TestHintCoalescer`;
- admission timeout, durable-attempt restart, completion/attach race, and Manager recovery: `TestJobRunner` and
  `TestOrchestrationCoordinator`;
- ratio and generation-fenced READY transition: `TestJobTracker`, `TestPrefetchReadyStore`, and
  `TestPrefetchJobStateMachine`;
- cancellation durability and queued/loading race convergence: `TestPrefetchCancellationStore` and
  `TestJobCanceller`;
- active pin renewal, retained-pin conversion, TTL ownership, and eviction fencing: `TestActiveJobPinManager`,
  `TestPinStore`, `TestCacheOrchestration`, `TestEvictionController`, `TestPermitRecovery`, and `TestCacheEviction`;
- public API, CLI, metrics, capability, drain, and rollback: `TestCacheManagerService`, `CacheAdminCli`,
  `TestCacheMetrics`, and `CachePhase3Rollout`.

## Local qualification evidence

The initial 2026-08-06 local qualification used the existing Clang 14 `RelWithDebInfo` incremental build with cache
transport disabled. All 80 non-AWS Phase 3 changed translation units compiled with
`-Wall -Wextra -Werror -Wpedantic`; the focused executables `test_cache_manager`, `test_cache`, `test_meta`,
`test_mgmtd`, and `test_admin_cli` built and linked successfully.

The 2026-08-07 follow-up enabled `HF3FS_ENABLE_CACHE` and `HF3FS_ENABLE_CACHE_INTEGRATION_TESTS`, built and linked
the AWS executor and `test_cache_minio` with Clang 14 and AWS SDK for C++ 1.10.55, and ran against MinIO
`RELEASE.2025-09-07T16-13-09Z` on an isolated loopback endpoint and disposable bucket. The complete MinIO suite
passed 3/3. Its Phase 3 case stores real prefix and manifest objects, exercises paged prefix listing/import,
namespace path and path-list planning, segmented manifest reads with cursor continuation, and verifies the planned
payload through ranged S3 reads. The same case also covers strict-priority selection, ready-ratio transition,
cancellation completion, and active-job pin TTL renewal over the real-object plan.

Final focused runs passed:

- Cache Manager planner, scheduler, admission, capacity, eviction, job, pin, cancellation, and restart cases: 118/118;
- cache common types, service contracts, origin/object-store boundary, and metrics cases: 37/37;
- Metadata orchestration, job/plan/pin stores, READY state, eviction, event, and capacity state-machine cases: 49/49;
- mgmtd Phase 2/3 capability, drain, rollback, and chain gates: 10/10;
- Admin CLI cache lifecycle and orchestration cases: 6/6.

Formatting validation and `git diff --check` pass for the Phase 3 source and test changes. The example Cache Manager
configuration includes every Phase 3 lifecycle, page-size, prefix-layout, active-pin TTL, and renewal setting while
retaining `enable_phase3 = false`.

## Remaining deployment qualification

The local real-MinIO path/list/manifest/prefix qualification is complete. Deployment CI still needs to repeat
`test_cache_minio` with the production dependency image and a disposable versioned bucket, then run the complete
multi-service planner/job workflow with injected Cache Manager process restarts. That workflow must cover durable
attempt replay and retained-pin conversion/expiry in addition to the locally qualified priority, ratio, cancellation,
and active-pin renewal controls.

The deployment run must also archive the Phase 3 status before enable and after drain, verify zero non-terminal jobs,
zero `ACTIVE_JOB` pins, and zero exclusive queued claims before rollback, and retain the Phase 1/2 real-MinIO
foreground fallback and Storage event/local-safety suites. Missing infrastructure is an unmet deployment test, not a
functional pass; any assertion or timeout after successful setup is a qualification failure.
