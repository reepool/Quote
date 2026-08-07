## Context

Business-profile work is versioned by a processing-identity hash, while semantic checkpoints also bind instruments, field families, knowledge cutoff, runtime identities, and promotion manifests. Production currently recovers every stale-scope failure before enqueueing current-identity work. Recovery preserves any checkpoint with a readable cutoff, but the stage runner uses the current runtime identity. After an LLM route identity changed, 28 old-identity checkpoints were therefore requeued, rejected again, and claimed ahead of ordinary work. Worker failures did not affect the top-level success status.

## Goals / Non-Goals

**Goals:**

- Resume only work whose durable processing identity matches the active invocation.
- Preserve a checkpoint only when its complete logical scope matches the work-bound scope.
- Automatically retire obsolete-identity pending and failed work when a current replacement exists.
- Keep recovery auditable and idempotent without deleting checkpoints or annual-report assets.
- Prevent newly recovered items from monopolizing a bounded stage budget.
- Report discovery and worker health truthfully through one shared status policy.

**Non-Goals:**

- Do not rewrite historical checkpoints or delete terminal-work evidence.
- Do not resume old runtime identities through a newly configured LLM route.
- Do not change stage concurrency, stage budgets, public commands, or database schema.
- Do not mark ordinary queue backlog or resumable discovery windows as failures.

## Decisions

1. Pass the active processing identity into stale-scope recovery and worker claims, and filter candidates by its stable hash. This is safer than executing an old identity with the current client, which would make lineage false, and safer than rotating every old item, which duplicates current-identity work.
2. During enqueue, supersede non-running, non-completed work for the same frontier and policy when its processing identity differs from the current work. Under the latest-annual policy, also supersede terminal work bound to an older annual-report frontier when a newer report or correction replaces it. The row and original error remain available for audit; only its queue disposition changes. Running work is never superseded underneath an active lease.
3. Inspect the complete checkpoint logical scope before in-place preservation. Compatibility includes instrument, field families, cutoff, runtime identities, and promotion manifest hashes; source revision remains intentionally mutable. Missing or incompatible scope rotates to a new checkpoint and restarts at acquire, where the durable annual-report manifest prevents duplicate downloads.
4. Order claims by report period and stage-entry time (`updated_at`) before creation time. Recovery updates `updated_at`, so already-waiting ordinary work gets a bounded opportunity before freshly recovered work without adding queue schema or mutable scheduler state.
5. Use one status helper for daily and backfill reports. Stop requests take precedence; discovery failure, worker retries, terminal failures, configuration blocks, and lease conflicts produce `degraded` plus explicit reason codes. A bounded run with remaining claimable work can still be successful when its attempted work is healthy.

## Risks / Trade-offs

- [A runtime identity changes while old work is running] -> Do not supersede an active lease; identity-filtered claims exclude it and the next enqueue retires it after expiry.
- [Old terminal rows remain visible] -> Preserve them as `superseded` audit records and exclude them from terminal health counts.
- [Stage-entry ordering delays recovery] -> It only prioritizes already-waiting work within the same latest report period; recovery remains claimable in later batches.
- [Malformed legacy metadata prevents exact scope comparison] -> Rotate the checkpoint and restart from acquire using reusable archived assets.

## Migration Plan

Deploy without a schema migration. On the next daily or backfill invocation, current-identity enqueueing retires obsolete work and eligible current-identity stale failures are recovered using full-scope validation. Rollback leaves superseded audit rows and rotated checkpoint files intact; no source asset or published fact is removed.

## Open Questions

None.
