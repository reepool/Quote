## Context

The durable queue stores a checkpoint path but currently constructs every stage scope from the invocation's current knowledge cutoff. A work item created on one day therefore becomes logically incompatible when resumed on a later day. `recover_stale_scope_items` resets queue state but retains both the stale checkpoint and the changing invocation cutoff, so recovered items fail again and become terminal. The async service also has no module logger, leaving discovery and multi-minute worker batches silent.

## Goals / Non-Goals

**Goals:**

- Keep one stable knowledge cutoff for the full lifetime of a durable work item.
- Recover existing stale-scope failures without manual database or checkpoint editing.
- Emit bounded, structured progress logs during all long operations and while individual batches are in flight.
- Preserve concurrent parse and semantic computation and the single-writer database coordinator.

**Non-Goals:**

- Do not change the public `/run business_profile_backfill` parameters.
- Do not increase stage budgets, discovery page limits, or LLM concurrency.
- Do not treat resumable discovery-window backlog as data loss or clear unrelated terminal failures.
- Do not add a database column or migrate existing production data.

## Decisions

1. Persist `knowledge_cutoff` inside the existing work-item metadata JSON. New work records receive it at enqueue. Existing records derive it from their semantic checkpoint when available and otherwise adopt the current enqueue cutoff before their first checkpoint is created. This avoids a schema migration while making temporal semantics explicit.
2. The stage runner uses the work-bound cutoff rather than the enclosing daily invocation cutoff. A work item therefore has one reproducible information set across acquire, parse, semantic, publish, interruption, and restart.
3. Stale-scope recovery first restores the checkpoint cutoff into work metadata. If the checkpoint is unreadable or has no usable scope cutoff, recovery rotates to a new checkpoint path, clears invalid stage results, and restarts from acquire; the acquire stage reuses a valid archived report and does not download it again.
4. Add a module logger to async production. Emit INFO logs for lifecycle boundaries, recovery/discovery/enqueue summaries, stage start/end, each completed batch, and periodic in-flight heartbeats. Emit WARNING logs for item failures, terminal counts, discovery backlog, and degraded completion. Identifiers and counts are logged, not report text or LLM payloads.
5. Keep the final scheduler report compatible but add explicit start/end logs and elapsed time. `Tasks Completed` remains fully published items; stage counters in logs and report detail explain partial progress.

## Risks / Trade-offs

- [Old work has no persisted cutoff and a damaged checkpoint] -> Rotate the checkpoint and restart from acquire using the current cutoff; archived PDFs remain reusable.
- [Heartbeat logs become noisy] -> Log at a fixed bounded interval and at batch boundaries, not for every successful item.
- [Concurrent stage counters are read while tasks update them] -> Event-loop updates are atomic between awaits; telemetry is observational and does not control correctness.
- [Recovery could touch unrelated stale work from an obsolete processing identity] -> Existing work identities remain isolated by processing identity; this change only requeues rows with the exact stale-scope marker and records recovery history.

## Migration Plan

Deploy without a database migration. On the next backfill, stale-scope rows are automatically repaired, existing checkpoints provide their original cutoff where possible, and new or reused work metadata is backfilled idempotently. Rollback leaves the extra metadata keys harmless and preserves rotated checkpoints.

## Open Questions

None.
