## Context

Company-action behavior spans CNInfo/TDX acquisition, candidate discovery, semantic decisions, operator review, canonical selection/promotion, factor rebuilding, validation, and historical backfill. DataManager currently contains the largest methods in the repository, while scheduler jobs encode stage order by naming and call patterns.

The completed `triage-announcement-only-xdxr-candidates` slice is part of the current behavior baseline. W6 extraction must preserve its announcement-only modes, case metadata, inactive-watch/reactivation semantics, and report fields while moving ownership outward.

## Goals / Non-Goals

**Goals:**

- Make the lifecycle and state ownership explicit.
- Separate provider protocol code from application orchestration.
- Preserve canonical factor values, evidence, operator decisions, and query semantics.
- Reduce DataManager to compatibility delegates after caller migration.

**Non-Goals:**

- Changing source priority, LLM prompts, factor formulas, or governance policy.
- Combining every company-action module into one file.
- Removing staged jobs that have distinct operational retry/approval semantics.

## Decisions

1. **Define four application services.** Observation/acquisition, resolution, operator review, and canonical-factor application each own a stage and its transitions.
2. **Use explicit state transition results.** Each stage returns accepted/rejected/pending/skipped details, evidence references, watermarks, and next-stage eligibility rather than relying on job names.
3. **Keep provider modules source-specific.** CNInfo/TDX request, parsing, and transport remain provider concerns; services consume normalized records.
4. **Keep canonical authority singular.** Rebuilds and candidate analysis may write staging/evidence tables, but only the canonical application service can promote the authoritative factor path.
5. **Use compatibility delegates during migration.** Existing DataManager and scheduler entry points invoke the new service; no parallel state machine is introduced.
6. **Keep scheduler migration staged.** W6 may rebind existing job methods to stage services, but domain handler extraction and final job resolution remain W7 responsibilities.

Alternatives rejected: a single rewrite risks factor regressions; keeping job-specific orchestration preserves hidden state flow; moving provider parsing into application services would duplicate source contracts.

## Risks / Trade-offs

- **[Historical decisions are not replayed identically] ->** Preserve state tables/evidence and compare canonical outputs on fixtures before cutover.
- **[Stage retries duplicate writes] ->** Reuse existing ids/checkpoints and test idempotent transitions.
- **[Manual operator workflows are missed] ->** Inventory Telegram/scripts/API commands before removing DataManager methods.
- **[Service split creates cycles] ->** Make stage dependencies one-way and inject provider/repository ports.

## Migration Plan

1. Draw the current state transition and table ownership map.
2. Extract normalized observation and resolution commands with read-only comparison mode.
3. Extract operator review and canonical promotion services.
4. Rebind scheduler and operator entry points, preserving job ids and reports.
5. Remove DataManager business blocks and retain thin delegates.
6. Rollback by rebinding stage triggers to existing methods; do not run old and new state machines together.

## Open Questions

- Which stage status fields are already consumed externally and require a compatibility projection?
- Which canonical factor operations can be migrated independently of the active announcement asset work?
