## Context

Daily updates and backfills are exposed through CLI, API, scheduler, and Telegram. Gap repair additionally has DataManager logic, scheduler orchestration, `smart_fill_gaps.py`, `find_gap_and_repair.py`, and Telegram subprocess execution. These paths differ in universe filtering, calendar assumptions, skip records, factor sync, and persistence checks.

## Goals / Non-Goals

**Goals:**

- Define one application command family for quote maintenance modes.
- Give gap repair one write owner and route every entry point to it.
- Preserve existing public entry points as thin compatibility adapters.
- Remove production dependence on validation scripts and Telegram subprocess bypasses.

**Non-Goals:**

- Changing source priority, quote schema, adjustment semantics, or coverage policy.
- Combining daily, historical, and gap repair into one untyped mega-function.
- Replacing APScheduler or introducing a queue.

## Decisions

1. **Use explicit command modes.** `daily`, `target_date`, `range`, `historical`, and `gap_repair` have separate validated command payloads while sharing identity, universe, calendar, source, and persistence ports.
2. **Create a quote-maintenance application boundary.** A quote-domain package owns command execution; `DataManager` and scheduler delegate during migration.
3. **Make gap repair authoritative.** Detection, lifecycle filtering, skip policy, fill, factor follow-up, persistence verification, and report data live in one service.
4. **Keep scripts as adapters.** Existing scripts parse operator arguments and call the service; they do not own SQL/download loops.
5. **Use a single-flight guard at the application boundary.** API, scheduler, and Telegram cannot launch concurrent writes for the same maintenance scope.

Alternatives rejected: leaving scripts as independent implementations preserves the bug; a single untyped `run_data(mode=...)` would hide incompatible semantics; rewriting all CLI/API contracts would unnecessarily break operators.

## Risks / Trade-offs

- **[Existing paths have undocumented parameter differences] ->** Characterize inputs and results before migration and preserve explicit mode contracts.
- **[A long repair is interrupted] ->** Retain checkpoints/skip records and verify persisted dates after each segment.
- **[A scheduler and manual command race] ->** Reuse scheduler/application single-flight identity and reject or attach to an equivalent run.
- **[Factor follow-up changes timing] ->** Preserve existing factor write behavior and test quote/factor equivalence separately.

## Migration Plan

1. Inventory command callers and compare parameters/results.
2. Add command/result models and contract tests without changing production bindings.
3. Move gap detection/fill into the service and bind scheduler/API first.
4. Convert CLI and Telegram to the same command adapters.
5. Remove subprocess execution and script business loops.
6. Migrate daily/range/historical modes and then reduce DataManager methods to delegates.
7. Rollback by rebinding adapters to the prior DataManager methods while retaining the new service tests; do not run both writers concurrently.

## Open Questions

- Should an equivalent in-flight repair be rejected or exposed as a shared status handle for manual callers?
- Which existing report fields are contractual for Telegram/API consumers and require compatibility snapshots?
