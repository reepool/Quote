## Context

Investigation on 2026-10-25 (evidence: `data/financials.db`, live CNInfo data20 probes archived under `data/QuoteBak/cninfo_equity_probe/`, task.log runs 2026-10-16..10-25):

- CNInfo data20 condensed statement endpoints return 11 balance-sheet rows / 6 income rows / 3 cash-flow rows per instrument. The three skipped balance-sheet rows are section headers (`资产类科目`/`负债类科目`/`股东权益类科目`) — nothing valuable is dropped.
- `equity_parent` is systematically absent (no `归属于母公司…权益` row, no `少数股东权益` row, so the existing `_derive_parent_equity` in `financial_xbrl_parser.py` can never fire). All other four core fields map and match third-party values exactly (spot-verified on 002254.SZ 2026-09-30).
- `financial_statement_maintenance_repair.py` computes readiness all-or-nothing per target, then the fallback path (`run_local_core_dryrun` → `FinancialStatementsShadowSyncService.sync(force_full=True)`) writes the akshare bundle as the authoritative core row (`financial_core_facts_hot.source_file_id` points at the akshare bundle).
- `config/10_research.json` → `disclosure_incremental_sync` declares `official_first: true` and `fallback_only_missing_or_ambiguous_facts: true`; neither flag is consumed anywhere.
- Core fact rows already carry `lineage_json` with per-field derivation metadata (`core_fact_alias_matches`), so per-field provenance has a natural home without migrating `financial_facts`/`financial_core_facts_hot`.

## Goals

- Official values stay authoritative wherever the official source provides them; third-party fills only genuine per-field gaps (today: `equity_parent`; occasionally per-instrument gaps).
- Every core field's supplying source is recorded and queryable per field.
- Overlapping official/fallback values are compared each run using the same fetched bundle (zero extra requests); discrepancies are marked and reported.
- No process-data accumulation: comparison-only fallback facts and stale comparison rows are cleaned within the run cycle.
- Rollback switch preserves today's whole-bundle behavior.

## Non-Goals

- Deriving `equity_parent` from CNInfo `getMainIndicators` per-share data (deferred; revisit only if provenance purity for that one field is later worth the derivation risk).
- Restoring `sse_commonquery` as the SSE official source (separate validation effort).
- Rewriting or re-stamping historical core rows; backfilling `field_sources` for existing rows.
- Any new data source, new endpoint, or change to the required-core-fact set.

## Decisions

### D1. Merge point: fallback write path takes official facts as input

The repair module (`financial_statement_maintenance_repair.py`) already owns the readiness view after the CNInfo pass (`_merge_official_cninfo_readiness`, `_ready_target_keys`). It will pass a per-target `official_facts` map (field → value + source_file_id) into the fallback run, and the core snapshot build in `financial_statements_sync.py` will compose the final row official-first per canonical field. Alternatives considered: post-write recomposition (rejected — double writes and changelog noise) and merging only in the read path (rejected — leaves authoritative tier wrong).

Governance note: `run_local_core_dryrun` lives under `scripts/dev_validation/` but is already the production fallback entry; this change keeps it a parameter pass-through only. The merge, comparison, and cleanup logic SHALL be implemented in `research/` modules, not added to the script.

### D2. Provenance and availability semantics of a merged row

Each written core row gains `lineage_json.field_sources = {<canonical_fact>: {source, source_file_id, role}}` where role is `official_structured` or `fallback_fill`. The row-level `source_file_id` continues to identify the writing bundle (write-path traceability); per-field authority is `field_sources`. Readers that need per-field authority read `field_sources` and fall back to row-level semantics when absent (historical rows unchanged). No schema migration: `facts_json`/`lineage_json` exist on `financial_facts`, `financial_core_facts_hot`, and `financial_core_facts_history`, and all three receive the same snapshot payload through the existing write path.

Row-level `publish_date`/`data_available_date` keep the writing (fallback) bundle's values, preserving today's availability semantics; data20 condensed endpoints carry no per-fact announcement date, so per-field availability refinement is deferred and out of scope. Consumers requiring point-in-time dates continue to use row-level dates as today.

### D3. Comparison semantics

Both sides are CNY-normalized numeric facts already. Comparison uses relative tolerance `source_fact_reconciliation.relative_tolerance` (default `0.005`) with absolute floor `absolute_floor_cny` (default `1.0`): equal if `abs(a-b) <= max(floor, tolerance * max(abs(a), abs(b)))`. Beyond tolerance → discrepancy record with both values and relative difference. The comparison scope starts with the five required core facts and is configurable; expanding it must not require code changes.

Single-sourced fill fields (no second source to compare, today `equity_parent`) additionally get a definitional plausibility check against same-row official facts: implied minority interest = `equity_total` − `equity_parent` must be non-negative and within a loose configurable share of `equity_total` (default `0.5`); violations are marked as review-only anomalies, reported, and do not block the write. Rationale: the only field without a second source should not be the only field without any quality check; minority interest can legitimately be large but rarely dominates total equity.

### D4. Comparison storage: run-scoped table, single-run retention

New table `financial_source_fact_comparison_results` (run_id, instrument_id, report_period, canonical_fact, official_source, official_value, fallback_source, fallback_value, relative_diff, compared_at). At run start the runner deletes prior rows; at run end the table holds only the latest run's discrepancies plus counts in run metadata. This is process data, not durable acceptance evidence — durable evidence is the operator report and, when needed, an escalated issue.

### D5. Cleanup of fetched-but-unused fallback facts

Fallback numeric facts that were not used for field-level fill (comparison-only overlap fields) are deleted at the end of the run, identified by the fallback bundle's `source_file_id` minus the fill-used field set. Persist-then-clean is chosen over never-persisting so the existing write path and in-run auditability are preserved.

Compatibility constraint: `financial_numeric_facts_hot` has other readers (`scripts/dev_validation/audit_financial_numeric_fact_coverage.py`, `scripts/dev_validation/live_audit_sina_ths_local_core.py`, `scripts/research_financial_l1_full_import.py`). Before enabling deletion, the change SHALL verify none of these consumers asserts the presence of fallback numeric facts for filled targets; audit tools that merely report coverage counts are expected to reflect the new retention policy rather than fail.

### D6. Flag consumption and rollback

`fallback_only_missing_or_ambiguous_facts` (already declared, default `true`) selects field-level fill; `false` restores whole-bundle fallback writes (today's behavior). `source_fact_reconciliation.enabled` (default `true`) independently gates comparison; when disabled, no comparison, no extra table writes, and cleanup still removes nothing beyond today's behavior.

### D7. Standalone comparability

The comparison is implemented as a pure service function over (official facts, fallback facts, tolerance) returning records + counts. The incremental run calls it inline with the already-fetched bundle; a future standalone audit task can call it over stored tiers without fetching.

## Risks / Trade-offs

- Discrepancies may be noisy at first (real source disagreements surface). Mitigation: report aggregates counts and shows top N samples (configurable `report_top_n`, default 5); discrepancies do not fail the run — they are marked and reported.
- Merging inside the shadow sync service adds parameters to a path shared with dev-validation scripts; both callers keep working (dev validation passes empty `official_facts`, equivalent to legacy fill of everything the bundle has).
- Extra deletes add write volume per run; bounded by the run's fallback target count (hundreds of instruments), acceptable for SQLite in the nightly window.

## Migration / Rollout

- Additive table + additive lineage keys; no data migration, no rewrite of existing rows.
- Rollout: flag-guarded, default-on for fill policy (matches the already-declared config intent), reconciliation on by default; both disable-able.
- Acceptance evidence: focused unit suites plus the first live nightly run's report showing a mixed row (4 official + 1 fallback field), comparison counts, and zero leftover comparison-only facts (verified by SQL count after the run).
