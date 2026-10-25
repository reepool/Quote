## Context

Investigation on 2026-10-25 (evidence: `data/financials.db`, live CNInfo data20 probes archived under `data/QuoteBak/cninfo_equity_probe/`, task.log runs 2026-10-16..10-25):

- CNInfo data20 condensed statement endpoints return 11 balance-sheet rows / 6 income rows / 3 cash-flow rows per instrument. The three skipped balance-sheet rows are section headers (`资产类科目`/`负债类科目`/`股东权益类科目`) — nothing valuable is dropped.
- `equity_parent` is systematically absent (no `归属于母公司…权益` row, no `少数股东权益` row, so the existing `_derive_parent_equity` in `financial_xbrl_parser.py` can never fire). All other four core fields map and match third-party values exactly (spot-verified on 002254.SZ 2026-09-30).
- `financial_statement_maintenance_repair.py` computes readiness all-or-nothing per target, then the fallback path (`run_local_core_dryrun` → `FinancialStatementsShadowSyncService.sync(force_full=True)`) derives the core row from the pooled numeric facts. Collision resolution today is implicit (alias iteration order plus per-`fact_name` last write in `facts_by_name`); production rows are already partially mixed as a result (e.g. `inventory`/`receivables` resolve to CNInfo Chinese aliases while the five required fields resolve to akshare aliases), and `financial_core_facts_hot.source_file_id` points at the akshare bundle.
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

### D1. Merge point: official-first source preference at core-fact derivation

Core rows are not built from the fallback bundle alone: `FinancialStatementsShadowSyncService` derives each core row via `storage.derive_financial_core_facts_from_numeric_facts(instrument, period, alias_mapping)`, which reads the pooled numeric facts — at fallback time the pool already holds both the CNInfo facts (written by the official pass earlier in the same run) and the freshly written fallback facts. Today multi-source collisions resolve implicitly: `facts_by_name` keeps one row per `fact_name` (last write wins) and alias iteration order decides across differently-named facts, which is why the fallback bundle stamps the row.

The field-level policy is therefore implemented as an explicit official-first source preference per core field at this derivation step: for each core field, prefer the fact whose source profile is the official one (`cninfo_data20`), and take a fallback alias only when the official fact is absent or invalid. Two ordering constraints: the existing `canonical_compatible` semantic gate is evaluated first and SHALL NOT be overridden by source preference — an official fact with the wrong semantics (e.g. `所有者权益`, total-owners-equity semantics) must never fill the parent-equity core field; and the preference applies across the derivation pool as read (`include_history=True`, hot plus history tiers). No `official_facts` map needs to be threaded through the shadow sync service; the repair module's post-CNInfo readiness view (`_merge_official_cninfo_readiness`, `_ready_target_keys`) remains the readiness authority and the fallback-target selector.

Alternatives considered: threading an `official_facts` map through the service (heavier, duplicates what derivation already sees) and post-write recomposition of core rows (rejected — double writes and changelog noise).

Governance note: `run_local_core_dryrun` lives under `scripts/dev_validation/` but is already the production fallback entry; this change keeps it a parameter pass-through only. The preference, comparison, and cleanup logic SHALL be implemented in `research/` modules, not added to the script.

### D2. Provenance, PIT ordering, and availability semantics of a merged row

Per-field provenance extends the existing `lineage_json.core_fact_alias_matches[<core_field>]` entries. Caveat verified against production data: the in-flight match info computes `fact_name`, `source_file_id`, and `parser_version` per field, but the persisted entries today keep `fact_name` and compatibility flags while dropping the source file reference (all sampled `source_file_id` values are null). This change therefore MUST ensure new writes persist `source`, `source_file_id`, and `role` (`official_structured` / `fallback_fill`) in those entries; readers take per-field authority from this structure and fall back to row-level semantics when the labels are absent (historical rows unchanged, not rewritten). A parallel provenance key is deliberately not introduced. The row-level `source_file_id` continues to identify the writing bundle. No schema migration: `facts_json`/`lineage_json` exist on `financial_facts`, `financial_core_facts_hot`, and `financial_core_facts_history`, and all three receive the same snapshot payload through the existing write path.

PIT ordering: `_append_financial_vintage` appends immutable point-in-time revisions before the latest-tier writes. The official-first preference MUST be resolved before any vintage append so PIT revisions record merged values; the vintage stage is currently disabled (report line `Filing vintage: disabled, blockers 1`), so this is a latent requirement that binds when it is re-enabled.

Row-level `publish_date`/`data_available_date` keep the writing (fallback) bundle's values, preserving today's availability semantics; data20 condensed endpoints carry no per-fact announcement date, so per-field availability refinement is deferred and out of scope. Consumers requiring point-in-time dates continue to use row-level dates as today.

### D3. Comparison semantics

Both sides are CNY-normalized numeric facts already. Comparison uses relative tolerance `source_fact_reconciliation.relative_tolerance` (default `0.005`) with absolute floor `absolute_floor_cny` (default `1.0`): equal if `abs(a-b) <= max(floor, tolerance * max(abs(a), abs(b)))`. Beyond tolerance → discrepancy record with both values and relative difference. The comparison scope starts with the five required core facts and is configurable; expanding it must not require code changes.

Single-sourced fill fields (no second source to compare, today `equity_parent`) additionally get a definitional plausibility check against same-row official facts: implied minority interest = `equity_total` − `equity_parent` must be non-negative and within a loose configurable share of `equity_total` (default `0.5`); violations are marked as review-only anomalies, reported, and do not block the write. Rationale: the only field without a second source should not be the only field without any quality check; minority interest can legitimately be large but rarely dominates total equity.

### D4. Comparison storage: run-scoped table, single-run retention

New table `financial_source_fact_comparison_results` (run_id, instrument_id, report_period, canonical_fact, record_kind, official_source, official_value, fallback_source, fallback_value, relative_diff, compared_at) where `record_kind` distinguishes `source_difference` from `sanity_review` (definitional sanity marks on single-sourced fill fields). At run start the runner deletes prior rows; at run end the table holds only the latest run's records plus counts in run metadata. This is process data, not durable acceptance evidence — durable evidence is the operator report and, when needed, an escalated issue.

### D5. Cleanup of fetched-but-unused fallback facts

Fallback numeric facts that were not used for field-level fill (comparison-only overlap fields) are deleted at the end of the run, identified by the fallback bundle's `source_file_id` minus the fill-used field set, reusing the existing per-source-file fact replacement path (`_replace_parsed_numeric_facts_for_source_file`). Deletion covers the physical tier table(s) the run actually wrote (hot tier; history only if the run wrote it) — same-run writes are process data, not immutable evidence. Persist-then-clean is chosen over never-persisting so the existing write path and in-run auditability are preserved.

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
