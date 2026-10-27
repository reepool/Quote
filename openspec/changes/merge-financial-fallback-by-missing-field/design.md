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
- Rewriting or re-stamping historical core rows; backfilling per-field source labels for existing rows.
- Any new data source, new endpoint, or change to the required-core-fact set.

## Decisions

### D1. Merge point: official-first merge on the fallback bundle core snapshot (as-built)

Implementation finding: the authoritative fallback core row is NOT built by `storage.derive_financial_core_facts_from_numeric_facts` — that derivation only runs on the official-payload write path (`_write_official_payload`). The fallback bundle path persists the provider-built `bundle.facts` snapshot verbatim via `upsert_financial_statement_bundle` → `upsert_financial_facts`, which is what replaces the earlier CNInfo-stamped row. The merge therefore lives in `FinancialStatementsShadowSyncService._write_fallback_bundles`: before persistence, `_merge_official_core_facts_into_snapshot` overrides bundle core fields with official values.

Field matching reuses the derivation vocabulary rather than a semantic join across catalogs: for each core field, the first official numeric fact whose `fact_name` appears in the configured core alias mapping (`_core_fact_alias_mapping()`, same alias order as derivation) and passes `describe_core_financial_fact_alias` compatibility wins. The merge iterates the field inventory derived from that alias mapping plus any existing provider lineage match keys — never a mirrored field list — so a core field added through `core_fact_alias_overrides` participates automatically. The semantic gate binds before source preference — an official fact with wrong semantics (e.g. `所有者权益`, total-owners-equity, under core field `equity`) can never fill the parent-attributable field; it is instead the reference fact for the single-sourced sanity check. (A direct canonical-semantic join between `_CANONICAL_SEMANTICS_V1` and the standard fact catalog does not work: the vocabularies disagree, e.g. core `revenue` semantic is `revenue` while catalog `revenue` semantic is `operating_revenue`.)

The repair module stays the policy owner: it reads `fallback_only_missing_or_ambiguous_facts`, maps official source profiles (`cninfo_data20` → source `cninfo`) through `OFFICIAL_FINANCIAL_SOURCE_PROFILES`, and passes `official_core_fill_sources` plus the reconciliation config through `run_local_core_dryrun` (parameter pass-through only, per the governance note below). Callers that omit the opt-in — the general shadow sync and dev-validation dry runs — keep today's whole-bundle behavior.

Alternatives considered: implementing the preference inside `derive_financial_core_facts_from_numeric_facts` (wrong point — that path's row is overwritten by the bundle snapshot immediately afterwards), and post-write recomposition of core rows (rejected — double writes and changelog noise).

Governance note: `run_local_core_dryrun` lives under `scripts/dev_validation/` but is already the production fallback entry; this change keeps it a parameter pass-through only. The preference, comparison, and cleanup logic SHALL be implemented in `research/` modules, not added to the script.

### D2. Provenance, PIT ordering, and availability semantics of a merged row

Per-field provenance persists through the bundle snapshot's existing `lineage_json.core_fact_alias_matches[<core_field>]` entries: overridden fields get `source`/`source_file_id`/`role: official_structured` from the official numeric fact; kept bundle fields get `role: fallback_fill` labelled with the writing bundle's manifest id. (Production rows previously dropped the source file reference in these entries — persistence of the labels is an explicit requirement of this change, verified by unit test.) The row-level `source_file_id` continues to identify the writing bundle. No schema migration: `facts_json`/`lineage_json` exist on `financial_facts`, `financial_core_facts_hot`, and `financial_core_facts_history`, and all three receive the same snapshot payload through the existing write path.

PIT ordering: `_append_financial_vintage` is invoked only on the official-payload write path (`_write_official_payload`), gated by the `financial_filing_vintages` rollout stage (currently disabled, report line `Filing vintage: disabled, blockers 1`); the fallback bundle path appends no vintage today. The merged final write happens on the fallback path, so when the vintage stage is re-enabled the fallback path MUST also capture or refresh the vintage revision for the instrument-period, otherwise PIT history diverges from the authoritative merged row. This change documents the constraint and does not enable the stage.

Row-level `publish_date`/`data_available_date` keep the writing (fallback) bundle's values, preserving today's availability semantics; data20 condensed endpoints carry no per-fact announcement date, so per-field availability refinement is deferred and out of scope. Consumers requiring point-in-time dates continue to use row-level dates as today.

### D3. Comparison semantics

Both sides are CNY-normalized numeric facts already. Comparison uses relative tolerance `source_fact_reconciliation.relative_tolerance` (default `0.005`) with absolute floor `absolute_floor_cny` (default `1.0`): equal if `abs(a-b) <= max(floor, tolerance * max(abs(a), abs(b)))`. Beyond tolerance → discrepancy record with both values and relative difference. The comparison scope starts with the five required core facts and is configurable; expanding it must not require code changes.

Single-sourced fill fields (no second source to compare, today `equity_parent`) additionally get a definitional plausibility check against the official same-period numeric facts for the same instrument (e.g. cninfo `所有者权益` as canonical `equity_total`; the core row itself has no total-equity column): implied minority interest = `equity_total` − `equity_parent` must stay within a loose configurable share of `equity_total` on the positive side (default `0.5`) and no further below than a configurable absolute floor on the negative side (`sanity_absolute_floor_cny`, default 1000 CNY). The floor absorbs the reference source's rounding granularity — data20 reports 万元 with two decimals while the fill carries exact yuan, so zero-minority companies show tiny negative implied interest (observed in the first production run: ~73 of 136 prompts were sub-100-yuan rounding noise). Violations are marked as review-only anomalies, reported, and do not block the write; genuinely review-worthy catches from the first run include equity-scale disagreements between sources (e.g. 600123.SH, −5.8亿 implied minority). Rationale: the only field without a second source should not be the only field without any quality check; minority interest can legitimately be large, and negative minority interest exists in real reporting, so both bounds stay review-only.

### D4. Comparison storage: run-scoped table, single-run retention

New table `financial_source_fact_comparison_results` (run_id, instrument_id, report_period, canonical_fact, record_kind, official_source, official_value, fallback_source, fallback_value, relative_diff, detail_json, compared_at) where `record_kind` distinguishes `source_difference` from `sanity_review` (definitional sanity marks on single-sourced fill fields). Clearing is owned at the maintenance-run boundary: `FinancialDisclosureIncrementalSyncService._run_targeted_import` clears the table once before its per-profile repair loop (so groups with different required core facts cannot wipe each other's records), and standalone `FinancialMaintenanceRepairRouter.repair_targets` invocations clear at the start of their fallback phase (`clear_source_fact_comparison`, default on) — one clear per run either way, because the fallback loop runs once per report period and each dry-run call constructs a fresh service instance (per-call clearing in the service would drop earlier exchanges' records mid-run). The sync service only inserts (upsert by primary key). At run end the table holds exactly the latest run's records plus counts in run metadata. This is process data, not durable acceptance evidence — durable evidence is the operator report and, when needed, an escalated issue.

Summary aggregation across every combining level — exchanges within a sync call, report periods within a repair run, and profiles within an incremental run — goes through one shared helper (`merge_reconciliation_summaries`) with additive counts, summed official presence counts, and samples bounded once by `report_top_n`; that single config knob is therefore the end-to-end report sample bound, and rendering takes what the summary carries. Per-instrument missing official fields reach the report as `cninfo_missing_required_core_fact_samples` (fact → bounded instrument ids), collected from the post-import readiness pass that already selects fallback targets (the previously duplicated second pass is removed), and rendered on the incidental line with sample instruments.

### D5. Cleanup of fetched-but-unused fallback facts

Fallback numeric facts that were not used for field-level fill (comparison-only overlap fields) are deleted at the end of the run, identified by the fallback bundle's `source_file_id` minus the fill-used field set, reusing the existing per-source-file fact replacement path (`_replace_parsed_numeric_facts_for_source_file`). Deletion covers the physical tier table(s) the run actually wrote (hot tier; history only if the run wrote it) — same-run writes are process data, not immutable evidence. Persist-then-clean is chosen over never-persisting so the existing write path and in-run auditability are preserved.

Compatibility constraint: `financial_numeric_facts_hot` has other readers (`scripts/dev_validation/audit_financial_numeric_fact_coverage.py`, `scripts/dev_validation/live_audit_sina_ths_local_core.py`, `scripts/research_financial_l1_full_import.py`). Before enabling deletion, the change SHALL verify none of these consumers asserts the presence of fallback numeric facts for filled targets; audit tools that merely report coverage counts are expected to reflect the new retention policy rather than fail.

### D6. Flag consumption and rollback

`fallback_only_missing_or_ambiguous_facts` (already declared, default `true`) selects field-level fill; `false` restores whole-bundle fallback writes (today's behavior). `source_fact_reconciliation.enabled` (default `true`) independently gates comparison; when disabled, no comparison, no extra table writes, and cleanup still removes nothing beyond today's behavior.

### D7. Standalone comparability

The comparison is implemented as a pure service function over (official facts, fallback facts, tolerance) returning records + counts. The incremental run calls it inline with the already-fetched bundle; any future standalone audit can call it with caller-supplied fact sets (note that after in-run cleanup the stored tiers no longer retain comparison-only fallback facts, so a standalone audit comparing all overlap fields would fetch its own bundle).

## Risks / Trade-offs

- Discrepancies may be noisy at first (real source disagreements surface). Mitigation: report aggregates counts and shows top N samples (configurable `report_top_n`, default 5); discrepancies do not fail the run — they are marked and reported.
- The source preference is implemented in `derive_financial_core_facts_from_numeric_facts`, whose only business caller is `FinancialStatementsShadowSyncService` (verified; the L1 full import builds core facts through its own path). The service itself serves both the disclosure fallback path and the general statements shadow sync, so the preference is an explicit opt-in passed by the disclosure incremental/reconciliation path; the general shadow sync and dev-validation dry-run callers keep today's alias-order resolution until separately validated.
- Extra deletes add write volume per run; bounded by the run's fallback target count (hundreds of instruments), acceptable for SQLite in the nightly window.

## Migration / Rollout

- Additive table + additive lineage keys; no data migration, no rewrite of existing rows.
- Rollout: flag-guarded, default-on for fill policy (matches the already-declared config intent), reconciliation on by default; both disable-able.
- Acceptance evidence: focused unit suites plus the first live nightly run's report showing a mixed row (4 official + 1 fallback field), comparison counts, and zero leftover comparison-only facts (verified by SQL count after the run).
