## 1. Field-level fallback fill

- [x] 1.1 Implement the per-field official-first source preference as an opt-in parameter of `derive_financial_core_facts_from_numeric_facts` (numeric facts pool resolution), passed by the disclosure incremental/reconciliation path and gated by `fallback_only_missing_or_ambiguous_facts` (`false`, or absent callers, preserve today's alias-order resolution).
- [x] 1.2 Ensure `lineage_json.core_fact_alias_matches` entries persist `source`, `source_file_id`, and `role` on write (production rows today drop the source file reference) and expose them through the core fact read paths; leave the filing-vintage stage untouched (the latent PIT constraint is documented in design D2 and binds when the stage is enabled).
- [x] 1.3 Keep `financial_statement_maintenance_repair.py` readiness/fallback-target selection unchanged, with `run_local_core_dryrun` remaining a parameter pass-through (no merge logic in `scripts/dev_validation/`).
- [x] 1.4 Unit tests: mixed-pool derivation prefers official values; fallback fills only fields the official source lacks; flag-off preserves legacy resolution; historical rows without labels read with row-level semantics.

## 2. Same-fetch source reconciliation

- [x] 2.1 Implement the comparison service (official facts vs fallback facts, relative tolerance + absolute floor) returning discrepancy records and consistent/discrepancy counts; no network access inside the service.
- [x] 2.2 Add `financial_source_fact_comparison_results` (run-scoped) with clear-prior-rows at run start; persist only the current run's discrepancy records.
- [x] 2.3 Wire comparison into the incremental/reconciliation repair run using the already-fetched fallback bundle; gate via `source_fact_reconciliation.enabled`.
- [x] 2.4 Add the definitional plausibility check for single-sourced fill fields (implied minority share for `equity_parent` vs official `equity_total`), marked review-only and non-blocking.
- [x] 2.5 Unit tests: equal values, within-tolerance, beyond-tolerance, one-side-missing (skipped, not flagged), near-zero floor case, and implausible-minority sanity case.

## 3. Process-data cleanup

- [x] 3.1 At run end, delete fallback numeric facts fetched only for comparison (fallback bundle `source_file_id` minus fill-used fields); keep fill-used facts.
- [x] 3.2 Before enabling deletion, verify no existing consumer (`audit_financial_numeric_fact_coverage`, `live_audit_sina_ths_local_core`, `research_financial_l1_full_import`) fails without the comparison-only fallback facts; adjust audit expectations that merely count coverage.
- [x] 3.3 Verify by test and by post-run SQL that no comparison-only fallback facts and no stale comparison rows remain.

## 4. Operator report

- [x] 4.1 Split the `CNInfo待补字段` line into systematically missing fields (missing across most officially parsed targets) versus per-instrument missing fields with sample instruments.
- [x] 4.2 Add the comparison summary line(s): compared field count, consistent count, discrepancy count, and top discrepancy samples (bounded by `report_top_n`).
- [x] 4.3 Surface review-only sanity anomalies (e.g., implausible implied minority interest) in the report alongside comparison discrepancies.
- [x] 4.4 Unit tests for the report renderings, including the zero-discrepancy case and the reconciliation-disabled case.

## 5. Verification and review

- [x] 5.1 Run focused suites: financial disclosure incremental, statements sync, storage, scheduler report tests.
- [x] 5.2 On the first live nightly run after rollout, confirm in the report and by SQL: a mixed-provenance row (four official fields plus one fallback-filled field, with `source`/`source_file_id`/`role` persisted in `core_fact_alias_matches`), comparison counts present, zero leftover comparison-only facts, and prior-run comparison rows cleared.
- [x] 5.3 Diff review of the change; update this change's tasks status and archive when accepted.

Production acceptance (2026-10-26/27 runs): 字段级补缺 merged 3195 official fields and 5732 fallback fills across 639 targets; 来源对照 compared 2556 facts with 2556 consistent and 0 differences; SQL verification recorded 4190 `official_structured` and 7641 `fallback_fill` role labels (11,831 entries carrying `source_file_id`, previously always null), zero comparison-only leftover facts, and single-run table retention working. The 136 first-night sanity prompts were adjudicated — all satisfied the accounting identity, none indicated a source error — and the check was upgraded to identity-first with an absolute rounding floor (residual review prompts: 0).

## 6. Review follow-ups (2026-10-25)

- [x] 6.1 Report per-instrument missing official fields with sample instruments: `cninfo_missing_required_core_fact_samples` (fact → instrument ids) collected from the post-import readiness pass on the incidental line; the duplicated second readiness pass is removed.
- [x] 6.2 Make `report_top_n` the single end-to-end bound for comparison samples (summary, every aggregation level, rendering) instead of the mixed 5/10/3 caps.
- [x] 6.3 Derive the merge field inventory from the configured core alias mapping ∪ lineage match keys; delete the mirrored `_CORE_FILL_FIELD_NAMES` tuple so config-added core fields participate automatically.
- [x] 6.4 Aggregate reconciliation summaries through one shared `merge_reconciliation_summaries` helper across exchange-, period-, and profile-level merging; multi-profile incremental runs no longer drop later profiles' `source_fact_reconciliation`/`official_core_fill`.
- [x] 6.5 Clear the comparison table once per maintenance run at the incremental service's run boundary (`clear_source_fact_comparison` opt-out on the router), so per-profile repair invocations cannot wipe earlier groups' records when profiles require different core facts.
