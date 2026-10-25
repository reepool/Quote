## 1. Field-level fallback fill

- [ ] 1.1 Implement the per-field official-first source preference in `derive_financial_core_facts_from_numeric_facts` (numeric facts pool resolution), gated by `fallback_only_missing_or_ambiguous_facts` (`false` preserves today's alias-order resolution).
- [ ] 1.2 Extend `lineage_json.core_fact_alias_matches` entries with explicit `source` and `role` labels on write and expose them through the core fact read paths; confirm the preference resolves before any filing-vintage append.
- [ ] 1.3 Keep `financial_statement_maintenance_repair.py` readiness/fallback-target selection unchanged, with `run_local_core_dryrun` remaining a parameter pass-through (no merge logic in `scripts/dev_validation/`).
- [ ] 1.4 Unit tests: mixed-pool derivation prefers official values; fallback fills only fields the official source lacks; flag-off preserves legacy resolution; historical rows without labels read with row-level semantics.

## 2. Same-fetch source reconciliation

- [ ] 2.1 Implement the comparison service (official facts vs fallback facts, relative tolerance + absolute floor) returning discrepancy records and consistent/discrepancy counts; no network access inside the service.
- [ ] 2.2 Add `financial_source_fact_comparison_results` (run-scoped) with clear-prior-rows at run start; persist only the current run's discrepancy records.
- [ ] 2.3 Wire comparison into the incremental/reconciliation repair run using the already-fetched fallback bundle; gate via `source_fact_reconciliation.enabled`.
- [ ] 2.4 Add the definitional plausibility check for single-sourced fill fields (implied minority share for `equity_parent` vs official `equity_total`), marked review-only and non-blocking.
- [ ] 2.5 Unit tests: equal values, within-tolerance, beyond-tolerance, one-side-missing (skipped, not flagged), near-zero floor case, and implausible-minority sanity case.

## 3. Process-data cleanup

- [ ] 3.1 At run end, delete fallback numeric facts fetched only for comparison (fallback bundle `source_file_id` minus fill-used fields); keep fill-used facts.
- [ ] 3.2 Before enabling deletion, verify no existing consumer (`audit_financial_numeric_fact_coverage`, `live_audit_sina_ths_local_core`, `research_financial_l1_full_import`) fails without the comparison-only fallback facts; adjust audit expectations that merely count coverage.
- [ ] 3.3 Verify by test and by post-run SQL that no comparison-only fallback facts and no stale comparison rows remain.

## 4. Operator report

- [ ] 4.1 Split the `CNInfo待补字段` line into systematically missing fields (missing across most officially parsed targets) versus per-instrument missing fields with sample instruments.
- [ ] 4.2 Add the comparison summary line(s): compared field count, consistent count, discrepancy count, and top discrepancy samples (bounded by `report_top_n`).
- [ ] 4.3 Surface review-only sanity anomalies (e.g., implausible implied minority interest) in the report alongside comparison discrepancies.
- [ ] 4.4 Unit tests for the report renderings, including the zero-discrepancy case and the reconciliation-disabled case.

## 5. Verification and review

- [ ] 5.1 Run focused suites: financial disclosure incremental, statements sync, storage, scheduler report tests.
- [ ] 5.2 On the first live nightly run after rollout, confirm in the report and by SQL: a mixed-provenance row (4 official + 1 fallback field with `field_sources`), comparison counts present, zero leftover comparison-only facts, and prior-run comparison rows cleared.
- [ ] 5.3 Diff review of the change; update this change's tasks status and archive when accepted.
