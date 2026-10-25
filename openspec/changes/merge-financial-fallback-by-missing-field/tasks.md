## 1. Field-level fallback fill

- [ ] 1.1 Add per-target official facts map (canonical fact → value, source_file_id) collection in `financial_statement_maintenance_repair.py` from the post-CNInfo readiness view.
- [ ] 1.2 Extend the fallback run path (`run_local_core_dryrun` → `FinancialStatementsShadowSyncService`) to accept `official_facts` plus the `fallback_only_missing_or_ambiguous_facts` policy, and compose the core snapshot official-first per canonical field.
- [ ] 1.3 Record per-field provenance in `lineage_json.field_sources` on write and expose it on read (`financial_facts` / `financial_core_facts_hot` via storage read paths); rows without the key keep row-level semantics.
- [ ] 1.4 Consume the existing config flag; `false` preserves legacy whole-bundle writes; both branches covered by unit tests.

## 2. Same-fetch source reconciliation

- [ ] 2.1 Implement the comparison service (official facts vs fallback facts, relative tolerance + absolute floor) returning discrepancy records and consistent/discrepancy counts; no network access inside the service.
- [ ] 2.2 Add `financial_source_fact_comparison_results` (run-scoped) with clear-prior-rows at run start; persist only the current run's discrepancy records.
- [ ] 2.3 Wire comparison into the incremental/reconciliation repair run using the already-fetched fallback bundle; gate via `source_fact_reconciliation.enabled`.
- [ ] 2.4 Unit tests: equal values, within-tolerance, beyond-tolerance, one-side-missing (skipped, not flagged), near-zero floor case.

## 3. Process-data cleanup

- [ ] 3.1 At run end, delete fallback numeric facts fetched only for comparison (fallback bundle `source_file_id` minus fill-used fields); keep fill-used facts.
- [ ] 3.2 Verify by test and by post-run SQL that no comparison-only fallback facts and no stale comparison rows remain.

## 4. Operator report

- [ ] 4.1 Split the `CNInfo待补字段` line into systematically missing fields (missing across most officially parsed targets) versus per-instrument missing fields with sample instruments.
- [ ] 4.2 Add the comparison summary line(s): compared field count, consistent count, discrepancy count, and top discrepancy samples (bounded by `report_top_n`).
- [ ] 4.3 Unit tests for both report renderings, including the zero-discrepancy case.

## 5. Verification and review

- [ ] 5.1 Run focused suites: financial disclosure incremental, statements sync, storage, scheduler report tests.
- [ ] 5.2 On the first live nightly run after rollout, confirm in the report and by SQL: a mixed-provenance row (4 official + 1 fallback field with `field_sources`), comparison counts present, zero leftover comparison-only facts, and prior-run comparison rows cleared.
- [ ] 5.3 Diff review of the change; update this change's tasks status and archive when accepted.
