## 1. Baseline and audit

- [x] 1.1 Add a read-only audit command/report that inventories semantic receipt latest status, work-item status, candidate descendants, approved history, and occurrence collisions by instrument/report/source.
- [x] 1.2 Add runtime/source-revision smoke verification so the running application reports the code identity used for a backfill.
- [x] 1.3 Add fixtures from the production-shaped 002496.SZ activity replay and 300750.SZ anonymous relationship response.

## 2. Stable fact occurrence identity

- [x] 2.1 Implement one normalized occurrence-material builder for activities, operating facts, and relationships using report period, source revision, page/evidence, source row or contract, subject scope, and object.
- [x] 2.2 Make semantic conversion and governance temporal identity consume the same occurrence material and fail closed when required row/contract identity is absent.
- [x] 2.3 Ensure `reuse` reuses an identical occurrence, `replace` creates an explicit successor, and different report periods/contracts/rows remain independent.
- [x] 2.4 Add regression tests for approved-history plus force/reuse replay, same-row idempotence, distinct contract rows, new annual report, and corrected annual report.

## 3. Anonymous relationship semantics

- [x] 3.1 Extend the normalized relationship contract with an explicit ordinary-versus-concentration discriminator derived from the model response and source label.
- [x] 3.2 Accept masked ordinary contract counterparties without `disclosed_share`; require finite `disclosed_share` only for top-five concentration facts.
- [x] 3.3 Add regression tests covering `客户 A(1)`, `前五名客户`, and `前五名供应商` in one response, including mixed valid and invalid rows.

## 4. Error taxonomy and retry behavior

- [x] 4.1 Preserve stable reason codes and retryability from semantic conversion through runtime quality payload and worker queue.
- [x] 4.2 Remove string-based wrapping that turns deterministic business/schema/evidence failures into `gateway_failure`.
- [x] 4.3 Ensure only provider rate-limit, timeout, and transport failures receive gateway backoff; deterministic failures become machine rework without another LLM call.
- [x] 4.4 Add tests for anonymous validation, duplicate/missing verification IDs, evidence failures, unit failures, and real gateway failures with distinct queue dispositions.

## 5. Historical cleanup and lifecycle migration

- [x] 5.1 Define and implement the cleanup classification for rejected receipts, non-reusable conversion pending receipts, legacy identities, failed work items, and candidate descendants.
- [x] 5.2 Execute cleanup transactionally for the selected production scope, preserving approved records, evidence, and review audit; emit a deletion manifest with counts and IDs.
- [x] 5.3 Make replay lookup exclude deleted/unusable receipts and prevent old failed outputs from re-entering `reuse`.
- [x] 5.4 Add an explicit pre-batch gate that blocks broad backfill while identity collisions or unusable receipts remain unresolved.
- [x] 5.5 Make repair converge after legacy machine-approved role duplication by retaining one current role, auditing duplicate rows out of current reads, and ignoring already-held history.
- [x] 5.6 Isolate replay execution state by rotating orphan/stale checkpoints at enqueue and restricting targeted backfill workers to the current invocation's work IDs.
- [x] 5.7 Replace checkpoint quarantine-by-rotation with owned-file deletion; remove retired shadow lifecycle rows, superseded runs/work, and orphan checkpoints while preserving approved records, evidence, review audit, active current work, and current receipts.
- [x] 5.8 Make retired-shadow detection mode-based: update `_legacy_shadow_work` and `_obsolete_work_reason` to use persisted `rollout_phase=structured_shadow` or a canonical immutable `retirement_marker` (reason plus timestamp), never a field-family name alone; write the marker during the retirement migration and add a regression proving an active `structured_segments` or `tabular_operating_facts` work item in a non-retired phase is preserved.
- [x] 5.9 Extend retired-shadow cleanup to semantic runs and durable receipts: persist `processing_identity.rollout_phase` for all newly created runs/receipts, and during migration stamp an immutable `retirement_marker` on selected legacy runs/receipts (deriving candidates only from linked work/creation metadata or explicit audit input); use the same mode/marker predicate and prove replay lookup cannot return a retired run after its work row is removed.

## 6. End-to-end acceptance

- [x] 6.1 Deploy code and verify runtime source revision before invoking LLM work.
- [ ] 6.2 Run targeted no-broad-retry replays for 002496.SZ and 300750.SZ and confirm zero temporal collision, zero anonymous-contract misclassification, and zero false gateway retry.
- [ ] 6.3 Validate one new annual report and one same-period correction report for append/successor behavior and approved-history preservation.
- [ ] 6.4 Run the 11-instrument batch only after all gates pass; confirm no terminal failures, no stale failed receipt reuse, and expected publication/reporting status.
- [x] 6.5 Review the full diff and focused test suite, then document remaining non-blocking backlog separately from acceptance failures.

## 7. Confirmed correctness defects

- [x] 7.0 修复或显式基线化当前 HEAD 上的存量红测试 `test_atomic_activity_rejects_unknown_fields_and_broad_roles` 与 `test_shadow_and_pilot_drills_execute_real_governed_components`；若暂不修复，必须记录可复现原因、豁免范围和解除条件，且不得将其计入本 change 的通过测试。

- [x] 7.1 Change `REPORT_FLOW` approved-as-of selection so report observation `valid_from/valid_to` remains metadata and cannot hide an approved annual fact after its publication date; add a production-shaped `002415.SZ` regression.
- [x] 7.2 Replace case-folded SI prefix parsing with complete-token, case-preserving parsing: test bare `m`=metre and `g`=gram, reject bare `M`/`G`/`k`, and test `mm`=millimetre, `mg`=milligram, `Mt`=megatonne, and `kt`=kilotonne conversions and dimensions.
- [x] 7.3 Carry table-header percent units through deterministic segment extraction, normalize all gross margins to fraction exactly once, and reject invalid values when reconciliation is not applicable.
- [x] 7.4 Include reported-margin disclosure precision in propagated reconciliation tolerance while retaining a bounded ceiling; add `35.249%` calculated versus `35%` disclosed and a true mismatch regression.
- [x] 7.5 Catch deterministic unknown-unit and numeric conversion failures per row/document, persist typed machine-rework diagnostics, continue independent rows, and prevent deterministic checkpoint crash loops.
- [x] 7.6 When gross-margin reconciliation is `not_applicable` because cost is missing, normalize and range-check the reported margin anyway; emit `publication_blocker` for out-of-range or unit-inconsistent values and prevent automatic promotion.
- [x] 7.7 Add a governed power-unit compatibility rule and regression matrix proving `MW`, `mw`, and `mW` all resolve to megawatt (`10^6 W`) while non-power SI compounds retain case-sensitive semantics.

## 8. Semantic contract and lineage corrections

- [x] 8.1 Choose and implement one reachable structured Chinese-language behavior: either re-raise language errors into the existing single-model repair flow or remove the dead repair branch and use row-level soft rejection with diagnostics; add an end-to-end functional test, not only a signature test.
- [x] 8.2 Make semantic source-value and source-unit fallback null-aware so explicit null source fields do not discard valid canonical fields.
- [x] 8.3 Reject contradictory `disclosed_share` fraction-plus-percent payloads or convert exactly once, and add tests for ordinary anonymous relationships and concentration relationships.
- [x] 8.4 Require current runtime/schema/prompt/catalog identities in semantic reuse and report a deterministic stale-reuse reason; verify `reuse` does not replay obsolete runs.
- [x] 8.5 Treat atomic unit-pending, incomplete identity, and missing evidence as family-blocking conditions; persist machine-rework targets and exclude them from automatic promotion and complete-family reuse.
- [x] 8.6 Write `source_activity_action` into exposure publication metadata and use the same field in predecessor lookup and collision repair; add `sells` versus `produces` same-commodity lineage tests.
- [x] 8.7 Make exposure collision repair recover `source_activity_action` from referenced facts for legacy publications; report `lineage_incomplete` when the fact is absent or action remains unknown, with a sells/produces supersession regression.

## 9. Worker, recovery, and selection reliability

- [x] 9.1 Protect human-held records in contract recovery; only automation-owned holds with explicit provenance may be changed automatically.
- [x] 9.2 Add lease heartbeat/renewal for long-running semantic, verify, and publish work, with tests proving an active item cannot be double-claimed after the initial lease expires.
- [x] 9.3 Contain claim/ack/fail and stage-level gather exceptions, return typed stage results, and include sibling-stage status and reason codes in the operation report.
- [x] 9.4 Change page selection to preserve explicit pages and table/heading anchors before context pages; emit dropped-anchor diagnostics and avoid silent final page-number truncation.
- [x] 9.5 Gate quarterly and half-year ingestion on explicit `period_basis`; add a disabled/blocked test proving no monthly interval is inferred when basis is absent.
- [x] 9.6 Remove the planner's no-op persisted-classification comparison or make the persisted classification authoritative, and add a focused test for the chosen behavior.
- [x] 9.7 Determine automation ownership from the latest hold audit (`reviewed_at`, `audit_id`, and reviewer prefix), preserving a later human hold even when an earlier system hold exists.
- [x] 9.8 Document the fail-closed policy for `0 < disclosed_share < 1` with a percent unit: emit deterministic machine rework rather than guessing between `0.5%` and fraction `0.5`.

## 10. Production migration and acceptance

- [x] 10.1 Run read-only scans for as-of visibility, unit anomalies (including legacy `mw`/`mW` values), stale reuse identities, pending rows, legacy exposure action lineage, held records and latest hold owners, retired runs/receipts, and lease-risk work before any LLM call. Evidence: `docs/development/business_profile_10_1_read_only_scan_20260831.md`.
- [x] 10.2 Apply cleanup only to confirmed non-reusable receipts/runs/work/candidates and orphan checkpoints, including mode/marker-qualified retired runs; preserve approved records, evidence, and review audits; emit a deletion manifest. Evidence: `docs/development/business_profile_10_2_cleanup_20260831.md`.
- [ ] 10.3 Re-run targeted `002415.SZ`, `002496.SZ`, and `300750.SZ` flows with `result_policy=reuse`; require zero temporal collisions, false gateway classifications, stale reuse, duplicate exposure lineage, and verify missing-cost margin blockers plus `mW` megawatt resolution.
- [ ] 10.4 Validate a new annual report and same-period correction report for append/successor behavior, approved-history visibility, and no accidental deletion.
- [ ] 10.5 Run the 11-instrument batch only after all gates pass; record LLM calls, retry counts, worker lease events, dropped anchors, publication gaps, and final statuses.
- [ ] 10.6 Review the implementation against the current HEAD, run focused and business-profile regression suites (including latest-human-hold, legacy-fact-action, missing-cost margin, sub-one-percent share, and power-alias cases), and update this change with evidence before archive.
- [ ] 10.7 Update the `Deferred Backlog Register` in `design.md` with the disposition of non-blocking findings (format checker, NFKC normalization, structured sync watermark, large-record pagination, approval supersession persistence, catalog monotonicity, source truncation, GBK handling, bookmark-title dead path, and legacy `BusinessProfileLLMClient`/`llm.py` null-validation path) before archive; each item must have an owner or explicit follow-up change.
