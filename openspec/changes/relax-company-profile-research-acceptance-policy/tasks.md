## 1. Policy contract and closed tables

- [ ] 1.1 Freeze the six core chapter tasks, report usability states, and the precedence rule that required execution failures and §14 blockers outrank caveats.
- [ ] 1.2 Define the closed `metric/object × subject_scope × subject_basis` usage table for research display, consolidated aggregation, ranking, and cross-company comparison.
- [ ] 1.3 Define deterministic confidence derivation from existing Evidence, verify/disposition, subject basis, and contradiction fields; prohibit LLM-supplied probability and free-form usage lists.
- [ ] 1.4 Add `subject_strictness` metadata to the 24 existing Gold annotations without changing their source-native values, expected subjects, or metric expectations.
- [ ] 1.5 Define the closed Gold normalization/equivalence table and `gold_contract_conflict` result semantics, including numeric punctuation and approved unit pairs only.

## 2. Benchmark and status implementation

- [ ] 2.1 Implement report-level usability aggregation so `unclear` alone is non-blocking while incomplete core chapters, execution failures, illegal subject promotion, and frozen §14 blockers remain blocking.
- [ ] 2.2 Implement projection enforcement for the closed usage table and expose an explicit restriction reason when an unclear consolidated-sensitive fact is withheld from aggregation or comparison.
- [ ] 2.3 Implement finite Gold matching for numeric normalization, approved unit equivalence, metric/object/physical-anchor alignment, subject strictness, and contract conflicts.
- [ ] 2.4 Split negative-case evaluation into fixture-guard results and real-report results; keep untriggered real cases `evaluated=false` and exclude them from the failure count.
- [ ] 2.5 Add report `usable` / `usable_with_caveats` / `hold` / `failed` and overall `research_slice_usable` states without redefining historical `research_slice_pass` or production authorization.

## 3. Fixture tests and regression coverage

- [ ] 3.1 Add fixture guards for inventory amount versus quantity, omitted required page, unreadable page, and ambiguous unit, proving the existing blockers still fire before provider invocation.
- [ ] 3.2 Add projection tests for unclear Activity availability, unclear consolidated-sensitive Measurement restriction, business-segment preservation, and explicit/reconciled group use.
- [ ] 3.3 Add Gold matcher tests for punctuation, closed unit conversions, subject strictness, physical-anchor separation, and `gold_contract_conflict`.
- [ ] 3.4 Add report-state tests proving a timed-out required chapter remains `hold`/`failed`, while complete chapters with only allowed subject caveats can become `usable_with_caveats`.
- [ ] 3.5 Add regression tests proving all records remain `accepted_for_review`, `production_authorization=not_authorized`, and no legacy production path is touched.

## 4. One post-policy validation run

- [ ] 4.1 Validate the OpenSpec change strictly and run the related offline test suite before any provider call.
- [ ] 4.2 Execute exactly one complete four-report semantic slice with a new run ID after the policy implementation; do not mix run-x, run-y, or historical Gold values into runtime.
- [ ] 4.3 Generate the post-run Gold, fixture-guard, and real-report negative reports from the committed bundle and retain all failed, conflicted, and unevaluated items.
- [ ] 4.4 Derive and publish report usability and overall `research_slice_usable` status with researcher-readable profiles and explicit caveats.
- [ ] 4.5 Perform isolated-output garbage audit, verify production non-authorization and legacy-path non-invocation, review the diff, and prepare the change for user acceptance.
