## 1. Matching policy

- [x] 1.1 Add deterministic special-event eligibility, normalized economic comparison, and role-aware trading-session date matching helpers.
- [x] 1.2 Add unit tests for unique matches, float noise, economic conflicts, date conflicts, ambiguity, and missing calendar evidence.

## 2. Governance integration

- [x] 2.1 Add a persisted-data-only TDX comparison stage that reports every match and non-match reason.
- [x] 2.2 Persist unique matches as resolved reviews with `approved_asymmetric` CNInfo/TDX lineage and refresh governance state.
- [x] 2.3 Expose the stage through the existing governance scope and scheduler argument/report surface without network or LLM work.

## 3. Backlog execution

- [x] 3.1 Add a reproducible validation script that compares the unresolved special-event backlog and emits a detailed JSON report.
- [x] 3.2 Run dry-run comparison, inspect every matched row, write approved matches, and verify unmatched events remain unchanged.

## 4. Verification

- [x] 4.1 Run focused tests, OpenSpec strict validation, database lineage checks, and governance-state counts.
- [x] 4.2 Review the complete uncommitted diff, fix confirmed defects, and document any rejected over-strict findings.
