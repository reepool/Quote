## 1. Provider-boundary completeness

- [x] 1.1 Derive active non-optional field identities from the existing checklist without changing request or record models.
- [x] 1.2 Add candidate-or-legal-empty completeness constraints to the compact segment, quantity, material-input, counterparty, regime, and generic extract schemas.
- [x] 1.3 Preserve optional omission and ensure semantic/Evidence rejection remains outside provider failover.

## 2. Regression verification

- [x] 2.1 Add provider-free tests for empty and partial required responses across the affected schema families.
- [x] 2.2 Add tests proving legal-empty coverage and optional omission remain valid and that the common gateway performs bounded fallback on the resulting schema error.
- [x] 2.3 Run focused tests, Ruff, compilation, and strict OpenSpec validation.

## 3. Small unseen canary

- [x] 3.1 Freeze one previously unseen local-valid manufacturing/materials annual report and its existing six-chapter Evidence plan; record identity, PDF hash, and limitations before provider I/O.
- [x] 3.2 Reject the stale Gemini-only twenty-report replay before provider creation and retain its historical artifacts unchanged.
- [x] 3.3 Execute the unseen report once with a new run ID through the current four-member logical pool; preserve typed failure or report-local success without targeted reruns.
- [x] 3.4 Publish a concise source-bound canary audit and retain `production_authorization=not_authorized`, Stage 6, approved writers, scheduler/backfill, commodity exposure, value-chain publication, and DCF as closed.

## 4. Close and hand off

- [x] 4.1 Review only blocking defects, verify immutable inputs/outputs and Git isolation, then commit and push the completed change without touching pre-existing workspace files.
