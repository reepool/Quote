## 1. Freeze validation inputs

- [x] 1.1 Record the source batch/report hashes, exact two scope identities, implementation hashes, Gemini profile, budgets, sole validation ID, output path, and research-only boundary.
- [x] 1.2 Add a change-local thin operator that validates every frozen input before provider creation and delegates each scope to the existing Stage 5 application service.

## 2. Provider-free verification

- [x] 2.1 Add focused tests for exact admission, hash/scope/budget drift, output reuse, per-case failure preservation, and unchanged historical inputs.
- [x] 2.2 Run focused tests, Ruff, strict OpenSpec validation, and source-batch hash checks before any Scorpio request.

## 3. Sole live validation

- [x] 3.1 Run one read-only Gemini profile preflight, repeating outside the sandbox only for a sandbox DNS/network-permission failure.
- [x] 3.2 Execute the two frozen scopes once under the sole validation identity and shared 12-call ceiling; do not tune, substitute, or rerun.

## 4. Closeout

- [x] 4.1 Persist the source-bound result and classify each case as resolved, retained substantive failure, or execution failure without recomputing batch readiness.
- [ ] 4.2 Record whether a later full-cohort replay is justified, retain `production_authorization=not_authorized`, verify isolated artifacts, and commit/push only this change.
