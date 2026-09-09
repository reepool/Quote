## 1. Corrected Evidence freeze

- [x] 1.1 Add a new immutable Evidence-plan revision for `000717.SZ` that replaces all unsupported checklist aliases with `material_input`, `counterparty_relationship`, and `business_regime` while preserving every other frozen input.
- [x] 1.2 Admit the archived failed plan, the two preserved preflight revisions, and the final corrected revision under the existing second-OOS manifest, with focused regression tests for the closed field contract and invariant plan diff.

## 2. Offline validation

- [x] 2.1 Run preparation-only and field-contract checks for all seven scopes; confirm no provider is called for an invalid field and the corrected plan is provider-ready.
- [x] 2.2 Confirm the prior failed run, three-model comparison, four-report authority, BaoSteel bundle, and production boundaries remain unchanged.

## 3. One formal completion run

- [x] 3.1 Perform and record an out-of-sandbox Scorpio/Gemini connectivity check without treating sandbox-only DNS failures as provider failures.
- [x] 3.2 Execute exactly one complete formal run with `gemini-3.8-flash`, a new run ID, extract budget 16,000, verify budget 12,000, 300-second deadline, and no targeted rerun or cross-run splice.

## 4. Evaluation and closure

- [x] 4.1 Preserve the immutable bundle or typed failure and produce a researcher-readable six-chapter assessment with accepted facts, legal-empty results, Evidence, caveats, blockers, and model traces.
- [x] 4.2 Confirm `production_authorization=not_authorized`, Stage 6 and all production consumers remain closed, and the result is not promoted beyond research review.
- [x] 4.3 Run focused tests, Ruff, strict OpenSpec validation, review only blocking defects, sync the delta spec, archive the change, and submit only this task's isolated files.
