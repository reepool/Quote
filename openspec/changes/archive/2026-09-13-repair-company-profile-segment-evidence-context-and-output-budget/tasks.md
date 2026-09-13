## 1. Provider-free implementation

- [x] 1.1 Add segment-only context-range logic that excludes arbitrary adjacent pages while preserving explicit continuation binding.
- [x] 1.2 Add compact exact-heading instructions to the segment partition request without changing the response schema or validator acceptance rules.
- [x] 1.3 Add focused regression tests for unrelated neighbors, explicit continuations, exact heading constraints, and unchanged non-segment behavior.

## 2. Preflight and validation

- [x] 2.1 Run focused tests, Ruff, strict OpenSpec validation, and source/hash checks before any provider request.
- [x] 2.2 Freeze the exact previously failing `000408.SZ:segment_financials-02` scope, new validation identity, unchanged Gemini route/budgets, and output boundary.

## 3. Sole external validation

- [x] 3.1 Run the frozen scope once through the authorized external Scorpio path; do not substitute models, rerun, or splice results.
- [x] 3.2 Persist and classify accepted rows, task completeness, output-budget diagnostics, and any remaining semantic blocker.

## 4. Closeout

- [x] 4.1 Decide whether the fix resolves this failure class; do not recompute the historical cohort readiness.
- [x] 4.2 Commit and push only this change; production remains `not_authorized` and Stage 6 remains closed.
