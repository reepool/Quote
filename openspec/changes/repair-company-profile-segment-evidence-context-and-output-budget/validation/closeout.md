# External validation closeout

- Validation ID: `manufacturing-materials-shadow-segment-evidence-context-output-budget-20260911-a`
- Frozen scope: `000408.SZ:segment_financials-02`
- Execution path: `sandbox_external_authorized`
- Provider: `semantic_extraction__scorpio_gemini`
- Result: `retained_substantive_failure`

## Observed result

The request contained only physical page 194; unrelated adjacent page 195 was excluded. Scorpio returned HTTP 200 and one extract request completed without DNS, connect, timeout, or output-budget failure. The response still used the ambiguous dimension prefix `报告分部` instead of the exact source heading `报告分部的财务信息`. The local fail-closed validator rejected it with `candidate_schema_invalid`, so the scope produced zero accepted records and remained incomplete.

## Decision

The context-range fix is validated for the observed routing issue, but this validation does not resolve the complete-heading failure class. No cohort replay, historical bundle mutation, Gold change, production path, or Stage 6 action is authorized. A later change may address structured heading binding; it must not relax the validator or rerun this scope under this change.

`production_authorization=not_authorized` remains in force.
