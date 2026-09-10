# Precision-closure replay execution result

## Decision

The sole authorized replay `manufacturing-materials-shadow-precision-closure-gemini-20260910-a` is `failed`. The failure is an execution-environment result, not a semantic or Evidence result: the sandboxed Python/httpx path returned two consecutive `dns_failure` transport errors at the frozen 300-second deadline, while the earlier read-only curl preflight had reached Scorpio successfully.

No report completed, so no source-review package, readiness audit, precision comparison, or production-readiness conclusion can be generated from this attempt. The empty batch directory and typed diagnostics are retained; the identity will not be reused.

## Observed diagnostics

- Request `caa6385e8f334a7eb813b8652325c2a2`: `transient_transport_error`, `dns_failure`, connect phase, `ConnectError`, then `deadline_exceeded` after 300,105 ms.
- Request `f9ca846e7d5f440f9a61b6b0b0a4aec3`: the same typed path after 300,065 ms.
- A third request was admitted and then stopped once the repeated sandbox-network invariant was established.
- Persisted reports: 0/20; manifest/review/readiness/comparison artifacts: none.

## Boundaries preserved

- No second batch or targeted run occurred in this change.
- No model, Evidence, prompt, token/output/deadline/call budget, or semantic rule changed.
- No historical batch or closure audit changed.
- `production_authorization=not_authorized`; Stage 6 and every production/downstream path remain closed.

## Next minimal action

Open a separate operational validation change with a new immutable batch ID and run the same frozen full cohort directly through the user-authorized external-sandbox network path. The external execution path is the only intended change; the model, Evidence, budgets, review rule, and readiness gates remain identical.
