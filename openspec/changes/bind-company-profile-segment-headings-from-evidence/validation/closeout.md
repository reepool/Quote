## Result

- Validation ID: `manufacturing-materials-shadow-segment-heading-binding-20260911-a`
- Frozen scope: `000408.SZ:segment_financials-02`, physical page 194
- Provider: `semantic_extraction__scorpio_gemini` (`gemini-3.8-flash-high`)
- Execution path: user-authorized sandbox-external transfer to `scorpio.reepool.com`
- Provider-free proof SHA-256: `1e24a8d7f21337a46aa25e1444c9d6e86377ece2916d4ab994928bf05c032ace`
- External result SHA-256: `162e16139b6418f632d02dbfe3a42ee33c2540fd274377c516c10927825f8863`

The confirmed short-heading failure class is resolved. The frozen Evidence produced one complete local heading, `报告分部的财务信息`; the compact response schema omitted and forbade provider `dimension`. Scorpio returned HTTP 200 for extract and independent verify, the scope completed, and eight records were accepted. Normal segment rows use the exact complete Evidence heading, while the explicit `分部间抵销` row remains `dimension=adjustment`.

## Execution facts

| Call | Status | Latency | Input tokens | Output tokens | Diagnostics |
|---|---:|---:|---:|---:|---|
| extract | success | 80,815 ms | 3,118 | 25,224 | valid response; provider exceeded requested 20,000 output-token budget |
| verify | success | 16,137 ms | 6,578 | 4,790 | none |

The provider-side output-cap overrun remains visible as `provider_output_budget_exceeded` and `provider_output_budget_exceeded_valid_response`. It did not cause transport, parse, schema, or semantic failure and is not hidden by raising the configured budget or rerunning this scope.

The frozen contract carried a stale non-authoritative `expected_partition_count=2` assumption copied from the earlier attempt. The current scope has 37 numeric occurrences, below the frozen partition threshold of 40, so the external path correctly used one extract call plus one verify call. Provider-free tests separately prove dimension-free partition merge. This metadata mismatch does not alter the submitted Evidence, route, budgets, response, or heading-binding result and is not a reason to rerun.

## Boundary

This result closes only the unique complete segment-heading binding defect. The report-level status is `hold` because this validation intentionally executed one scope and therefore lacks the other five core chapters; it is not a new company-profile bundle or a report-usability decision. The historical twenty-report bundle, Gold, and prior validation artifacts remain unchanged.

`production_authorization=not_authorized` remains authoritative. Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, DCF, and the pending twenty-report replay remain closed.
