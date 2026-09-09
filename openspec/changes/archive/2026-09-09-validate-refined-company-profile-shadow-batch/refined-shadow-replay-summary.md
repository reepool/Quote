# Refined shadow replay empirical summary

## Decision

The single authorized replay `manufacturing-materials-shadow-refined-gemini-20260910-a` is complete and remains `hold`. The result does **not** justify a restricted-production promotion design yet. All records remain `accepted_for_review`, and `production_authorization=not_authorized` remains unchanged.

## Immutable result

- Frozen cohort: 20 reports; same manifest hash as v1.
- Persisted reports: 20/20; execution-completed reports: 15/20.
- Report status: 15 `hold`, 5 `failed`; usable rate: 0%.
- Accepted research facts: 1,348; Evidence traceability: 100%.
- Provider calls: 323; failed calls: 9.
- Unresolved review workload: median 4.5, p90 11.
- Contracted source review: 106/106 rows reviewed; 81 correct, 23 noncritical errors, 2 critical errors; sampled precision 76.42%.

## What v2 improved

Compared with `manufacturing-materials-shadow-gemini-20260909-a`, the refined Evidence plan reduced mechanical semantic burden:

- `required_coverage_missing`: 127 -> 84 (-43).
- `candidate_schema_invalid`: 13 -> 4 (-9).
- Total review-package rows: 308 -> 262 (-46).
- Unresolved review median/p90: 7.5/15 -> 4.5/11.
- Accepted records: 1,310 -> 1,348 (+38).
- Input tokens: 2,001,081 -> 1,958,156 (-42,925).

These are real improvements, but they did not produce a usable report or satisfy a readiness gate that would permit promotion design.

## Residual blockers

### Evidence and coverage correctness

Of the 23 noncritical source-review errors, 19 are wrong or insufficient chapter-page selections, three use a principal-business statistical-calibre checkbox as a broader business-regime conclusion, and one over-structures generic `材料` cost as a named material input. The most common bad selections are financial-statement notes used for business overview/regime/segment/counterparty conclusions.

Two critical errors remain: the 600256 and 000408 Evidence explicitly states a consolidation-scope change, while runtime closes `business_regime` as `not_applicable`. This is source contradiction, not a formatting issue.

### Execution and provider contract

- Execution completion fell from 90% to 75%; failed reports increased from two to five.
- Provider failures fell from 16 to 9, but the batch still recorded five `provider_unavailable` calls and four terminal schema failures.
- Five successful extract responses exceeded the requested 20,000-token output budget; the largest reported 40,673 output tokens.
- Five calls took at least 100 seconds, two at least 150 seconds, and the maximum observed latency was 259,482 ms.
- Some parse/schema failures were followed by unsuccessful provider responses on the repair path, so the repair request contract still requires isolated verification before another full batch.

## Gate result

Passed:

- cohort/preparation complete;
- accepted-record Evidence traceability 100%;
- source review complete.

Failed:

- execution completion >=95%;
- usable reports >=90%;
- sampled precision >=99%;
- zero critical semantic errors;
- median unresolved review <=2;
- p90 unresolved review <=5.

## Next minimal sequence

1. Fix provider-free Evidence routing and the two business-regime contradiction paths. Add closed assertions that overview, segment, counterparty, material, and regime scopes are owned by the correct disclosure sections; a statistical-calibre checkbox must not close business/control regime, and an applicable consolidation change must prevent `not_applicable`.
2. Separately verify the repair protocol and output-budget behavior with bounded fixtures/requests. Do not launch another twenty-report batch until HTTP/provider rejection and oversized segment responses have a demonstrated mitigation (including scope splitting only where a real oversized scope requires it).
3. Run one new controlled cohort replay only after steps 1 and 2 pass provider-free and focused integration checks. Recompute the same source review and readiness gates; do not tune after seeing that run.

This change validates v2 honestly; it does not open Stage 6, approved writers, scheduler/backfill, commodity exposure, value-chain publication, or DCF.
