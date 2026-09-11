# External live validation closeout

- Validation ID: manufacturing-materials-shadow-segment-normalization-external-live-20260911-b
- Execution path: sandbox_external_authorized
- Target: scorpio.reepool.com
- Provider: semantic_extraction__scorpio_gemini (gemini-3.8-flash-high)
- Result SHA-256: dad5dc72493528e99a42289d756eebee5075e6804cd0d777a773ff81fcdb3ac2
- Predecessor sandbox result SHA-256: a486e45f08b58111713b484d1ec8552851b2b414b5e061504e0fd7f52f1cfdaf

## Outcome

Six provider calls returned upstream HTTP 200 responses. The annual-period-conflict case resolved with 44 accepted source-bound records and complete scope coverage. The complete-report-segment-heading case remains a substantive failure: the first extract exceeded the configured output budget (the response was valid but warned), the second partition required repair, and the assembled scope still lacked segment_dimension, operating_revenue, and operating_cost; no records were accepted and the scope is incomplete.

The normalization is therefore only partially empirically validated. The result does not justify a twenty-report replay yet; the incomplete-heading failure needs a separate, minimal Evidence/request-shaping investigation before any cohort run. The historical batch, predecessor result, and readiness state were not modified or recomputed. production_authorization remains not_authorized.

This is a research validation artifact only. It does not authorize Stage 6, approved tables, scheduler/backfill, commodity exposure, value-chain publication, or DCF.
