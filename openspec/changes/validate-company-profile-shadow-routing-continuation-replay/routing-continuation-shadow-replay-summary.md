# Routing/continuation shadow replay result

## Decision

The sole immutable v5 replay closes as `hold`. All twenty reports executed and all
accepted research facts remain source-traceable, but the replay does not meet the
frozen usability, sampled-precision, or human-review-workload gates.
`production_authorization=not_authorized` remains unchanged.

This result does not authorize Stage 6, approved-table writes, scheduler/backfill,
commodity exposure, value-chain publication, or DCF use.

## Frozen execution

- Batch: `manufacturing-materials-shadow-routing-continuation-gemini-20260910-a`
- Evidence plan: `manufacturing_materials_shadow.2026-09-10.5`
- Model profile: `semantic_extraction__scorpio_gemini`
- Reports persisted and execution-complete: 20/20
- Report statuses: 20 `hold`, 0 usable, 0 failed
- Accepted records: 672
- Accepted-record Evidence traceability: 100%
- Provider traces: 296 total; 260 successful and 36 failed
- Call types: 203 extract and 93 verify; all 36 failed traces are bounded
  `candidate_schema_invalid` extract responses
- No DNS, deadline, or provider-unavailable failure occurred
- No second replay, targeted rerun, parameter tuning, model substitution, or result
  splice was performed

## Source review and failed gates

The complete review package contains 426 rows: 72 blockers, 273 unresolved rows, and
81 chapter precision samples. Source review classified 424 rows as correct, two as
noncritical errors, and zero as critical errors. The sampled precision is 79/81, or
97.5309%.

| Gate | Required | Actual | Result |
| --- | ---: | ---: | --- |
| Execution completion | at least 95% | 100% | pass |
| Usable reports | at least 90% | 0% | fail |
| Evidence traceability | 100% | 100% | pass |
| Source review complete | complete | complete | pass |
| Sampled precision | at least 99% | 79/81 = 97.5309% | fail |
| Critical semantic errors | 0 | 0 | pass |
| Unresolved review median | at most 2 | 14 | fail |
| Unresolved review p90 | at most 5 | 19 | fail |

The two noncritical source-bound errors are both in `920016.BJ`:

1. Physical pages 56-57 contain industry-policy discussion rather than a segment or
   revenue/cost owner, so they do not support
   `segment_dimension=not_disclosed`.
2. Physical page 15 explicitly states that the principal business did not change;
   `business_regime=not_disclosed` understates that source-supported legal-empty
   conclusion and should be `not_applicable`.

## Routing/continuation recurrence

- `000055.SZ`: the rejected subsidiary financial-table page 36 is absent. Issuer page
  24 is retained and produces source-bound BusinessOverview records.
- `920016.BJ`: the rejected “部分产品” page 14 is absent and valid segment-owner pages
  19-21 are retained. Their extract did not complete, while a separate industry-policy
  scope on pages 56-57 produced the noncritical owner error above. The exact case is
  closed, but the segment-owner defect family recurred.
- `920076.BJ`: pages 15-17 remain in one material-input scope and the continued wording
  `役后耐火材料` is accepted.
- `920033.BJ`: the physical-page-72 explicit material legal empty remains complete.

The previously closed high-risk semantic errors did not recur: there is no
contradicted `business_regime=not_applicable`, top-five aggregate Relationship,
cost-component Segment, unsupported `consolidated_group` promotion, or generic
`材料`/`原材料` Relationship in the 672 accepted records.

## Comparison with owner-closure replay

| Metric | Owner-closure baseline | v5 replay | Delta |
| --- | ---: | ---: | ---: |
| Execution completion | 100% | 100% | unchanged |
| Usable report rate | 0% | 0% | unchanged |
| Accepted records | 809 | 672 | -137 |
| Sampled precision | 97.5% | 97.5309% | +0.0309 percentage points |
| Critical semantic errors | 0 | 0 | unchanged |
| Unresolved median | 12.5 | 14 | +1.5 |
| Unresolved p90 | 20 | 19 | -1 |
| Provider calls | 299 | 296 | -3 |
| Failed traces | 39 | 36 | -3 |
| Input tokens | 1,576,493 | 1,533,172 | -43,321 |
| Output tokens | 1,304,490 | 1,083,570 | -220,920 |
| Total latency | 5,555,135 ms | 4,374,969 ms | -1,180,166 ms |

The v5 plan reduces output volume and latency and preserves zero critical errors, but
it does not improve report usability and increases median review workload. It is not a
production-readiness pass.

## Provider observations

Seven successful responses reported output beyond their frozen call-type limit; the
maximum was 27,111 tokens. Maximum recorded latency was 176,348 ms. These are future
cost and tail-latency observations only and did not cause a report execution failure.
They do not authorize parameter tuning or another replay in this change.

## Next bounded blocker

The next source-bound business blocker is segment-financial completion. It accounts
for 30 of 72 report-local blockers and 135 of 273 unresolved review rows; within the
runtime findings, segment financials contribute 107 `required_coverage_missing` and
28 `candidate_schema_invalid` reasons. A later bounded change should close the
non-owner legal-empty fallback and the schema-invalid completion failure on governed
segment/revenue-cost scopes before considering another cohort replay.

Production remains closed. This validation is complete as an honest `hold` and does
not itself authorize that follow-up implementation or another provider-bearing run.
