# Owner-closure shadow replay result

## Decision

The sole immutable replay closes as `hold`. It improves execution completion and sampled semantic precision, but it does not meet the frozen scale-readiness gates. `production_authorization=not_authorized` remains unchanged.

This result does not authorize Stage 6, approved-table writes, scheduler/backfill, commodity exposure, value-chain publication, or DCF use.

## Frozen execution

- Batch: `manufacturing-materials-shadow-owner-closure-gemini-20260910-a`
- Evidence plan: `manufacturing_materials_shadow.2026-09-10.4`
- Model profile: `semantic_extraction__scorpio_gemini`
- Reports persisted: 20/20
- Report execution failures: 0
- Report statuses: 20 `hold`, 0 usable
- Accepted records: 809
- Accepted-record Evidence traceability: 100%
- Provider traces: 299 total, 260 successful and 39 failed extract responses
- Typed failed responses: 39 `candidate_schema_invalid` after gateway responses; the affected scopes stopped without a repair call
- Workflow call types: 155 extract, 95 verify, 0 repair
- No DNS, deadline, or provider-unavailable failure occurred
- No second replay, targeted rerun, tuning, or historical-result splice was performed

## Source review and failed gates

The frozen review package contains 404 rows: 71 blockers, 253 unresolved rows, and 80 chapter precision samples. Source review classified 402 rows as correct, 2 as noncritical errors, and 0 as critical errors.

| Gate | Required | Actual | Result |
| --- | ---: | ---: | --- |
| Execution completion | at least 95% | 100% | pass |
| Usable reports | at least 90% | 0% | fail |
| Evidence traceability | 100% | 100% | pass |
| Source review complete | complete | complete | pass |
| Sampled precision | at least 99% | 78/80 = 97.5% | fail |
| Critical semantic errors | 0 | 0 | pass |
| Unresolved review median | at most 2 | 12.5 | fail |
| Unresolved review p90 | at most 5 | 20 | fail |

The two source-bound noncritical errors are:

1. `920016.BJ`: `segment_dimension=not_disclosed` is bound to physical page 14, a management-plan/production page, rather than a segment or revenue/cost owner.
2. `920076.BJ`: `material_input=not_disclosed` on physical page 15 misses the adjacent continuation disclosure of consumed refractory materials, including `役后耐火材料`.

## Owner/substance recurrence

All seven exact frozen rejected page/field assignments are absent from the regenerated provider-free plan, and the `920033.BJ` physical-page-72 material applicability control remains present. Empirical replay nevertheless found one defect family recurring on a different page:

- `000055.SZ` no longer uses the rejected cross-reference-only physical page 24 for `BusinessOverview`, but the regenerated plan selected physical page 36, a subsidiary financial table. The model produced no accepted `BusinessOverview`; the required overview scope remained incomplete and the report correctly stayed `hold`.

Therefore exact-case recurrence is 0/7, while defect-family recurrence is 1/7 (`business_overview_substance`). The other six frozen defect families did not recur. The `920033.BJ` material legal-empty record is a valid positive control, not an error.

## Comparison with corrected external baseline

| Metric | Corrected baseline | Owner-closure replay | Delta |
| --- | ---: | ---: | ---: |
| Execution completion | 95% | 100% | +5 percentage points |
| Usable report rate | 0% | 0% | unchanged |
| Accepted records | 783 | 809 | +26 |
| Sampled precision | 91.5663% | 97.5% | +5.9337 percentage points |
| Critical semantic errors | 0 | 0 | unchanged |
| Unresolved median | 11 | 12.5 | +1.5 |
| Unresolved p90 | 17 | 20 | +3 |
| Provider calls | 319 | 299 | -20 |
| Failed traces | 33 | 39 | +6 |
| Input tokens | 1,670,605 | 1,576,493 | -94,112 |
| Output tokens | 1,277,997 | 1,304,490 | +26,493 |
| Total latency | 4,802,909 ms | 5,555,135 ms | +752,226 ms |

The comparison uses the already validated corrected-baseline metric snapshot because the current stricter `BusinessOverview` validator intentionally cannot reload the historical invalid cross-reference record. Neither historical bundle is mutated.

## Provider observations

The replay used the frozen extract/verify limits of 20,000/18,000 output tokens and a 300-second request deadline. Eleven successful responses reported more than 20,000 output tokens; the largest reported output was 28,784 tokens. Under the existing audit convention that treats local failed traces with no provider latency as zero, recorded latency p50/p90/p95/p99/max was 10,119 / 51,976 / 81,093 / 120,108 / 209,883 ms.

These are material future cost and tail-latency risks. They are findings from this immutable replay, not authorization to tune parameters or rerun inside this change.

## Scale-production conclusion

The owner corrections improved precision and removed the prior execution failure, but did not reduce report-level blockers or human-review workload enough for high-quality scaled production. The next bounded work should address the empirically observed Evidence selection/continuation defects and the dominant required-coverage workload before another pre-approved cohort validation. Production remains closed until a later change demonstrates the frozen readiness gates and separately authorizes promotion.
