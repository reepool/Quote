# Post-isolation scale-readiness closeout

## Outcome

The authoritative twenty-report replay remains `hold`. The source-reviewed empirical result is unchanged: all 20 reports executed, 1,443 research records were accepted with 100% Evidence traceability, 1 of 20 reports was usable-with-caveats, sampled precision was 93/97 (95.8763%), and unresolved-review median/p90 were 6/10.

The later invalid-item isolation implementation applies to 29 failed extract calls in 16 reports. Historical raw provider responses were not persisted, so the exact number of valid siblings, recovered records, and changed report statuses remains unavailable.

## Deterministic bounds

Three hold reports (`000059`, `000420`, and `000629`) contain no applicable failed extract call. Even under the deliberately optimistic assumption that every failure-affected hold report becomes usable, the absolute usable-rate upper bound is therefore 17/20 (85%), below the 90% gate.

Of 127 observed unresolved-review rows, 82 are located in failed scopes. Removing all 82 is a maximum-removal counterfactual, not an expected recovery. It would reduce the workload to a median of 0 but a p90 of 6, so the p90 gate would still fail. The sampled precision remains 95.8763% with four noncritical source-review errors and therefore also remains below 99%.

Item isolation alone cannot close the usable-rate, sampled-precision, or p90 workload gates. It is a correct future-response robustness improvement, not evidence of scale readiness.

## Boundaries and next priority

- Provider/LLM calls: 0.
- Frozen batch/report/review files changed: none.
- Historical statuses or accepted facts rewritten: none.
- Dynamic token budgets changed: no.
- Production authorization: `not_authorized`.
- Stage 6, approved storage, scheduler/backfill, commodity exposure, value-chain publication, and DCF remain closed.

The next business priority is an independently authorized empirical validation after the remaining hold and reviewed-precision causes are addressed. Adaptive output-token selection remains reasonable, but it must first be calibrated in a separate provider-free dry-run and must not be presented as a fix for the 29 schema-invalid calls.
