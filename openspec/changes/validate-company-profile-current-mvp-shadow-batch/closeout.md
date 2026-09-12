# Current-MVP Shadow Batch Closeout

## Decision

The validation closes as `hold` on September 12, 2026. The sole provider-bearing
result is
`manufacturing-materials-shadow-current-mvp-four-pool-20260912-a`; no report was
rerun or spliced and no replacement batch identity was created.

This result does not authorize production. `production_authorization` remains
`not_authorized`; Stage 6, approved tables, scheduler/backfill, commodity exposure,
value-chain publication, and DCF consumers remain closed.

## Immutable result

- Batch result hash:
  `f11efedbc4c25f2dfcf6d3e4fdb6738b8d0e70a3f38aa00566d3048f65127788`
- Review package hash:
  `7b6290be7f554eca7ad220974e695a5f6c536baae923846173947364c1e64144`
- Twenty report files were persisted independently under the batch manifest.
- The batch wrapper recorded 20 completed report executions and no missing report
  artifact. The reviewed report-level result contains 9 `failed` and 11 `hold`
  reports because required scopes did not all complete or pass.
- Historical cohort, PDF, Evidence-plan, and September 11 baseline inputs remain
  unchanged.

## Observed readiness

| Gate | Observed | Required | Result |
| --- | ---: | ---: | --- |
| Execution completion | 55% | at least 95% | fail |
| Usable reports | 0% | at least 90% | fail |
| Accepted Evidence traceability | 100% | 100% | pass |
| Source review | 415/415 rows | complete | pass |
| Sampled precision | 92/99 (92.93%) | at least 99% | fail |
| Critical semantic errors | 0 | 0 | pass |
| Human-review workload median | 10 | at most 2 | fail |
| Human-review workload p90 | 25 | at most 5 | fail |

The result contains 1,297 accepted research records with complete Evidence
traceability. The source-bound review retained all 56 blockers and 260 unresolved
items instead of promoting them into accepted facts. Among 99 chapter samples, 92
were correct, 7 had noncritical errors, and none had a critical semantic error.

The noncritical errors are common Evidence-owner or label-preservation defects:

- four BusinessOverview samples used revenue, regulatory, or risk-section text
  instead of a substantive principal-business owner;
- two legal-empty samples cited a general risk/industry page instead of the required
  material or segment owner;
- one correct consolidation event used a canonical English label in
  `source_native.name` rather than source-native wording.

## Comparison with the September 11 baseline

The current run is materially worse and therefore cannot support restricted
production promotion:

- execution completion fell from 100% to 55%;
- usable-report rate fell from 5% to 0%;
- accepted records fell from 1,443 to 1,297;
- review workload worsened from median/p90 6/10 to 10/25;
- sampled precision fell from 95.88% to 92.93%.

Evidence traceability remained 100%. Provider failed-call count fell from 29 to 12,
but all 12 current failures were `deadline_exceeded` and caused nine reports to be
classified `failed`.

## Provider and token findings

The four-model route executed 320 calls: 308 succeeded and 12 failed. Dynamic output
budgets were active with the frozen base/large/very-large tiers. Nine successful calls
returned more output than the base limit, so the scope-size mechanism is materially
used and should not be replaced with one global increase.

The maximum observed verify output was 27,580 tokens, above the configured very-large
24,000-token request cap. The response remained usable, but provider-side cap
compliance needs a controlled gateway check. Four successful Grok/GLM calls also took
275–298 seconds, so globally shortening the 300-second deadline would reject valid
results.

## Bounded next work

The next implementation should address common P1 defects, not rerun individual
companies:

1. Admit eligible deadline exhaustion into the existing bounded cross-model failover
   and reserve route time so parse/schema repair cannot consume the entire deadline.
2. Add source-owner regressions for the seven reviewed overview/legal-empty/label
   failures without changing the semantic field set.
3. Validate both fixes with controlled fixtures and a small representative vertical
   slice before considering another immutable cohort validation.
