# Shadow Evidence owner regression closure

This change closes the seven source-reviewed Evidence-owner/substance errors without invoking a provider or mutating the external replay. The `920033.BJ` material legal-empty row remains valid because its owner page explicitly marks `主要原材料及能源情况` as not applicable.

## Corrected source-review baseline

- Correct precision rows: 76 / 83 (91.566265%)
- Critical semantic errors: 0
- Readiness decision: `hold`
- Production authorization: `not_authorized`

## Provider-free closure

- Operating capacity/quantity legal empties from industry, production-planning, or generic context are rejected.
- Material legal empties from controlling-shareholder context are rejected.
- Segment legal empties from industry/background narrative are rejected.
- Explicit issuer capacity Evidence remains supported.
- Explicit material-section applicability remains supported.
- Cross-reference-only `BusinessOverview` targets are rejected while substantive text in the same Evidence remains supported.
- Provider calls: 0
- Cohort replay performed: false

## Remaining blocker before any production proposal

The immutable external replay remains `hold`: execution completion is 95%, usable-report rate is 0%, corrected sampled precision is 91.566265%, and unresolved-review median/p90 are 11/17. This fixture closure only authorizes proposing a separate empirical replay; it does not establish cohort usability or production readiness and does not open Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, or DCF.
