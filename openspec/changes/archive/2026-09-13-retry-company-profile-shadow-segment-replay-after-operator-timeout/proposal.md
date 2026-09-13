## Why

The first segment-financial repair replay was interrupted by an operator-level
420-second process cutoff after one report, so it cannot establish the frozen
cohort's post-repair readiness. The repaired implementation and admission path
are valid; a new, independently identified replay is needed to obtain a
complete empirical result without reusing or splicing the partial output.

## What Changes

- Preserve the interrupted batch and its partial report as an immutable
  execution-failure reference.
- Admit one new batch identity bound to the same frozen twenty-report manifest,
  v5 Evidence plan, prepared scopes, audits, Gemini profile, and provider
  budgets.
- Execute the cohort once without an operator-wide hard cutoff, targeted reruns,
  model substitution, parameter tuning, output reuse, or result splicing.
- Generate the complete source-review, segment-recurrence, readiness, and
  before/after comparison artifacts from the new batch only.
- Close as `ready`, `hold`, or `failed` from the existing gates while retaining
  `production_authorization=not_authorized`.

## Capabilities

### New Capabilities

- `company-profile-shadow-replay-retry`: admits and audits one bounded retry of
  an operator-interrupted segment-financial shadow replay.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: add a source-bound
  retry identity and require the prior interrupted attempt to remain immutable
  and excluded from the new result.

## Impact

- Affected operator: `scripts/run_company_profile_shadow_batch.py` and the
  existing shadow batch service.
- Affected artifacts: one new immutable batch plus change-local failure,
  review, recurrence, readiness, comparison, and result-summary records.
- No changes to semantic fields, Evidence selection, Gold expectations,
  production writers, approved tables, scheduler/backfill, commodity or value
  chain publication, DCF, or Stage 6.
