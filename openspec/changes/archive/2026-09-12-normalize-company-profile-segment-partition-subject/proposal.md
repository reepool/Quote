## Why

The immutable Hualu-Hengsheng canary exposed one P1 adapter bug: metric partitions for
the same disclosed segment row can carry `consolidated_group/report_default_group_scope`,
`business_segment`, or `unclear` before expansion, causing row reconciliation to hold a
physically complete segment table. The subject convention must be normalized before
partition identity comparison.

## What Changes

- Normalize ordinary, Evidence-bound `unclear` and `business_segment` rows before
  merging partition responses, and collapse a report-default group draft to that
  segment representation when paired with the same segment row.
- Retain the existing stronger-basis rule when a report-default draft is paired with an
  affirmative or numeric-reconciled consolidated-group result.
- Preserve explicit issuer, named-subsidiary, direct group, numeric-reconciled group, and
  consolidation-adjustment subject semantics as blocking identity distinctions.
- Add provider-free regression coverage for the exact canary failure shape and for the
  explicit-scope boundaries.
- Validate against the immutable canary response shape without provider calls or bundle
  mutation; do not rerun the canary under this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: normalize default/unclear subject metadata
  consistently before segment partition row identity comparison.

## Impact

- Affects only segment partition normalization in
  `research/company_profile/stage5_provider.py` and focused provider-free tests.
- Does not change Evidence, request/record models, LLM routing, token budgets, Gold,
  historical bundles, production authorization, Stage 6, or downstream publication.
