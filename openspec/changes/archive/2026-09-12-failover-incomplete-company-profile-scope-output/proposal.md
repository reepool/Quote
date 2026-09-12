## Why

The fast four-model MVP run exposed a concrete execution bug: several extract responses
were JSON-schema valid even though they returned no candidate or legal-empty result for
one or more active required fields. The gateway therefore recorded a provider success,
and the workflow discovered `required_result_missing` only after the opportunity for
bounded model failover had passed.

## What Changes

- Tighten the existing model-side extract schemas so every active non-optional field is
  represented by either a candidate fact or a permitted legal-empty coverage result.
- Classify an empty or partially missing required extract response as the existing
  `schema_validation_error`, allowing the current four-member logical pool to perform
  bounded failover without adding another retry loop.
- Preserve semantic rejection, Evidence mismatch, prohibited inference, and verifier
  disagreement as terminal semantic outcomes; they do not trigger model cycling.
- Add provider-free regressions for the schema families that produced real missing
  results: segment rows, operating quantities, material inputs, counterparties, and
  business regime.
- Validate the fix with one small immutable unseen-report canary after local checks. A
  `usable_with_caveats`, `hold`, or typed `failed` result closes the canary honestly;
  it does not authorize repeated targeted runs.
- Do not execute the stale Gemini-only twenty-report replay
  `replay-company-profile-shadow-segment-financial-repair`; its frozen implementation,
  subject policy, model policy, and budgets are superseded by the current MVP baseline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-bounded-semantic-workflow`: require model-side extract completeness
  for active non-optional fields before a provider response can be treated as successful.
- `company-profile-manufacturing-materials-shadow-batch`: replace the stale broad replay
  with one bounded unseen-report canary using the current four-model/default-group/token
  baseline.

## Impact

- Affects the compact extract-schema builders in
  `research/company_profile/stage5_provider.py` and focused tests.
- Reuses the existing `semantic_extraction` logical route, schema validation, typed
  failover, immutable bundle writer, and report benchmark; no new gateway or service is
  introduced.
- Historical bundles, Gold expectations, approved/production tables, scheduler/backfill,
  commodity exposure, value-chain publication, DCF, and Stage 6 remain unchanged and
  closed. `production_authorization` remains `not_authorized`.
