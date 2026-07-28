## Why

The final eight CNInfo corporate-action blockers now have explicit operator
decisions, including two suspended restructuring dilutions, three pre-listing
distributions, two externally funded performance-compensation payments, and
one official special reference-price adjustment. The current workflow
intentionally left these events unresolved because it cannot represent an
analysis-free operator no-effect decision without fabricating announcement or
LLM evidence.

## What Changes

- Add an audited fixed-manifest path for exactly the eight operator-reviewed
  CNInfo event identities; do not create a category-wide approval rule.
- Allow an unchanged current CNInfo event to be approved with
  `factor_effect=none` from explicit operator attestation when no suitable
  announcement or LLM analysis exists, without creating synthetic source
  evidence.
- Validate the two suspended restructuring events against their first resumed
  trading session and retain normal CNInfo capitalization economics.
- Apply the `002076.SZ` official adjusted-reference-price event on
  `2022-12-21` using the published `2.60` and `2.23` prices, yielding the
  project-convention factor `2.60 / 2.23`.
- Preview and apply all eight decisions locally, then verify that raw CNInfo
  observations, TDX audit data, archived documents, and existing production
  factor rows are unchanged by the review writes.
- Rebuild the CNInfo factor path only after all eight governance blockers are
  resolved and verify the expected normal, excluded-no-effect, and official
  reference-price outcomes.
- Treat an implemented event that occurs during suspension as effective on the
  first valid traded session on or after the implementation date, bounded by
  the requested factor-rebuild end date. Preserve the implementation date as
  source lineage and do not require another announcement or LLM analysis.

## Capabilities

### New Capabilities

- `cninfo-final-blocker-operator-decisions`: Defines the audited,
  fixed-identity approval and verification rules for the final eight CNInfo
  corporate-action blockers.

### Modified Capabilities

None.

## Impact

- CNInfo manual-review orchestration and review lineage in `data_manager.py`.
- A fixed-list local preview/write script and focused unit tests.
- Bounded quote-evidence lookup for long suspensions and focused resumption-date
  regression tests.
- Existing corporate-action review, resolved-term, effective-date-evidence,
  and governance-state tables; no schema migration is required.
- The local `data/quotes.db` receives eight operator review decisions after a
  successful preview. No network, document download, OCR, or LLM work is
  performed.
