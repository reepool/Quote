## Why

The remaining CNInfo review backlog is dominated by asymmetric share-reform,
performance-compensation, debt-settlement, and similar events. A normal
all-shareholder validation formula cannot represent operator-approved
total-share-capital terms, beneficiary-only terms, or an event that is recorded
but has no adjustment-factor effect.

## What Changes

- Add an auditable CNInfo-only manual asymmetric override path that supersedes
  prior reviews without modifying raw observations.
- Store total-share-capital economic terms separately from beneficiary-only
  descriptive terms and record whether the event affects adjustment factors.
- Allow an approved event to remain recorded while explicitly contributing no
  adjustment-factor change.
- Add deterministic announcement-title prefiltering before title LLM
  classification and document resolution, while preserving the original
  announcement scan record and filter lineage.
- Apply the operator decisions for `000519.SZ`, `600449.SH`, `000031.SZ`, and
  `000035.SZ` from existing persisted CNInfo data without redownloading or
  reanalyzing documents.
- Allow an operator to approve unchanged current CNInfo terms and persisted
  official-date evidence when no LLM analysis exists, without fabricating an
  analysis row or creating a redundant resolved-term overlay.
- Apply the CNInfo-only operator decision for `000623.SZ`: retain the official
  `10派2.14元` terms, use the reform implementation date, and record the
  non-tradable-share contraction as non-factor descriptive lineage.

## Capabilities

### New Capabilities

- `cninfo-manual-asymmetric-override`: Defines audited operator overrides,
  total-share-capital terms, beneficiary-only terms, review supersession, and
  explicit adjustment-factor effects.
- `cninfo-corporate-action-title-prefilter`: Defines conservative,
  deterministic exclusion of non-implementation announcement titles before
  expensive classification and document work.

### Modified Capabilities

- `a-share-adjustment-factor-path-governance`: Resolved CNInfo events explicitly
  marked as having no factor effect remain recorded but do not contribute to
  factor derivation.

## Impact

- `data_manager.py` review orchestration, CNInfo discovery, and factor overlay
  preparation.
- CNInfo governance helpers and database resolved-term read lineage.
- Corporate-action review API payload behavior and focused unit tests.
- Existing CNInfo review, resolved-term, and effective-date-evidence tables;
  no raw observation mutation and no schema migration are required.
- TDX remains comparison evidence only; no TDX economic term or factor is
  written into the CNInfo factor path.
