## 1. Operator-attested review support

- [x] 1.1 Extend manual CNInfo review validation and persistence for structured operator-attestation evidence with unchanged source terms and no synthetic announcement or analysis.
- [x] 1.2 Expose analysis-free operator-attested `factor_effect=none` reviews through the resolved factor-policy loader without creating an economic overlay.
- [x] 1.3 Add focused regression tests for allowed no-effect attestations, rejected economic changes, evidence provenance, and overlay precedence.

## 2. Fixed eight-event decisions

- [x] 2.1 Add an immutable preview/write script for the eight frozen events, including row and decision hashes, dates, factor effects, official price references, and source-isolation snapshots.
- [x] 2.2 Add fixed-manifest tests covering all eight decisions, preview safety, factor-policy counts, official factor direction, and idempotent resume reporting.
- [x] 2.3 Run the preview and apply all eight reviews to the configured database; verify zero remaining blockers and no review-write mutation of raw CNInfo, TDX, document, or production-factor data.

## 3. Validation and factor closure

- [x] 3.1 Run focused tests, strict OpenSpec validation, and an uncommitted review; fix only confirmed in-scope defects.
- [x] 3.2 Rebuild and inspect the CNInfo factor path, verifying two normal events, five no-effect exclusions, and the `002076.SZ` official factor `1.165919282511`.
  - The two normal events were materialized on `2013-02-08` and the five
    no-effect events were excluded. The `002076.SZ` event-level policy factor
    was verified as `2.60 / 2.23`, but its complete cumulative series remains
    blocked by pre-existing quote-anchor gaps for the `2014-06-13` and
    `2017-06-01` events.
- [ ] 3.3 Commit and push only the implementation, tests, and OpenSpec artifacts created for this change.
