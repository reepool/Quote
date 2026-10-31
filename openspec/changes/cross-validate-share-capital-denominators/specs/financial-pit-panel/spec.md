## MODIFIED Requirements

### Requirement: Derived indicators with computability boundaries
The system SHALL precompute per-share, year-over-year, and TTM derived indicators at panel build time from cumulative statement values, SHALL compute them only when the required prior-period inputs exist, and SHALL return null outside the computability boundaries. Per-share denominators SHALL be resolved through multi-source cross-validation with the outcome annotated, never silently substituted.

#### Scenario: YoY boundaries reflect available history
- **WHEN** a panel row's report period is an annual period on or after 2024-12-31, or a quarterly period on or after the corresponding 2025 period
- **THEN** `rev_yoy` and `np_yoy` SHALL be computed against the prior-year same period when that input exists
- **AND** for earlier periods (2023-12-31 and quarterly 2024 periods) these fields SHALL be null

#### Scenario: TTM boundary reflects available history
- **WHEN** a panel row's report period is on or after 2025-03-31
- **THEN** `ttm_revenue` and `ttm_net_income` SHALL be computed as current cumulative plus prior annual minus prior-year same-period cumulative when inputs exist
- **AND** for earlier periods these fields SHALL be null

#### Scenario: Denominator cross-validation reaches consensus
- **WHEN** a panel row's available share anchors (shares-outstanding canonical fact, share-capital fact, valuation-implied shares, EPS-implied shares, plus the paid-in-capital fact as a validator) agree within their tolerance bands
- **THEN** the denominator SHALL be the highest-preference anchor value
- **AND** the row SHALL carry `share_validation=consensus` with `share_basis` naming the adopted anchor

#### Scenario: Anchor disagreement is annotated, not silently fixed
- **WHEN** any two available anchors disagree beyond their tolerance bands
- **THEN** the denominator SHALL still be the highest-preference available anchor value
- **AND** the row SHALL carry `share_validation=discrepancy` with `share_candidates` recording every anchor's value, basis, and absence reasons
- **AND** consumers SHALL be able to filter or inspect flagged rows through the API and coverage outputs

#### Scenario: Paid-in-capital anchor is implicitly par-validated
- **WHEN** the paid-in-capital fact (a CNY amount) disagrees with other share-count anchors
- **THEN** it SHALL NOT be adopted as the denominator and the disagreement SHALL be visible in `share_candidates`
- **AND** it SHALL be adopted as the denominator only when no share-count anchor of any kind is available, with `share_basis` marking the par-value assumption
- **AND** no explicit par-value reference table SHALL be required

#### Scenario: EPS anchor is skipped when either operand is unstable
- **WHEN** net income or the reported basic EPS is near zero
- **THEN** the EPS-implied anchor SHALL be excluded and the exclusion reason recorded in `share_candidates`

#### Scenario: Unit-mismatch of the paid-in-capital validator does not flip consensus
- **WHEN** all available share-count anchors agree with each other but the paid-in-capital fact (a CNY amount) disagrees with them
- **THEN** the row SHALL remain `share_validation=consensus` with a `unit_mismatch` note (likely non-unit par value) recorded in `share_candidates`
- **AND** the paid-in-capital disagreement SHALL escalate to `discrepancy` only when fewer than two share-count anchors are available or when it corroborates a dissenting share-count anchor

#### Scenario: Non-positive share values are excluded from anchor assembly
- **WHEN** an anchor's fact value is zero or negative
- **THEN** that anchor SHALL be treated as absent with the reason recorded, and SHALL never become the adopted denominator

#### Scenario: Incremental refresh applies the same anchor assembly
- **WHEN** a panel row is refreshed incrementally after a sync upsert
- **THEN** its denominator SHALL be resolved by the same cross-validation rules as a full rebuild

#### Scenario: Multiple facts of one anchor resolve deterministically
- **WHEN** one anchor type carries multiple facts for the same instrument and period (e.g., express and final filings)
- **THEN** the latest-updated fact of that anchor SHALL be used and stale facts SHALL NOT create phantom disagreements

#### Scenario: Single-anchor and no-anchor rows are labelled honestly
- **WHEN** exactly one anchor is available
- **THEN** the row SHALL carry `share_validation=single_source` with `share_basis` naming it
- **AND** when no anchor is available the denominator, `eps` (when not reported), `bps`, and `ocfps` SHALL be null

#### Scenario: Quarantined share facts degrade validation transparently
- **WHEN** a share-capital fact row has been quarantined as `share_capital_suspect`
- **THEN** it SHALL be excluded from anchor assembly
- **AND** the remaining anchors SHALL determine the validation outcome without error
