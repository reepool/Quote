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
- **WHEN** a panel row's available share anchors (share-capital fact, paid-in-capital fact, valuation-implied shares, EPS-implied shares) agree within their tolerance bands
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
- **AND** no explicit par-value reference table SHALL be required

#### Scenario: Single-anchor and no-anchor rows are labelled honestly
- **WHEN** exactly one anchor is available
- **THEN** the row SHALL carry `share_validation=single_source` with `share_basis` naming it
- **AND** when no anchor is available the denominator, `eps` (when not reported), `bps`, and `ocfps` SHALL be null

#### Scenario: EPS anchor is skipped near zero profit
- **WHEN** net income is near zero for the period
- **THEN** the EPS-implied anchor SHALL be excluded from validation rather than producing an unstable implied share count
- **AND** the exclusion SHALL be recorded in `share_candidates`

#### Scenario: Quarantined share facts degrade validation transparently
- **WHEN** a share-capital fact row has been quarantined as `share_capital_suspect`
- **THEN** it SHALL be excluded from anchor assembly
- **AND** the remaining anchors SHALL determine the validation outcome without error
