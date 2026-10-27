## ADDED Requirements

### Requirement: Empty Daily-Quote Results Are Classified By Observed Trading State

The system SHALL classify an empty daily-quote fetch result for an A-share stock as
`legitimate_no_quote` instead of `empty_unresolved` when the instrument's latest
persisted quote row exists and records a non-trading state (`tradestatus=0`).

#### Scenario: Suspended stock returns no data
- **WHEN** an A-share stock whose latest persisted quote row has `tradestatus=0` returns no usable rows from all configured sources for the update window
- **THEN** the result SHALL be counted as `legitimate_no_quote`
- **AND** it SHALL NOT increment the exchange's stock failure count
- **AND** it SHALL NOT be added to empty-result retry candidates

#### Scenario: Trading stock returns no data
- **WHEN** an A-share stock whose latest persisted quote row has `tradestatus=1` (or whose master `trading_status` marks it trading) returns no usable rows
- **THEN** the result SHALL be classified as `empty_unresolved`
- **AND** it SHALL increment the exchange's stock failure count and remain retry-eligible

#### Scenario: Master non-trading exemption is preserved
- **WHEN** an instrument's master `trading_status` is present and not 1
- **THEN** an empty result SHALL continue to be classified as `legitimate_no_quote` as before, for every exchange

#### Scenario: Quality-rejected rows are not a suspension
- **WHEN** sources return rows for an A-share stock but all rows are rejected by row-level validation
- **THEN** the result SHALL NOT be classified as `legitimate_no_quote` regardless of prior suspension evidence

#### Scenario: No quote history is not a suspension
- **WHEN** an A-share stock has no persisted quote history for the instrument
- **THEN** an empty result SHALL be classified as `empty_unresolved`

#### Scenario: Evidence lookup fails closed
- **WHEN** the latest-quote-row lookup is unavailable or returns no row
- **THEN** an empty result SHALL keep the failure classification

#### Scenario: Non-A-share exchanges are unchanged
- **WHEN** a non-A-share (HKEX/US) instrument returns no usable rows
- **THEN** classification SHALL behave exactly as before this change

### Requirement: Suspended Empty Results Do Not Block The Stock Readiness Watermark

The per-exchange stock operational watermark SHALL NOT be blocked by empty results
that are classified as `legitimate_no_quote`.

#### Scenario: Night with only suspended empties
- **WHEN** an A-share exchange completes its daily update with zero stock failures other than empty results classified as `legitimate_no_quote`
- **THEN** the exchange's stock failure count SHALL be zero
- **AND** the per-exchange stock watermark write SHALL be eligible for `success`

#### Scenario: Any real stock failure still blocks
- **WHEN** at least one empty result for a trading stock or any other stock failure occurs on an exchange
- **THEN** the per-exchange stock watermark SHALL keep its blocking behavior unchanged

### Requirement: Suspension Exemption Does Not Fabricate Quote Rows

The exemption SHALL be a classification of an empty result only.

#### Scenario: Suspended day remains absent
- **WHEN** an empty result is classified as `legitimate_no_quote`
- **THEN** no daily-quote row SHALL be written or forward-filled for that instrument and date
- **AND** the missing day SHALL remain repairable by later catch-up when a source serves the instrument again

### Requirement: Operator Report Shows The Suspension Exemption Count

The daily-update operator report SHALL expose the number of instruments classified as
`legitimate_no_quote` in the integrity section.

#### Scenario: Report includes suspended empties
- **WHEN** a daily update run classifies one or more results as `legitimate_no_quote`
- **THEN** the integrity line SHALL include a non-zero suspension-exemption count distinct from `未覆盖` (empty unresolved)
