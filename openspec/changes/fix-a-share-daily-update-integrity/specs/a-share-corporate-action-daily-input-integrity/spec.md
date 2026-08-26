## MODIFIED Requirements

### Requirement: Daily factor discovery preserves event dates

The system SHALL preserve the event date associated with every discovered A-share ex-dividend symbol, SHALL use that dated evidence when selecting per-instrument factor synchronization work, and SHALL query all report-period anchors required by the target event years.

#### Scenario: Discovered event falls inside the instrument window

- **WHEN** a discovered event date for an instrument falls inside that instrument's inclusive factor request window
- **THEN** the system SHALL select the instrument for factor synchronization and SHALL pass the instrument's own request window to the factor source

#### Scenario: Discovered event falls outside the instrument window

- **WHEN** a symbol is discovered because its event falls inside another instrument's wider union window but all of its own discovered event dates fall outside its own factor request window
- **THEN** the system SHALL exclude that instrument from factor synchronization and SHALL NOT count it as skipped source data or a download failure

#### Scenario: Discovery date cannot be normalized

- **WHEN** the discovery source returns a matched symbol with a missing or invalid event date
- **THEN** the system SHALL expose discovery uncertainty and SHALL NOT use that row as proof that an empty factor response is successful

#### Scenario: Target year includes quarterly disclosure periods

- **WHEN** daily factor synchronization requests event discovery for one or more target years
- **THEN** the discovery layer SHALL query the deduplicated report-period anchors needed for first-quarter, interim, third-quarter, and annual plans, including `0331`, `0630`, `0930`, and `1231` as applicable
- **AND** it SHALL retain prior-year annual anchors when the requested event window crosses a year boundary

#### Scenario: One report period fails

- **WHEN** any required report-period query fails or returns an unusable schema
- **THEN** discovery SHALL return an incomplete/partial result rather than a successful empty event set
- **AND** the factor stage SHALL expose the failed period and remain eligible for retry or maintenance recovery
