## MODIFIED Requirements

### Requirement: Daily Quote Update Performs Bounded Catch-Up

The daily quote update task SHALL perform bounded catch-up for active tradable instruments whose local quote history is missing or recently behind after instrument master governance completes, while applying the daily integrity rules for coverage, quality, calendar evidence, and source freshness.

#### Scenario: Newly listed instrument has no local quotes

- **WHEN** a normal daily quote update runs for an instrument with no local quote rows and a `listed_date` on or before the target date
- **THEN** the system SHALL request daily quotes from the later of the instrument `listed_date` and the configured new-instrument catch-up lower bound through the target date
- **AND** it SHALL save returned quotes using the existing daily quote upsert path
- **AND** an empty or invalid response SHALL remain unresolved rather than count as success

#### Scenario: Newly listed instrument is discovered after its first trading day

- **WHEN** an instrument first enters the local active universe after one or more trading sessions have already occurred within the configured catch-up window
- **THEN** the next normal daily quote update SHALL include those prior sessions in the instrument's fetch window
- **AND** the daily update SHALL NOT require a manual backfill to cover those sessions

#### Scenario: Instrument has a recent short quote gap

- **WHEN** an instrument has a latest local quote date earlier than the normal daily update window and the missing span is within the configured short-gap catch-up window
- **THEN** the daily update SHALL request quotes from the bounded catch-up start through the target date
- **AND** duplicate dates already present locally SHALL remain safe under the existing quote upsert behavior
- **AND** the result SHALL distinguish rows actually written from an empty unresolved response

#### Scenario: Existing target-date row is not complete

- **WHEN** an instrument has a target-date row that is incomplete or fails daily quality validation
- **THEN** the bounded daily update SHALL treat the instrument as requiring refresh
- **AND** a valid replacement SHALL be eligible for the existing upsert/changelog path

#### Scenario: Missing span exceeds catch-up limit

- **WHEN** an instrument's missing span starts before the configured catch-up lower bound
- **THEN** the daily update SHALL cap the request at the configured lower bound
- **AND** it SHALL expose that the catch-up window was capped so broader repair can be handled by gap repair workflows

#### Scenario: Catch-up is reported

- **WHEN** a daily update completes after evaluating catch-up windows
- **THEN** the structured update result and report SHALL include catch-up counters and representative samples for new-instrument catch-up, short-gap catch-up, capped windows, catch-up quote rows, and unresolved/quality outcomes

#### Scenario: Historical range backfill remains isolated

- **WHEN** a historical range backfill or explicit point-in-time quote backfill runs
- **THEN** the bounded daily catch-up behavior SHALL NOT force current master refresh beyond the existing historical-backfill policy
- **AND** it SHALL NOT replace the explicit date range requested by the operator
