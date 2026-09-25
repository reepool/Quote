## ADDED Requirements

### Requirement: Calendar backfill refreshes official holiday notices before daily probes
The official calendar backfill SHALL refresh configured exchange holiday notices through trading-day governance before it probes official daily files. This refresh SHALL be part of the existing backfill operation, not a separate job.

#### Scenario: Daily preflight contains a published holiday
- **WHEN** official calendar backfill runs for an exchange whose current official holiday notice closes a date inside the requested range
- **THEN** the backfill SHALL persist that notice and its closed calendar rows before probing
- **AND** it SHALL NOT request the official daily file for those notice-closed dates
- **AND** those dates SHALL NOT be reported as unresolved

#### Scenario: Notice content is unchanged
- **WHEN** the fetched notice hash matches the stored notice for that exchange and year
- **THEN** the backfill SHALL keep the existing notice evidence
- **AND** it SHALL still ensure the derived closed dates are present

#### Scenario: One exchange notice cannot be fetched or parsed
- **WHEN** holiday-notice refresh fails or requires review for one exchange
- **THEN** the backfill SHALL NOT invent closed rows for that exchange
- **AND** the failure SHALL NOT prevent notice refresh for the other requested exchanges
- **AND** weekdays in the failed exchange that lack a covering notice SHALL continue to the daily-file probe

### Requirement: Uncovered weekday misses stay unresolved
A missing official daily file SHALL NOT by itself classify a weekday as closed.

#### Scenario: Post-cutoff daily file is missing and no notice covers the date
- **WHEN** the publication cutoff has passed and the official daily endpoint returns 404, no report, or no contract rows for a weekday that no official holiday notice closes
- **THEN** the backfill SHALL leave that date unresolved
- **AND** it SHALL NOT write `is_trading_day=false` from that response

#### Scenario: Daily rows contradict a notice closure
- **WHEN** a later official daily probe returns parseable contract rows for a date closed by `official_holiday_notice`
- **THEN** the backfill SHALL preserve the notice-closed row
- **AND** it SHALL record the contradiction and create a review-required record
- **AND** it SHALL NOT silently mark the date as a trading day

#### Scenario: Probe fails on an existing notice closure
- **WHEN** a notice-closed date is already `backfilled_verified` and a probe is nevertheless run and is unresolved
- **THEN** the backfill SHALL preserve the notice classification
- **AND** the probe failure SHALL NOT be a blocking unresolved date

## MODIFIED Requirements

### Requirement: Calendar rows must be officially verified
The backfill SHALL write a calendar row only when an official exchange source classifies the date as trading or closed. The backfill MUST NOT create accurate calendar rows from weekday guesses. A bare 404, no-report response, or empty contract payload is not a recognized official closure.

#### Scenario: Official source has trading rows
- **WHEN** an official exchange daily endpoint returns parseable contract rows for a date that is not closed by an official holiday notice
- **THEN** the backfill SHALL write `is_trading_day=true` with official source metadata

#### Scenario: Official source confirms no session
- **WHEN** an official exchange source returns a recognized official closure, including a parsed holiday-notice closure or a recognized no-session payload that is not a bare 404 or empty file
- **THEN** the backfill SHALL write `is_trading_day=false` with official source metadata

#### Scenario: Date cannot be verified
- **WHEN** the official endpoint fails, times out, returns 404 or no contract rows, or returns an unclassified response for a date no official notice closes
- **THEN** the backfill SHALL NOT guess the calendar row and SHALL report the date as unresolved
