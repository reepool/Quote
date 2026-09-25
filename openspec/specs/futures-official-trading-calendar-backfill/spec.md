## Purpose
Define reliable official trading-calendar backfill across configured domestic futures exchanges, including evidence quality, unresolved-date handling, bounded retries, dry-run semantics, persistence rules, and operator diagnostics required before historical or daily market-data collection.
## Requirements
### Requirement: Official futures calendar backfill covers configured domestic exchanges
The system SHALL provide a futures trading-calendar backfill operation that targets every configured domestic futures exchange: SHFE, INE, DCE, CZCE, and GFEX.

#### Scenario: Default exchange coverage
- **WHEN** the official futures calendar backfill runs without an exchange override
- **THEN** it SHALL target SHFE, INE, DCE, CZCE, and GFEX

#### Scenario: Bounded exchange override
- **WHEN** the operator supplies an exchange list
- **THEN** the backfill SHALL limit work to that list and SHALL reject unsupported exchanges with an explicit diagnostic

### Requirement: Official futures calendar backfill starts from 2010 by default
The system SHALL default historical futures calendar backfill to `2010-01-01` unless an operator supplies a later bounded range.

#### Scenario: Default start date
- **WHEN** the backfill is invoked without `start_date`
- **THEN** the request range SHALL begin at `2010-01-01`

#### Scenario: Operator supplies a narrower range
- **WHEN** the backfill is invoked with explicit `start_date` and `end_date`
- **THEN** the backfill SHALL only evaluate the requested inclusive date range

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

### Requirement: Calendar backfill persists evidence and quality
The backfill SHALL persist source interface, parser version, evidence URL, payload hash or request outcome, row count, and quality flag metadata for every written calendar row.

#### Scenario: Calendar row is written
- **WHEN** a date is classified by an official source
- **THEN** the persisted row SHALL include enough metadata to audit the source and classification

### Requirement: Future calendar coverage is limited to accurate official knowledge
The system SHALL not mark future weekdays as accurate futures trading days unless an official source or notice explicitly verifies those future dates.

#### Scenario: Future date has no official evidence
- **WHEN** the requested date is in the future and no official calendar notice or equivalent official source covers it
- **THEN** the backfill SHALL leave that date unresolved rather than writing an estimated accurate row

### Requirement: Backfill readiness reports unresolved gaps
The backfill SHALL return per-exchange counts for written trading days, written closed days, unresolved dates, source failures, and latest verified date.

#### Scenario: Backfill completes with gaps
- **WHEN** one or more dates cannot be officially classified
- **THEN** the result SHALL have warning or blocked status and SHALL list unresolved counts by exchange

### Requirement: DCE calendar anti-bot failure is bounded per provider run
The calendar backfill SHALL stop repeating full DCE browser readiness work after the configured direct and proxy routes have failed within one provider run.

#### Scenario: All bounded DCE routes fail
- **WHEN** direct access and every configured DCE proxy lease fail challenge validation, business-request validation, or a hard browser timeout
- **THEN** the provider SHALL open a run-scoped DCE circuit breaker
- **AND** remaining requested DCE dates SHALL be reported unresolved without starting another browser readiness cycle
- **AND** the backfill result SHALL retain warning or blocked status according to existing unresolved-date rules

#### Scenario: A later run starts
- **WHEN** a new provider instance is created for a subsequent scheduled or manual run
- **THEN** the DCE circuit breaker SHALL start closed and permit a new bounded access attempt

#### Scenario: Rolling re-probe fails for a date with strong evidence
- **WHEN** the current official probe is unresolved but the stored calendar row is `backfilled_verified` from official daily rows or an official closure notice
- **THEN** the backfill SHALL preserve the stored verified classification
- **AND** it SHALL report the current probe failure as preserved evidence diagnostics rather than a blocking unresolved date

#### Scenario: Route exhaustion includes a timeout summary
- **WHEN** the DCE client reports that all bounded routes are exhausted and its sanitized last error mentions a timeout
- **THEN** the failure SHALL remain a non-retryable DCE route-exhaustion classification
- **AND** outer provider and calendar retry loops SHALL NOT wait or repeat the open circuit

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
