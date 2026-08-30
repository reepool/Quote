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
The backfill SHALL write a calendar row only when an official exchange source classifies the date as trading or closed. The backfill MUST NOT create accurate calendar rows from weekday guesses.

#### Scenario: Official source has trading rows
- **WHEN** an official exchange daily endpoint returns parseable contract rows for a date
- **THEN** the backfill SHALL write `is_trading_day=true` with official source metadata

#### Scenario: Official source confirms no session
- **WHEN** an official exchange daily endpoint returns a recognized official no-data or no-session response
- **THEN** the backfill SHALL write `is_trading_day=false` with official source metadata

#### Scenario: Date cannot be verified
- **WHEN** the official endpoint fails, times out, or returns an unclassified response
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
