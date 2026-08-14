## MODIFIED Requirements

### Requirement: Calendar rows must be officially verified
The backfill SHALL write a trading calendar row only when positive official market rows, deterministic weekend policy, or date-specific official closure evidence classifies the date. The backfill MUST NOT classify a weekday as closed solely because a daily-market request returned no contract rows, a generic no-data response, an anti-bot response, or an unclassified payload.

#### Scenario: Official source has trading rows
- **WHEN** an official exchange daily endpoint returns parseable contract rows for a date
- **THEN** the backfill SHALL write `is_trading_day=true` with official source metadata

#### Scenario: Official evidence confirms no session
- **WHEN** a weekend rule or date-specific official holiday or temporary-closure notice confirms that an exchange has no session
- **THEN** the backfill SHALL write `is_trading_day=false` with the closure evidence metadata

#### Scenario: Weekday endpoint returns an empty payload
- **WHEN** an official exchange daily endpoint returns no parseable contract rows for a weekday without date-specific closure evidence
- **THEN** the backfill SHALL NOT write or retain a closed-day classification from that response alone
- **AND** it SHALL report the date as unresolved with the request outcome

#### Scenario: Date cannot be verified
- **WHEN** the official endpoint fails, times out, returns an anti-bot response, or returns an unclassified response
- **THEN** the backfill SHALL NOT guess the calendar row and SHALL report the date as unresolved

## ADDED Requirements

### Requirement: Calendar classification must respect exchange publication cutoffs
The official calendar backfill SHALL evaluate daily-market evidence using the configured publication cutoff and timezone for the target exchange.

#### Scenario: Manual run occurs before publication cutoff
- **WHEN** a manual daily backfill runs before an exchange's configured cutoff on the current local date
- **THEN** an empty response for the current date SHALL be classified as not yet due and unresolved
- **AND** it SHALL NOT be counted as evidence of a closed day or a post-cutoff source failure

#### Scenario: Empty response persists after publication cutoff
- **WHEN** a governed trading date is publication-eligible and its daily endpoint still returns no classifiable rows
- **THEN** the backfill SHALL leave the date unresolved
- **AND** it SHALL report a post-cutoff source or data-quality blocker

### Requirement: Positive official evidence repairs weak recent calendar rows
The official calendar backfill SHALL re-evaluate recent rows based only on empty-payload evidence and SHALL allow later positive official market rows to replace their trading status.

#### Scenario: Later rows prove a weakly closed date traded
- **WHEN** a recent date stored as closed with `official_empty_payload` or equivalent weak evidence later returns parseable official contract rows
- **THEN** the backfill SHALL update the date to `is_trading_day=true`
- **AND** it SHALL preserve auditable metadata for the new evidence and repair outcome

#### Scenario: Explicit closure evidence remains authoritative
- **WHEN** a recent closed date has date-specific official closure evidence and a re-probe yields only an empty payload
- **THEN** the backfill SHALL preserve the verified closed-day classification
- **AND** it SHALL NOT replace the closure evidence with generic empty-payload evidence

### Requirement: Daily calendar backfill re-probes a bounded recent window
The daily official calendar path SHALL re-probe a configurable rolling window of three through five natural dates, defaulting to five, for each selected exchange.

#### Scenario: Daily run finds weak or unresolved recent dates
- **WHEN** the rolling window contains missing rows, unresolved dates, or rows classified from weak empty-payload evidence
- **THEN** the backfill SHALL include those dates in the repair probe set

#### Scenario: Operator supplies an explicit historical range
- **WHEN** the operator supplies explicit `start_date` and `end_date`
- **THEN** the backfill SHALL preserve that inclusive range
- **AND** it SHALL NOT widen the request solely because a daily rolling-window default exists
