## ADDED Requirements

### Requirement: Futures sync success requires exchange-level target-date completeness
Futures daily sync and backfill SHALL compare governed target dates with persisted price coverage for each selected exchange after provider processing. The overall run MUST NOT report `success` while any selected exchange remains stale, has a required missing target date, or has an unresolved governance blocker.

#### Scenario: Every exchange covers its expected latest date
- **WHEN** every selected exchange has persisted coverage for all required governed target dates through its expected latest trading date
- **AND** no governance or provider blocker remains
- **THEN** the run MAY report `success`

#### Scenario: One exchange remains behind its expected latest date
- **WHEN** a selected exchange's expected latest trading date is later than its actual latest persisted price date
- **THEN** the run SHALL report `partial` when other useful work completed
- **AND** it SHALL identify the stale exchange and missing dates instead of reporting `success`

#### Scenario: An internal recent trading-date gap remains
- **WHEN** the actual latest persisted price date is current but an earlier required target date in the bounded repair window has no coverage
- **THEN** the run SHALL report the missing target date as a completeness blocker
- **AND** it SHALL NOT report `success`

#### Scenario: Governance blocks provider work
- **WHEN** unresolved or below-threshold calendar evidence prevents production provider work for a selected exchange
- **THEN** the run SHALL retain `blocked` status according to the existing governance contract
- **AND** it SHALL NOT convert the blocker to `success` because no provider request failed

### Requirement: Futures completeness respects resolved scope and lifecycle skips
The exchange-level completeness comparison SHALL use the task's resolved exchange/date scope and existing instrument lifecycle eligibility rules.

#### Scenario: Instrument is outside its active lifecycle
- **WHEN** an instrument is not eligible on a governed target date under existing listing, delisting, or lifecycle rules
- **THEN** the sync SHALL preserve that item as an explicit lifecycle skip
- **AND** it SHALL NOT create a missing-price blocker solely for that ineligible instrument

#### Scenario: Required exchange date has persisted bars
- **WHEN** persisted bars cover a governed target date for the task's resolved exchange scope
- **THEN** that date SHALL count as covered even when the provider result for the current run reports those rows as unchanged

### Requirement: Futures sync results expose per-exchange freshness diagnostics
Futures sync results and ingestion-run metadata SHALL expose the inputs and outcome of exchange-level completeness checks.

#### Scenario: Sync finishes with mixed exchange outcomes
- **WHEN** futures sync finishes for one or more exchanges
- **THEN** each exchange result SHALL include its requested range, governed target dates, expected latest trading date, actual latest persisted price date, repaired dates, remaining missing dates, lifecycle skips, and blockers
- **AND** the top-level status SHALL be derived from those exchange outcomes

#### Scenario: Dry-run encounters a completeness or governance blocker
- **WHEN** a dry-run identifies a stale exchange, missing target date, or unresolved governance issue
- **THEN** it SHALL retain a non-success status and expose the blocker
- **AND** it SHALL NOT present the run as production-complete
