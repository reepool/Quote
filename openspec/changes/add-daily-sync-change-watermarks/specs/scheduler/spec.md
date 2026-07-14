## ADDED Requirements

### Requirement: Scheduled Daily Jobs Report Change Counters
Scheduler reports for daily sync, reconciliation, backfill, and repair jobs SHALL include inserted, changed, unchanged, skipped, and changelog-written counters when the job writes a changelog-enabled domain.

#### Scenario: Daily quote overlap produces unchanged rows
- **WHEN** a daily quote update re-fetches an overlap window and all fetched rows match existing semantic hashes
- **THEN** the scheduler report SHALL show unchanged rows
- **AND** it SHALL show zero changelog-written rows for those unchanged rows

### Requirement: Changelog Emission Does Not Change Task Lifecycle
Changelog emission SHALL NOT alter existing scheduler task activation, dependency execution, report delivery timeout, or active-task cleanup behavior.

#### Scenario: Report delivery fails after changelog write
- **WHEN** a job finishes data writes and changelog writes but notification delivery fails or times out
- **THEN** the scheduler SHALL still release task running state according to the existing task lifecycle contract

### Requirement: Daily Jobs Preserve Existing Correctness Backstops
Scheduled jobs SHALL keep configured overlap windows, catch-up windows, reconciliation jobs, gap repair, master governance, and trading-calendar governance unless an operator explicitly changes those settings.

#### Scenario: Changelog is enabled
- **WHEN** changelog emission is enabled for a daily sync job
- **THEN** the job SHALL still execute its existing market close checks, trading-day checks, governance requirements, and fetch-window policy
