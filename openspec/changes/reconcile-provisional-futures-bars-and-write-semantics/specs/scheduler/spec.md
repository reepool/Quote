## MODIFIED Requirements

### Requirement: Scheduled Daily Jobs Report Change Counters
Scheduler reports for daily sync, reconciliation, backfill, and repair jobs SHALL include inserted, changed, unchanged, skipped, and changelog-written counters when the job writes a changelog-enabled domain. Futures reports SHALL additionally distinguish new business-date coverage, source upgrades, same-source corrections, post-cutoff verified unchanged rows, and remaining provisional dates while retaining the aggregate counters.

#### Scenario: Daily quote overlap produces unchanged rows
- **WHEN** a daily quote update re-fetches an overlap window and all fetched rows match existing semantic hashes
- **THEN** the scheduler report SHALL show unchanged rows
- **AND** it SHALL show zero changelog-written rows for those unchanged rows

#### Scenario: Futures official source replaces fallback rows
- **WHEN** a futures daily run replaces lower-priority fallback observations with official observations for dates already present
- **THEN** the report SHALL show the affected rows as source upgrades
- **AND** it SHALL NOT describe them as newly covered business dates

#### Scenario: Futures run adds a previously absent date
- **WHEN** a futures daily run writes observations for a trade date that had no prior resolved-scope coverage
- **THEN** the report SHALL show new business-date rows and the affected dates separately from source upgrades

#### Scenario: Post-cutoff verification changes no prices
- **WHEN** the nightly futures run verifies that provisional values are final without a semantic price change
- **THEN** the report SHALL show post-cutoff verified unchanged rows
- **AND** it SHALL NOT report those rows as corrections

## ADDED Requirements

### Requirement: Nightly futures jobs must reconcile provisional target dates
Scheduled and manual futures jobs running at or after an exchange publication cutoff SHALL reconcile publication-eligible provisional rows before determining final task status.

#### Scenario: Scheduled 21:30 run finds provisional current-date rows
- **WHEN** the configured 21:30 futures daily job finds provisional rows for the expected latest trading date
- **THEN** it SHALL include that date in provider processing even though persisted rows already exist
- **AND** it SHALL attempt to finalize the rows through the configured source hierarchy

#### Scenario: Nightly reconciliation succeeds
- **WHEN** every selected exchange finalizes or verifies its publication-eligible target dates
- **THEN** the task MAY report success when no other blocker remains
- **AND** the report SHALL show finalized dates and reconciliation counters by exchange

#### Scenario: Nightly reconciliation fails
- **WHEN** a selected exchange retains stale provisional rows after publication is due
- **THEN** the scheduler SHALL preserve a partial or blocked task status
- **AND** the report SHALL identify the exchange, stale dates, attempted sources, and blockers
- **AND** notification delivery or the presence of old rows SHALL NOT convert the task to success
