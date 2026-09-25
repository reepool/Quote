## ADDED Requirements

### Requirement: Production futures sync skips notice-closed dates
Production futures market-data sync SHALL request prices only for target trading dates produced by trading-day governance. Dates closed by an official holiday notice SHALL NOT be sent to an official daily price fetch.

#### Scenario: Repair window includes a holiday and a later trading day
- **WHEN** a production futures sync preflight covers a notice-closed holiday and a later verified trading day
- **THEN** the calendar preflight SHALL refresh holiday notices through the existing official calendar backfill
- **AND** price sync SHALL run for the trading day
- **AND** the notice-closed holiday SHALL NOT be fetched and SHALL NOT block the exchange

#### Scenario: Holiday notice is missing for a weekday
- **WHEN** the publication cutoff has passed and a weekday in the sync window has neither a holiday-notice closure nor an official daily file
- **THEN** that date SHALL remain unresolved
- **AND** the existing per-exchange calendar block SHALL still apply

## MODIFIED Requirements

### Requirement: Futures market-data tasks can skip calendar backfill without skipping trading-day governance

Futures market-data sync and backfill tasks SHALL support an operator option that skips official trading-calendar backfill while preserving trading-day governance over the task's requested date range.

#### Scenario: Historical backfill reuses verified stored calendar

- **Given** a futures market-data backfill is requested for an exchange with already backfilled official calendar rows
- **And** the request includes `skip_trading_calendar_backfill`
- **When** the task starts
- **Then** it SHALL NOT call the official calendar backfill preflight
- **AND** it SHALL NOT refresh holiday notices
- **AND** it SHALL still call trading-day governance for the requested `start/end` range
- **AND** price downloads SHALL use the governed target trading dates, excluding dates already closed by a stored official holiday notice.

#### Scenario: Trading-day governance is explicitly skipped

- **Given** a futures market-data task includes `skip_trading_day_governance`
- **When** the task starts
- **Then** the task MAY skip both official calendar backfill and trading-day governance
- **And** this mode SHALL remain a diagnostic override, not the production default.

#### Scenario: Operator report distinguishes the two calendar steps

- **Given** a futures market-data task is started manually
- **When** the task start acknowledgement is sent
- **Then** it SHALL show whether official calendar backfill is enabled
- **And** it SHALL separately show whether trading-day governance is enabled.
