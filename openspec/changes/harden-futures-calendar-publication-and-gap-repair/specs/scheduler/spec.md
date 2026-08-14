## ADDED Requirements

### Requirement: Futures daily jobs must run bounded calendar and price-gap repair
Scheduled and manual futures daily jobs SHALL run publication-aware calendar repair for the configured three-to-five-natural-day window before final target-date expansion and provider processing.

#### Scenario: Scheduled daily job starts
- **WHEN** the scheduled futures daily job starts without an explicit date range
- **THEN** it SHALL repair the bounded recent calendar window for each selected exchange
- **AND** it SHALL pass repaired governed target dates and recent uncovered dates into the futures sync service

#### Scenario: Manual daily job starts before an exchange cutoff
- **WHEN** an operator invokes the daily task before a selected exchange's configured publication cutoff
- **THEN** the job SHALL use that exchange's latest publication-eligible trading date
- **AND** it SHALL report the cutoff and target date used

#### Scenario: Manual job supplies explicit dates
- **WHEN** an operator invokes the task with explicit `start` and `end` dates
- **THEN** the scheduler SHALL preserve the explicit inclusive request range
- **AND** it SHALL still apply calendar evidence and completeness checks within that range

### Requirement: Futures task reports must expose per-exchange targets and freshness
Scheduled and manual futures reports SHALL render the exchange-level diagnostic contract returned by governance and market-data sync.

#### Scenario: Futures job completes
- **WHEN** a futures daily or bounded sync job finishes
- **THEN** the report SHALL show each exchange's requested range, governed target dates, expected latest trading date, actual latest persisted price date, repaired gaps, remaining missing dates, and blockers
- **AND** calendar skips, lifecycle skips, unchanged persisted rows, and data failures SHALL remain distinguishable

#### Scenario: One exchange remains stale
- **WHEN** one selected exchange remains behind its expected latest trading date after the job
- **THEN** the task and report SHALL show `partial` when other useful work completed
- **AND** the report SHALL NOT show overall `success`

#### Scenario: Daily job has complete unchanged data
- **WHEN** all selected exchanges already cover every required target date and fetched rows are unchanged
- **THEN** the task MAY report `success`
- **AND** zero inserted rows SHALL NOT by itself be treated as a gap or failure

### Requirement: Scheduler must preserve non-success governance outcomes
The scheduler SHALL derive the final futures task status from calendar governance, provider processing, and exchange completeness rather than from task execution or report delivery alone.

#### Scenario: Dry-run contains governance warnings that block production readiness
- **WHEN** a scheduled or manual dry-run continues with unresolved or below-threshold calendar evidence
- **THEN** the final task status SHALL remain non-success
- **AND** the report SHALL identify the affected exchange and dates

#### Scenario: Calendar repair remains unresolved after cutoff
- **WHEN** publication is due and the bounded repair step cannot classify a required exchange date
- **THEN** the scheduler SHALL preserve the returned `blocked` or `partial` outcome
- **AND** it SHALL NOT convert the outcome to `success` because the orchestration completed normally
