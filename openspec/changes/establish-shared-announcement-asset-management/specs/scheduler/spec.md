## ADDED Requirements

### Requirement: Annual-Report Asset Backfill Is An Independent Operator Job
The scheduler SHALL expose an annual-report latest-asset backfill job that is independent from business-profile and broker processing jobs.

#### Scenario: Operator starts the bootstrap
- **WHEN** the operator runs the annual-report asset backfill
- **THEN** it SHALL target the configured active SSE, SZSE, and BSE stock universe
- **AND** it SHALL discover and store only each instrument's latest available effective annual-report attachment

#### Scenario: Business consumers are disabled
- **WHEN** business-profile and broker risk-control tasks are disabled
- **THEN** the annual-report asset backfill SHALL remain runnable and SHALL retain its own progress and result reporting

#### Scenario: Backfill is resumed
- **WHEN** a prior backfill stopped with incomplete windows, pending instruments, or retryable downloads
- **THEN** a subsequent execution SHALL resume durable work without reprocessing verified completed assets

### Requirement: Annual-Report Asset Daily Update Has Its Own Schedule
The scheduler SHALL provide a daily annual-report asset update with configuration and state independent from consuming business tasks.

#### Scenario: Daily job is enabled
- **WHEN** annual-report asset daily scheduling is enabled
- **THEN** the scheduler SHALL register the configured cron trigger regardless of business-profile rollout state

#### Scenario: Daily job is disabled
- **WHEN** annual-report asset daily scheduling is disabled
- **THEN** manual backfill, manual daily update, readiness queries, and on-demand ensure SHALL remain available

#### Scenario: Daily update discovers a correction
- **WHEN** a complete corrected annual report appears in the discovery window
- **THEN** the daily job SHALL ensure the corrected attachment and execute the governed effective-version replacement workflow

#### Scenario: A correction is indexed outside normal overlap
- **WHEN** a provider exposes a correction after its publication time has fallen outside the daily overlap
- **THEN** a separately bounded long-lookback reconciliation cohort SHALL discover it within the configured maximum period

#### Scenario: Active stock universe changes
- **WHEN** stocks list, delist, or change active status after bootstrap
- **THEN** the daily task SHALL use a refreshed auditable universe snapshot for coverage and repair
- **AND** a delisting SHALL NOT trigger attachment deletion

### Requirement: Annual-Report Jobs Are Bounded And Observable
Annual-report scheduler jobs SHALL use explicit network, time, storage, and work-volume bounds and SHALL report progress by acquisition stage.

#### Scenario: Configured bounds are reached
- **WHEN** a run reaches a page, request, window, instrument, byte, elapsed-time, or storage limit
- **THEN** it SHALL stop or checkpoint at a safe boundary
- **AND** it SHALL return partial or blocked status rather than claim complete coverage

#### Scenario: One publication day exceeds page bounds
- **WHEN** a dense day cannot complete in one run
- **THEN** the job SHALL resume stable page ranges or provider-supported subscopes under a fixed cutoff
- **AND** it SHALL not advance the parent cursor until every child scope completes

#### Scenario: Job completes
- **WHEN** a backfill or daily run completes
- **THEN** the scheduler report SHALL include discovery, local reuse, download, correction, deletion, retry, coverage, storage, backup, and elapsed-time metrics

#### Scenario: Concurrent execution is attempted
- **WHEN** another scheduled or manual instance targets the same annual-report operation scope
- **THEN** scheduler and asset leases SHALL prevent duplicate active acquisition while reporting the existing run identity

### Requirement: Consumer Processing Can Depend On Asset Readiness
Scheduler dependency configuration SHALL allow consuming business jobs to require annual-report asset availability without embedding asset downloads inside those jobs.

#### Scenario: Broker task processes newly discovered annual reports
- **WHEN** annual-report daily update succeeds and configuration declares broker processing as a post-success dependency
- **THEN** broker risk-control SHALL process affected shared asset ids rather than rescan or redownload the announcements

#### Scenario: Asset update is partial
- **WHEN** annual-report daily update ends partial with valid completed assets and retryable failures
- **THEN** dependency policy SHALL determine whether consumers process only completed affected assets or wait
- **AND** the decision and skipped scopes SHALL be reported
