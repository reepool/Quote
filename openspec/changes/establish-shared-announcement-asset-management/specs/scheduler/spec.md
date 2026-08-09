## ADDED Requirements

### Requirement: Annual-Report Asset Backfill Is An Independent Operator Job
The scheduler SHALL expose `annual_report_asset_latest_backfill` as an annual-report latest-asset operator job that is independent from business-profile and broker processing jobs.

#### Scenario: Scheduler configuration is loaded before rollout
- **WHEN** the new annual-report asset job definitions first appear in production configuration and templates
- **THEN** latest-backfill SHALL be manual-only and daily, integrity, backup, and destructive-cleanup execution SHALL default disabled
- **AND** their enablement SHALL NOT inherit business-profile or broker switches

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

#### Scenario: Bootstrap completes before daily enablement
- **WHEN** latest-only bootstrap and readiness gates complete successfully
- **THEN** the scheduler SHALL persist a bootstrap-to-daily handoff cutoff and compatible per-source coverage watermarks
- **AND** daily enablement SHALL use those watermarks with overlap so no publication interval is skipped

### Requirement: Annual-Report Asset Daily Update Has Its Own Schedule
The scheduler SHALL provide `annual_report_asset_daily_update` with configuration and state independent from consuming business tasks.

#### Scenario: Daily job is enabled
- **WHEN** annual-report asset daily scheduling is enabled
- **THEN** the scheduler SHALL register the configured cron trigger regardless of business-profile rollout state

#### Scenario: Daily enablement is requested before bootstrap readiness
- **WHEN** bootstrap coverage, local integrity, storage reserve, backup, or migration gates have not passed
- **THEN** scheduled daily enablement SHALL fail closed or remain disabled with explicit blockers
- **AND** local reads, manual bounded operations, and on-demand ensure SHALL remain available

#### Scenario: Daily job is disabled
- **WHEN** annual-report asset daily scheduling is disabled
- **THEN** manual backfill, manual daily update, readiness queries, and on-demand ensure SHALL remain available

#### Scenario: Daily update discovers a correction
- **WHEN** a complete corrected annual report appears in the discovery window
- **THEN** the daily job SHALL ensure the corrected attachment and execute the governed effective-version replacement workflow

#### Scenario: Daily update discovers an original annual report
- **WHEN** the daily job selects a new effective complete original annual report
- **THEN** it SHALL proactively acquire and validate that attachment in version 1 rather than wait for a business consumer

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

#### Scenario: A complete publication window has no records
- **WHEN** all required source scopes complete successfully through the run cutoff without an in-range result
- **THEN** the job SHALL still advance the range-coverage watermark to the cutoff
- **AND** it SHALL report a successful empty window separately from an incomplete window

#### Scenario: Job completes
- **WHEN** a backfill or daily run completes
- **THEN** the scheduler report SHALL include discovery, local reuse, download, correction, deletion, retry, coverage, storage, backup, and elapsed-time metrics

#### Scenario: Concurrent execution is attempted
- **WHEN** another scheduled or manual instance targets the same annual-report operation scope
- **THEN** scheduler and asset leases SHALL prevent duplicate active acquisition while reporting the existing run identity

### Requirement: Annual-Report Jobs Have A Durable Operator Control Plane
Manual and scheduled backfill, daily-update, integrity, and backup execution SHALL use one shared command service backed by durable operation state rather than process-local task state. Existing scheduler task registration, operator CLI, and any operator HTTP adapter SHALL call that service and SHALL NOT create independent run semantics. Manual commands SHALL require an `annual_report_assets:operate`-equivalent scope, while service-scheduled commands SHALL use an auditable configured service principal whose identity/configuration version is reloadable without rewriting operation history.

#### Scenario: Operator starts a manual job
- **WHEN** an authorized operator starts a bounded annual-report job with a validated scope and policy
- **THEN** the control plane SHALL return the durable asset operation id as `run_id`, normalized scope, accepted bounds, and current status; adapters SHALL NOT invent a second process-local run identity
- **AND** the command, authenticated principal, effective permission, request fingerprint, accepted configuration version, and start time SHALL be auditable

#### Scenario: The same job scope is already active
- **WHEN** a manual, API, or cron trigger targets an equivalent active normalized scope and policy
- **THEN** it SHALL return the existing durable operation identity rather than start duplicate work

#### Scenario: Operator requests a cooperative stop
- **WHEN** an authorized operator stops a cancellable batch job
- **THEN** the worker SHALL checkpoint at a safe boundary, release or expire its lease, and preserve completed windows and verified assets
- **AND** status SHALL become `cancelled` without rolling back committed coverage; a non-cancellable stage SHALL reject the stop explicitly
- **AND** stopping the batch SHALL stop creation of new child work but SHALL NOT cancel an already-shared child acquisition that still has another principal or consumer subscription

#### Scenario: Process restarts with a stale job heartbeat
- **WHEN** a run remains non-terminal after its heartbeat and lease expire
- **THEN** status and checkpoints SHALL remain queryable and an authorized resume SHALL reclaim the same operation id, increment its attempt/resume generation, and record the resuming principal or service identity
- **AND** restart SHALL NOT create a second logical run or repeat verified acquisition

#### Scenario: Operator queries run history
- **WHEN** an authorized operator queries scheduler health
- **THEN** the service SHALL return retained recent runs, last successful cutoff by scope, active heartbeat age, consecutive failures, cursor lag, oldest retry age, terminal outcome, and bounded diagnostics
- **AND** stale heartbeat, lag, failure-count, storage, and backup thresholds SHALL produce explicit readiness blockers or alerts
- **AND** a front-facing readiness summary SHALL omit provider-sensitive and filesystem diagnostics exposed only to operators

#### Scenario: A non-operator invokes the control plane
- **WHEN** a caller lacks the operator scope for start, stop, resume, detailed history, repair, or backup control
- **THEN** the command SHALL fail before creating or mutating an operation and SHALL follow the configured 401/403/404 non-disclosure policy

### Requirement: Annual-Report Integrity And Backup Have Governed Jobs
The scheduler SHALL expose independently configured `annual_report_asset_integrity_audit` and `annual_report_asset_backup` jobs in addition to latest-backfill and daily update.

#### Scenario: Integrity audit runs
- **WHEN** an operator or low-frequency schedule starts integrity audit
- **THEN** it SHALL default to read-only verification and SHALL require explicit bounded authorization for repair, quarantine, move, link, or deletion

#### Scenario: Archive backup runs
- **WHEN** the configured archive-backup job starts
- **THEN** it SHALL use the shared durable operation and independent-failure-domain backup contracts
- **AND** a backup failure SHALL degrade readiness without causing a local asset to become invalid

### Requirement: Consumer Processing Can Depend On Asset Readiness
Scheduler dependency configuration SHALL allow consuming business jobs to require annual-report asset availability without embedding asset downloads inside those jobs.

#### Scenario: Broker task processes newly discovered annual reports
- **WHEN** annual-report daily update succeeds and configuration declares broker processing as a post-success dependency
- **THEN** broker risk-control SHALL process affected shared asset ids rather than rescan or redownload the announcements

#### Scenario: Asset update is partial
- **WHEN** annual-report daily update ends partial with valid completed assets and retryable failures
- **THEN** dependency policy SHALL determine whether consumers process only completed affected assets or wait
- **AND** the decision and skipped scopes SHALL be reported
