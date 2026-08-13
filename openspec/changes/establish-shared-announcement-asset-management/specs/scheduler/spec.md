## ADDED Requirements

### Requirement: Annual-Report Asset Backfill Is An Independent Operator Job
The scheduler SHALL expose `annual_report_asset_latest_backfill` as an annual-report latest-asset operator job that is independent from business-profile and broker processing jobs.

#### Scenario: Scheduler configuration is loaded before rollout
- **WHEN** the new annual-report asset job definitions first appear in production configuration and templates
- **THEN** latest-backfill SHALL be `enabled=false`, `manual_only=true`, and have no cron trigger
- **AND** daily update and archive backup SHALL each support cron plus manual invocation but default to `enabled=false`
- **AND** integrity audit SHALL be manual or low-frequency
- **AND** integrity audit SHALL be read-only by default
- **AND** destructive cleanup SHALL be separately disabled
- **AND** destructive cleanup SHALL require operator authorization
- **AND** their enablement SHALL NOT inherit business-profile or broker switches

#### Scenario: Annual-report configuration contract is loaded
- **WHEN** production configuration, the configuration template, and the shared schema are loaded
- **THEN** they SHALL round-trip and validate active exchanges, instrument type/status, `dry_run`, initial lookback, provider/classifier policy, versioned `universe_refresh_cadence`, master/census freshness, overlap, `reconciliation_lookback_days`, `reconciliation_cohort_size`, `reconciliation_max_cycle_days`, `missing_repair_cohort_size`, targeted-repair request/instrument/elapsed bounds, download/per-source concurrency, rate limits, storage and per-task byte limits, lease/retry policy, backup target/freshness, runtime `max_unprotected_bytes`/`max_unprotected_age` with accumulation/reset semantics, version 1 provisional-result policy, consumer dependency policy, and rollout gates
- **AND** all reconciliation, repair, and targeted-repair bounds SHALL be positive, enter the normalized configuration fingerprint, and drive degraded/blocked readiness when the maximum reconciliation cycle is exceeded
- **AND** missing, invalid, or unsafe values SHALL fail closed before scheduler registration
- **AND** the normalized configuration fingerprint SHALL be persisted with each job operation and handoff decision

#### Scenario: Operator starts the bootstrap
- **WHEN** the operator runs the annual-report asset backfill
- **THEN** it SHALL target the configured active SSE, SZSE, and BSE stock universe
- **AND** it SHALL publish, persistently retain, and count coverage only for each instrument's latest available effective annual-report attachment, while independently adopted older local assets remain outside bootstrap coverage
- **AND** a versioned candidate-verification policy MAY read bounded competing bytes before winner selection
- **AND** the policy SHALL persist only immutable verification evidence for non-winners
- **AND** the policy SHALL count non-winner bytes separately from canonical acquisition/coverage

#### Scenario: Business consumers are disabled
- **WHEN** business-profile and broker risk-control tasks are disabled
- **THEN** the annual-report asset backfill SHALL remain runnable
- **AND** the annual-report asset backfill SHALL retain its own progress and result reporting

#### Scenario: Backfill is resumed
- **WHEN** a prior backfill stopped with incomplete windows, pending instruments, or retryable downloads
- **THEN** a subsequent execution SHALL resume durable work without reprocessing verified completed assets

#### Scenario: Bootstrap completes before daily enablement
- **WHEN** latest-only bootstrap and readiness gates complete successfully
- **THEN** the scheduler SHALL persist a bootstrap-to-daily handoff cutoff and compatible per-source coverage watermarks
- **AND** daily enablement SHALL use those watermarks with overlap so no publication interval is skipped

### Requirement: Annual-Report Asset Daily Update Has Its Own Schedule
The scheduler SHALL provide `annual_report_asset_daily_update` with configuration and state independent from consuming business tasks.

#### Scenario: Daily cadence and a missed run are governed
- **WHEN** the daily-update schedule is configured
- **THEN** it SHALL use the project timezone
- **AND** the default schedule SHALL admit at least one eligible run in every project calendar day
- **AND** the schedule SHALL allow a tighter cadence when explicitly configured
- **AND** the persisted schedule SHALL retain cadence, timezone, cutoff, overlap, and configuration fingerprint
- **WHEN** one or more scheduled runs are missed or fail before committing a complete window
- **THEN** the next run SHALL resume from the last committed per-scope watermark with the configured overlap and a bounded catch-up window
- **AND** it SHALL not silently skip the missed publication interval, replace the denominator with an empty result, or turn catch-up into an unbounded historical rescan

#### Scenario: Daily job is enabled
- **WHEN** annual-report asset daily scheduling is enabled
- **THEN** the scheduler SHALL register the configured cron trigger regardless of business-profile rollout state

#### Scenario: Daily enablement is requested before bootstrap readiness
- **WHEN** bootstrap coverage/handoff, local integrity, storage reserve, backup configuration, or asset-adoption promotion gates have not passed
- **THEN** scheduled daily enablement SHALL fail closed or remain disabled with explicit blockers
- **AND** local reads, manual bounded operations, and on-demand ensure SHALL remain available
- **AND** a completely proven `overdue_missing` period SHALL degrade readiness but SHALL NOT block daily discovery under the default version 1 policy, because the daily job must remain able to discover the delayed filing

#### Scenario: Daily enablement has passed the initial backup gate
- **WHEN** the daily cron is enabled for the first time
- **THEN** the independent backup failure-domain identity, backup job operability, initial required-blob protection, and paired catalog/file watermark freshness SHALL be verified before the cron is admitted
- **AND** a runtime backup failure after enablement SHALL keep metadata discovery and verified local reads available
- **AND** deletion and cleanup SHALL stop during that runtime backup failure
- **AND** new attachment acquisition SHALL continue only while configured unprotected-byte/age limits remain within policy
- **AND** crossing those limits SHALL block scheduled attachment writes
- **AND** readiness SHALL expose the blocker

#### Scenario: Business consumers have not migrated
- **WHEN** business-profile, broker risk-control, or both still use their legacy consumer paths after the asset bootstrap is ready
- **THEN** shared annual-report daily scheduling SHALL remain independently enableable
- **AND** shared annual-report daily scheduling SHALL continue discovery
- **AND** shared annual-report daily scheduling SHALL continue proactive attachment acquisition
- **AND** shared annual-report daily scheduling SHALL continue correction replacement
- **AND** shared annual-report daily scheduling SHALL continue emitting asset events
- **AND** incomplete consumer migration SHALL block only that consumer's cutover and legacy cleanup, not asset backfill or daily maintenance

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
- **THEN** the daily task SHALL attempt refresh or revalidation before each run according to the configured versioned cadence, persist attempted/effective refresh time and paired master/census snapshot id, and use the resulting auditable snapshot for coverage and repair
- **AND** a delisting SHALL NOT trigger attachment deletion

#### Scenario: Active-universe refresh is stale or incomplete
- **WHEN** master-data refresh fails, exceeds its freshness limit, or leaves eligibility-indeterminate instruments
- **THEN** the daily task SHALL retain the last complete acceptable denominator rather than replace it with an empty or partial snapshot
- **AND** market discovery MAY continue while coverage/readiness reports expose snapshot age, refresh failure, and indeterminate count
- **AND** the scheduler SHALL NOT report complete full-market coverage until an acceptable complete snapshot is restored

### Requirement: Annual-Report Jobs Are Bounded And Observable
Annual-report scheduler jobs SHALL use explicit network, time, storage, and work-volume bounds.

Annual-report scheduler jobs SHALL report progress by acquisition stage.

#### Scenario: Configured bounds are reached
- **WHEN** a run reaches a page, request, window, instrument, byte, elapsed-time, or storage limit
- **THEN** it SHALL stop or checkpoint at a safe boundary
- **AND** it SHALL return partial or blocked status rather than claim complete coverage

#### Scenario: One publication day exceeds page bounds
- **WHEN** a dense day cannot complete in one run
- **THEN** the job SHALL resume stable page ranges or provider-supported subscopes under a fixed cutoff
- **AND** it SHALL not advance the parent cursor until every child scope completes

#### Scenario: Dense publication day reaches the provider page cap
- **WHEN** a fixture contains exactly 1,500 distinct announcement ids for one publication day and the provider returns at most 600 records per page or continuation scope
- **THEN** the job SHALL persist and resume enough child pages or stable subscopes to register exactly 1,500 distinct ids without loss or duplicate coverage credit
- **AND** the parent day's cursor and range `covered_until` SHALL remain unadvanced until every required child has committed successfully

#### Scenario: A complete publication window has no records
- **WHEN** all required source scopes complete successfully through the run cutoff without an in-range result
- **THEN** the job SHALL still advance the range-coverage watermark to the cutoff
- **AND** it SHALL report a successful empty window separately from an incomplete window

#### Scenario: Job completes
- **WHEN** a backfill or daily run completes
- **THEN** the scheduler report SHALL include discovery, local reuse, download, correction, silent-update verification/limitations, deletion, retry, coverage, storage, backup, and elapsed-time metrics

#### Scenario: Concurrent execution is attempted
- **WHEN** another scheduled or manual instance targets the same annual-report operation scope and versioned acquisition work fingerprint
- **THEN** scheduler and asset leases SHALL prevent duplicate active acquisition while reporting the existing run identity
- **AND** a trigger with a different route, classifier/integrity policy, configuration, or accepted work/network/storage bound SHALL NOT be represented as the same work; it SHALL be serialized, queued, or rejected explicitly according to policy

### Requirement: Annual-Report Jobs Have A Durable Operator Control Plane
Manual and scheduled backfill, daily-update, integrity, and backup execution SHALL use one shared command service backed by durable operation state rather than process-local task state.

Existing scheduler task registration, operator CLI, and any operator HTTP adapter SHALL call that service.

Those adapters SHALL use the shared service's durable run semantics.

Manual commands SHALL require an `annual_report_assets:operate`-equivalent scope.

Service-scheduled commands SHALL use an auditable configured service principal.

The configured service-principal identity and configuration version SHALL be reloadable.

Existing operation history SHALL remain unchanged by such reload.

#### Scenario: Operator starts a manual job
- **WHEN** an authorized operator starts a bounded annual-report job with a validated scope and policy
- **THEN** the control plane SHALL return the durable asset operation id as `run_id`, normalized scope, accepted bounds, and current status
- **AND** adapters SHALL use that durable operation id as the sole run identity
- **AND** the command, authenticated principal, effective permission, request fingerprint, accepted configuration version, and start time SHALL be auditable

#### Scenario: The same job scope is already active
- **WHEN** a manual, API, or cron trigger targets an equivalent active normalized scope and acquisition work fingerprint
- **THEN** it SHALL return the existing durable operation identity rather than start duplicate work
- **AND** caller idempotency and operation work fingerprints SHALL remain distinct, and an incompatible fingerprint SHALL NOT inherit the existing run identity

#### Scenario: Operator requests a cooperative stop
- **WHEN** an authorized operator stops a cancellable batch job
- **THEN** the worker SHALL checkpoint at a safe boundary, release or expire its lease, and preserve completed windows and verified assets
- **AND** status SHALL become `cancelled` without rolling back committed coverage
- **AND** a non-cancellable stage SHALL reject the stop explicitly
- **AND** stopping the batch SHALL stop creation of new child work
- **AND** an already-shared child acquisition SHALL remain active while it still has another principal or consumer subscription

#### Scenario: Process restarts with a stale job heartbeat
- **WHEN** a run remains non-terminal after its heartbeat and lease expire
- **THEN** status and checkpoints SHALL remain queryable
- **AND** an authorized resume SHALL reclaim the same operation id
- **AND** the resumed operation SHALL increment its attempt/resume generation
- **AND** the resumed operation SHALL record the resuming principal or service identity
- **AND** restart SHALL NOT create a second logical run or repeat verified acquisition

#### Scenario: Operator queries run history
- **WHEN** an authorized operator queries scheduler health
- **THEN** the service SHALL return retained recent runs, last successful cutoff by scope, active heartbeat age, consecutive failures, cursor lag, oldest retry age, terminal outcome, and bounded diagnostics
- **AND** stale heartbeat, lag, failure-count, storage, and backup thresholds SHALL produce explicit readiness blockers or alerts
- **AND** a front-facing readiness summary SHALL omit provider-sensitive and filesystem diagnostics exposed only to operators

#### Scenario: A non-operator invokes the control plane
- **WHEN** a caller lacks the operator scope for start, stop, resume, detailed history, repair, or backup control
- **THEN** the command SHALL fail before creating or mutating an operation
- **AND** the response SHALL follow the configured 401/403/404 non-disclosure policy

### Requirement: Annual-Report Integrity And Backup Have Governed Jobs
The scheduler SHALL expose independently configured `annual_report_asset_integrity_audit` and `annual_report_asset_backup` jobs in addition to latest-backfill and daily update.

#### Scenario: Integrity audit runs
- **WHEN** an operator or low-frequency schedule starts integrity audit
- **THEN** it SHALL default to read-only verification
- **AND** each repair, quarantine, move, link, or deletion action SHALL require explicit bounded authorization

#### Scenario: A destructive audit action is not explicitly authorized
- **WHEN** a repair request lacks operator scope, lacks the separate action flag for its requested network repair, quarantine, link, move, or deletion, targets a scope broader than the configured bound, or arrives while the trusted identity boundary is unavailable
- **THEN** the command SHALL fail before creating or mutating an operation and SHALL perform zero provider requests, file changes, catalog changes, or watermark advancement
- **AND** read-only audit SHALL remain non-destructive: it SHALL NOT change catalog business state or any inspected file's bytes, hash, modification time, or permissions

#### Scenario: Archive backup runs
- **WHEN** the configured archive-backup job starts
- **THEN** it SHALL use the shared durable operation and independent-failure-domain backup contracts
- **AND** a backup failure SHALL degrade readiness without causing a local asset to become invalid

### Requirement: Consumer Processing Can Depend On Asset Readiness
Scheduler dependency configuration SHALL allow consuming business jobs to require annual-report asset availability.

Consuming business jobs SHALL obtain required annual reports through that dependency rather than embedded asset downloads.

The dependency policy SHALL be versioned with the scheduler configuration.

The dependency decision SHALL be persisted in the durable parent result.

Completed affected asset ids SHALL be persisted in the durable parent result.

Skipped scopes SHALL be persisted in the durable parent result.

#### Scenario: Broker task processes newly discovered annual reports
- **WHEN** annual-report daily update succeeds and configuration declares broker processing as a post-success dependency
- **THEN** broker risk-control SHALL process affected shared asset ids rather than rescan or redownload the announcements

#### Scenario: Asset update is partial
- **WHEN** annual-report daily update ends partial with valid completed assets and retryable failures
- **THEN** dependency policy SHALL determine whether consumers process only completed affected assets or wait
- **AND** the decision and skipped scopes SHALL be reported

#### Scenario: Consumer processing fails after asset completion
- **WHEN** an asset window and its effective decisions are committed but a consumer is disabled, cannot enqueue, fails parsing, or falls behind its checkpoint
- **THEN** only that consumer's continuation, processing state, checkpoint, and consumer readiness SHALL change
- **AND** the failure SHALL NOT regress the asset cursor, revoke the effective winner, rewrite the asset operation result, downgrade asset-layer readiness, or prevent another consumer from using the verified asset

#### Scenario: Same-batch correction is not yet verifiable
- **WHEN** one daily batch discovers an original and a legally later correction but the correction attachment cannot yet be acquired or verified
- **THEN** the daily result SHALL remain partial or blocked
- **AND** consumer enqueue SHALL remain suppressed for the unverified correction
- **AND** the correction SHALL remain durably retryable or blocked
- **AND** version 1 SHALL project an existing default-effective original result as `stale` with reason `pending_correction`, exclude it from DCF contracts requiring current facts, and preserve exact-observation/knowledge-cutoff results only when the correction lies outside their evidence scope
- **AND** once the correction verifies and activates, its replacement event SHALL drive the declared consumer reprocessing policy
