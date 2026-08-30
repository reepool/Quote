# scheduler

## Purpose
This capability defines scheduler tasks and configuration contracts for composite maintenance workflows.
## Requirements
### Requirement: Composite Gap Detect and Repair Task
The scheduler SHALL provide a composite task that runs data gap detection and then triggers gap repair in the same execution.

#### Scenario: Scheduled run detects and repairs gaps
- **WHEN** the composite task is executed by the scheduler
- **THEN** the system detects gaps for the configured scope and starts repair for the detected gaps

### Requirement: Composite Task Configuration Parameters
The scheduler SHALL accept configuration parameters for the composite gap task, including exchanges, date range, and repair filters.

#### Scenario: Configuration limits scope
- **WHEN** exchanges and date range are configured for the composite task
- **THEN** the gap detection and repair are limited to the configured scope

### Requirement: Scheduler Jobs Must Support Configured Dependencies
The scheduler SHALL allow each configured job to declare pre-task and post-task dependencies in scheduler configuration without hard-coding those relationships inside business task methods.

#### Scenario: Job has no dependencies
- **WHEN** a scheduler job has no `dependencies` block
- **THEN** it SHALL run with the same behavior as before

#### Scenario: Job has configured dependencies
- **WHEN** a scheduler job declares `pre_success`, `post_success`, or `post_always` dependency groups
- **THEN** the scheduler dependency executor SHALL orchestrate those dependency groups around the parent job

#### Scenario: Dependency node is manual-only
- **WHEN** a dependency node references a job whose config has `manual_only=true`
- **THEN** the scheduler SHALL allow dependency-triggered execution while still avoiding standalone cron registration for that job

### Requirement: Dependency Execution Must Support Pre And Post Phases
The scheduler dependency executor SHALL support `pre_success`, `post_success`, and `post_always` phases with explicit success and failure semantics.

#### Scenario: Pre-success dependency succeeds
- **WHEN** all required `pre_success` dependencies complete successfully
- **THEN** the scheduler SHALL start the parent job

#### Scenario: Pre-success dependency fails
- **WHEN** a required `pre_success` dependency fails
- **THEN** the scheduler SHALL apply the configured failure policy and SHALL NOT silently start the parent job

#### Scenario: Parent succeeds
- **WHEN** the parent job succeeds
- **THEN** the scheduler SHALL run configured `post_success` dependency groups

#### Scenario: Parent fails
- **WHEN** the parent job fails
- **THEN** the scheduler SHALL NOT run `post_success` dependency groups
- **AND** it SHALL still run configured `post_always` groups

### Requirement: Dependency Groups Must Support Parallel And Serial Modes
The scheduler dependency executor SHALL support both parallel and serial dependency group execution.

#### Scenario: Parallel post tasks
- **WHEN** job A succeeds and has a `post_success` dependency group with `mode=parallel` containing jobs B and C
- **THEN** the scheduler SHALL start B and C concurrently

#### Scenario: Serial post tasks
- **WHEN** job A succeeds and has a `post_success` dependency group with `mode=serial` containing jobs B and C
- **THEN** the scheduler SHALL run B first and SHALL run C only after B succeeds or after policy permits continuation

#### Scenario: Serial task fails with stop-chain
- **WHEN** a serial dependency node fails and its failure policy is `stop_chain`
- **THEN** the scheduler SHALL skip later nodes in the same serial group

### Requirement: Dependency Nodes Must Support Explicit Parameter Inheritance
Dependency nodes SHALL inherit parent runtime parameters only when those parameter names are explicitly listed in the node `inherit` configuration.

#### Scenario: Parameter is inherited
- **WHEN** a dependency node declares `inherit=["exchanges", "dry_run"]`
- **THEN** the scheduler SHALL copy those runtime parent parameters into the dependency task parameters when present

#### Scenario: Parameter is not inherited
- **WHEN** a parent runtime parameter is not listed in `inherit`
- **THEN** the scheduler SHALL NOT implicitly pass that parameter to the dependency task

#### Scenario: Node parameter overrides inherited value
- **WHEN** a dependency node declares both inherited parameters and explicit `parameters`
- **THEN** node-level `parameters` SHALL override inherited values

### Requirement: Dependency Configuration Must Be Validated
The scheduler SHALL validate dependency configuration before using it.

#### Scenario: Unknown dependency job id
- **WHEN** a dependency references a job id that is not configured
- **THEN** dependency validation SHALL fail with an explicit diagnostic

#### Scenario: Dependency graph has a cycle
- **WHEN** configured dependencies create a direct or indirect cycle
- **THEN** dependency validation SHALL fail and SHALL NOT execute the cyclic graph

#### Scenario: Invalid dependency mode or failure policy
- **WHEN** a dependency group or node uses an unsupported mode or failure policy
- **THEN** dependency validation SHALL fail with an explicit diagnostic

### Requirement: Dependency Results Must Be Reported
The scheduler SHALL include dependency execution results in parent job reporting and logs.

#### Scenario: Dependency node runs
- **WHEN** a dependency node executes
- **THEN** the parent result SHALL include the dependency phase, group id, job id, status, elapsed time, inherited parameters, summary counters, and error summary when present

#### Scenario: Broker post task is migrated
- **WHEN** `financial_disclosure_incremental_sync` succeeds and configuration declares `broker_risk_control_incremental_sync` as a `post_success` dependency
- **THEN** the broker task SHALL run through the generic dependency executor and its result SHALL appear under dependency results rather than a task-specific hard-coded report field

### Requirement: Daily Quote Update Performs Bounded Catch-Up
The daily quote update task SHALL perform bounded catch-up for active tradable instruments whose local quote history is missing or recently behind after instrument master governance completes.

#### Scenario: Newly listed instrument has no local quotes
- **WHEN** a normal daily quote update runs for an instrument with no local quote rows and a `listed_date` on or before the target date
- **THEN** the system SHALL request daily quotes from the later of the instrument `listed_date` and the configured new-instrument catch-up lower bound through the target date
- **AND** it SHALL save returned quotes using the existing daily quote upsert path

#### Scenario: Newly listed instrument is discovered after its first trading day
- **WHEN** an instrument first enters the local active universe after one or more trading sessions have already occurred within the configured catch-up window
- **THEN** the next normal daily quote update SHALL include those prior sessions in the instrument's fetch window
- **AND** the daily update SHALL NOT require a manual backfill to cover those sessions

#### Scenario: Instrument has a recent short quote gap
- **WHEN** an instrument has a latest local quote date earlier than the normal daily update window and the missing span is within the configured short-gap catch-up window
- **THEN** the daily update SHALL request quotes from the bounded catch-up start through the target date
- **AND** duplicate dates already present locally SHALL remain safe under the existing quote upsert behavior

#### Scenario: Missing span exceeds catch-up limit
- **WHEN** an instrument's missing span starts before the configured catch-up lower bound
- **THEN** the daily update SHALL cap the request at the configured lower bound
- **AND** it SHALL expose that the catch-up window was capped so broader repair can be handled by gap repair workflows

#### Scenario: Catch-up is reported
- **WHEN** a daily update completes after evaluating catch-up windows
- **THEN** the structured update result and report SHALL include catch-up counters and representative samples for new-instrument catch-up, short-gap catch-up, capped windows, and catch-up quote rows

#### Scenario: Historical range backfill remains isolated
- **WHEN** a historical range backfill or explicit point-in-time quote backfill runs
- **THEN** the bounded daily catch-up behavior SHALL NOT force current master refresh beyond the existing historical-backfill policy
- **AND** it SHALL NOT replace the explicit date range requested by the operator

### Requirement: Jobs can declare master-governance requirements
Scheduler job configuration SHALL allow jobs to declare required master-governance scopes separately from generic task dependencies.

#### Scenario: Daily update declares master prerequisites
- **WHEN** `daily_data_update` is configured with master-governance requirements
- **THEN** the scheduler or task entry point SHALL pass those requirements to the master-governance orchestrator before quote targets are resolved
- **AND** the requirements SHALL be visible in configuration review

#### Scenario: Governance requirement is not a scheduler dependency node
- **WHEN** a job declares a master-governance requirement
- **THEN** the scheduler SHALL NOT require that requirement to correspond to a standalone cron job
- **AND** it SHALL execute the requirement through the governance orchestrator

### Requirement: Scheduler validates governance requirement configuration
The scheduler configuration loader SHALL validate master-governance requirement syntax and supported scopes.

#### Scenario: Unknown governance scope in job config
- **WHEN** scheduler configuration includes an unknown master-governance scope for a job
- **THEN** validation SHALL fail with an explicit diagnostic naming the job and scope

#### Scenario: Manual-only governance task remains callable
- **WHEN** a governance policy also has a manual-only task surface
- **THEN** the scheduler SHALL keep the manual task runnable through operator commands
- **AND** it SHALL NOT register it as a standalone cron job unless cron scheduling is explicitly enabled

### Requirement: Scheduler reports merged governance results
Scheduler and Telegram reports SHALL include normalized master-governance results for jobs that declare governance requirements.

#### Scenario: Governance runs before scheduled job
- **WHEN** a scheduled job executes one or more master-governance requirements
- **THEN** its report SHALL include top-level governance status plus child policy results
- **AND** warnings and errors from child policies SHALL remain visible to operators

#### Scenario: Governance is skipped for backfill
- **WHEN** a historical backfill skips current-master governance by policy
- **THEN** the report SHALL include the skip reason
- **AND** the skip SHALL NOT be reported as an upstream data failure

### Requirement: Futures scheduler jobs accept configured scopes
Futures scheduler jobs SHALL be able to reference one or more configured futures download scopes.

#### Scenario: Single scope daily update
- **WHEN** a futures daily update job is configured with `scope_ids=["gfex_all"]`
- **THEN** the scheduler SHALL pass that scope to the futures sync service
- **AND** the job SHALL NOT expand to other exchanges or categories

#### Scenario: Scope blocker occurs before network requests
- **WHEN** a scheduled futures job references an invalid or empty scope
- **THEN** the job SHALL fail with a structured scope blocker before provider requests are made

### Requirement: Task Report Delivery Must Not Block Task Lifecycle
The scheduler SHALL bound task report delivery time so Telegram or notification transport delays do not prevent a completed task from releasing scheduler state.

#### Scenario: Report delivery succeeds within timeout
- **WHEN** a task completes and report delivery succeeds before the configured timeout
- **THEN** the scheduler SHALL log report delivery success
- **AND** the task SHALL complete using its data-task result

#### Scenario: Report delivery exceeds timeout
- **WHEN** a task completes and report delivery exceeds the configured timeout
- **THEN** the scheduler SHALL log report delivery timeout
- **AND** the task SHALL still complete using its data-task result
- **AND** the scheduler SHALL release the task running state

#### Scenario: Report delivery raises an exception
- **WHEN** a task completes and report delivery raises an exception
- **THEN** the scheduler SHALL log the report delivery failure
- **AND** the task SHALL still complete using its data-task result unless the data task itself failed

### Requirement: Futures Jobs Must Declare Trading Day Governance Prerequisite
The scheduler SHALL run or validate futures trading-day governance before commodity futures market-data jobs perform production provider fetches or writes.

#### Scenario: Daily futures market-data job starts
- **WHEN** the scheduled futures market-data daily job starts
- **THEN** it SHALL refresh or validate trading-day governance for target exchanges before provider fetches
- **AND** it SHALL pass the governed target trading dates into the futures sync service

#### Scenario: Historical futures backfill job starts
- **WHEN** an operator schedules a futures historical backfill
- **THEN** the scheduler or job runner SHALL require trading-day governance expansion for the requested date range before enqueueing provider work

#### Scenario: Governance prerequisite fails
- **WHEN** trading-day governance reports missing calendar coverage, unresolved conflicts, or quality below the configured production threshold
- **THEN** the scheduled production write job SHALL stop before provider fetches or writes
- **AND** the task report SHALL identify the exchange, date range, quality issue, and review-required records

### Requirement: Scheduler Reports Must Distinguish Calendar Skips From Data Failures
The scheduler SHALL report governed non-trading-day skips separately from provider errors and market-data gaps.

#### Scenario: Job range contains holidays
- **WHEN** a scheduled futures job covers governed holidays or rest days
- **THEN** the task report SHALL count those dates as calendar skips
- **AND** it SHALL NOT count them as failed provider requests

#### Scenario: Provider returns empty data on a governed trading day
- **WHEN** a provider returns no futures data for a governed trading day
- **THEN** the task report SHALL classify the result as a provider/data-quality issue rather than a calendar skip

### Requirement: Scheduler Must Support Dry-run With Calendar Warnings
The scheduler SHALL allow explicit futures dry-run jobs to continue with estimated or partially verified calendars when the output reports the governance risk.

#### Scenario: Dry-run uses estimated calendar
- **WHEN** a scheduled or manual futures dry-run uses estimated calendar rows
- **THEN** the scheduler SHALL allow the dry-run if dry-run policy permits it
- **AND** the task report SHALL clearly mark the run as not production-ready because of calendar quality

### Requirement: Scheduler exposes futures official calendar backfill
The scheduler SHALL expose a bounded operator task for official futures trading-calendar backfill separate from price backfill.

#### Scenario: Calendar backfill task runs
- **WHEN** the futures official calendar backfill scheduler task is invoked
- **THEN** it SHALL run calendar verification and write calendar rows only, without writing futures price bars

#### Scenario: Calendar backfill reports unresolved gaps
- **WHEN** the calendar backfill encounters unverified exchange dates
- **THEN** the scheduler report SHALL show unresolved counts and source failure counts separately from rows written

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

### Requirement: Annual-Report Scheduling Must Use Shared Asset Jobs Only
Scheduled and manual annual-report discovery, download, repair, integrity, and latest-report backfill SHALL execute only through shared announcement asset application services and task adapters.

#### Scenario: Daily annual-report maintenance runs
- **WHEN** the configured annual-report daily job starts
- **THEN** it SHALL invoke the shared announcement asset daily workflow
- **AND** no legacy business-profile archive synchronization job SHALL run

#### Scenario: A consumer requires missing coverage
- **WHEN** business-profile or broker processing reports a missing shared asset
- **THEN** an operator or dependency SHALL invoke the shared ensure/backfill operation
- **AND** the consumer SHALL resume from the shared asset after it becomes ready

#### Scenario: Legacy job id is requested
- **WHEN** a caller requests a retired annual-report archive sync job or command
- **THEN** command resolution SHALL reject it as unavailable rather than silently executing duplicate acquisition logic

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
