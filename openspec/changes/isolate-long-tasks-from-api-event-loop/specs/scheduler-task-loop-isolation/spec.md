## ADDED Requirements

### Requirement: Long task execution must not starve the shared event loop
The system SHALL execute scheduler task coroutines off the API/Telegram shared event loop on a dedicated worker event loop per run, so that any registered long task can run to completion while the API and the bot remain responsive; jobs explicitly configured to stay on the main loop (documented escape hatch) are exempt and their known impact SHALL be recorded. The isolation SHALL stay at the scheduler wrapper layer; business task implementations SHALL NOT be duplicated or relocated.

#### Scenario: Long task runs while API serves reads
- **WHEN** any registered long task is running on the isolated worker execution path (e.g. financial_disclosure_reconciliation_sync scanning the financial database)
- **THEN** API health and read endpoints SHALL respond within the configured latency SLO (defaults: health 500ms, reads p95 1s over any 10-minute window)
- **AND** the API accept backlog SHALL NOT grow unboundedly
- **AND** a job configured via the escape hatch to stay on the main loop SHALL be exempt from this SLO with its known impact recorded

#### Scenario: Long task runs while bot serves commands
- **WHEN** a long task is running and an operator issues /run, /stop or /status
- **THEN** the bot SHALL acknowledge the command within the configured SLO (default 10s)

#### Scenario: Isolation stays at the wrapper layer
- **WHEN** the execution isolation is applied
- **THEN** task functions and business services SHALL keep a single implementation and unchanged semantics
- **AND** the scheduler wrapper SHALL remain the only execution-location change
- **AND** business services SHALL only add await checkpoints and progress reporting as defined by this capability

### Requirement: Stop and timeout must be enforced under isolation
The system SHALL enforce /stop and max_runtime_seconds against the worker execution such that cancellation takes effect at the task's next await checkpoint within a configured bound, even while the task is inside a long synchronous phase.

#### Scenario: Operator stops a running long task
- **WHEN** /stop is issued for a task running in the worker
- **THEN** the worker task SHALL be cancelled at its next checkpoint within the configured bound (default 30s)
- **AND** the task SHALL be reported as stopped

#### Scenario: max_runtime_seconds elapses
- **WHEN** a task exceeds its configured max_runtime_seconds
- **THEN** the run SHALL be cancelled within the same bound
- **AND** the failure report SHALL identify the timeout with existing TimeoutError semantics

#### Scenario: Long synchronous phase delays cancellation
- **WHEN** cancellation arrives while the task is inside a long synchronous stretch
- **THEN** the task SHALL reach an await checkpoint within the configured checkpoint bound (default 30s) so cancellation can land
- **AND** services with long phases SHALL provide such checkpoints (e.g. per candidate batch)

### Requirement: Long tasks must expose progress
The system SHALL log progress heartbeats for long task phases and expose the current run status through the bot and the API.

#### Scenario: Heartbeat during a long phase
- **WHEN** a long task phase runs longer than the configured heartbeat interval (default 5 minutes)
- **THEN** a progress log entry SHALL be emitted with task, run_id, phase, progress counters and elapsed time

#### Scenario: Operator queries current run
- **WHEN** /status or the corresponding API endpoint is called while a task runs
- **THEN** the response SHALL include run_id, task name, phase, elapsed time and the latest progress
- **AND** existing /status entries and task report contracts (e.g. the financial-operations-scheduler reports) SHALL remain unchanged

### Requirement: Isolation must preserve scheduling and data semantics
The system SHALL keep max_instances, run_id tracking, dependency groups and job_config semantics unchanged, and SHALL keep the zero-write scan-phase and idempotent-write semantics of reconciliation tasks under isolated execution.

#### Scenario: Duplicate trigger while task runs in worker
- **WHEN** a second run of the same task is requested while max_instances=1 and the first run is active in the worker
- **THEN** the second run SHALL be skipped with the existing max_instances behavior

#### Scenario: Worker is killed mid-run
- **WHEN** the worker execution is cancelled or killed during the scan phase
- **THEN** the target database SHALL contain no partial writes from that run
- **AND** a later re-run SHALL produce the same idempotent reconciliation semantics

#### Scenario: Different jobs run concurrently in workers
- **WHEN** two different jobs run in separate workers and write to the same database
- **THEN** their writes SHALL be serialized per database through the coordination defined in the design (per-database queueing or mutex, with busy_timeout rechecked)
- **AND** neither job SHALL fail due to cross-job write contention newly introduced by the isolation

#### Scenario: Service restarts while a task is running
- **WHEN** the service shuts down or restarts while a task runs in the worker
- **THEN** the shutdown SHALL cancel running workers and wait within a bounded time
- **AND** the interrupted run SHALL leave no inconsistent state (scan phase: no writes; apply phase: only complete per-candidate transactions)
- **AND** a later re-run SHALL complete the remaining candidates idempotently

### Requirement: Loop-bound shared resources must stay on their owning loop
The system SHALL ensure resources bound to the main event loop (Telethon client, async DB engines and pools, shared HTTP sessions) are not used from the worker loop, either via per-run instances created inside the worker or via thread-safe marshalling back to the main loop.

#### Scenario: Task sends a report while running in worker
- **WHEN** a task running in the worker sends its completion or error report
- **THEN** the report delivery SHALL be marshalled to the main loop
- **AND** the Telethon client SHALL NOT be touched from the worker loop
- **AND** the report content and channel contracts of existing capabilities SHALL remain unchanged

#### Scenario: Task needs async DB sessions in worker
- **WHEN** a task requires async DB sessions that belong to the main-loop engine pool
- **THEN** it SHALL either use per-run sessions created inside the worker loop under the task async DB workload configuration (preserving the api-db-resource-isolation workload separation) or be configured to stay on the main loop with the reason recorded

### Requirement: Worker failure must be contained and reported
The system SHALL contain worker failures so the API and the bot stay available, and SHALL report the task as failed with a diagnosable reason.

#### Scenario: Worker raises an unexpected exception
- **WHEN** the task raises inside the worker loop
- **THEN** the run SHALL be marked failed and the existing error report path SHALL be used
- **AND** the API and the bot SHALL remain responsive

#### Scenario: Worker thread becomes stuck
- **WHEN** a worker thread is stuck such that cancellation cannot land
- **THEN** the main loop timeout SHALL still mark the run failed at max_runtime_seconds
- **AND** the orphaned worker SHALL be logged for diagnosis without blocking the API or the bot
