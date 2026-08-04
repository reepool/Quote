## ADDED Requirements

### Requirement: Backfill SHALL support opt-in continuous draining
The business-profile backfill SHALL preserve one-pass behavior by default and SHALL repeatedly execute the existing bounded production pass only when `continuous=true`.

#### Scenario: Initial one-pass validation
- **WHEN** an operator runs `business_profile_backfill` without continuous mode
- **THEN** the system performs one bounded discovery, enqueue, and worker pass and returns its report

#### Scenario: Continuous bootstrap
- **WHEN** an operator runs `business_profile_backfill continuous=true`
- **THEN** the system executes bounded passes until the active phase completes, a stop is requested, or automatic progress is blocked

### Requirement: Continuous draining SHALL remain idempotent and resumable
Every continuous pass SHALL reuse the durable work queue, processing identities, annual-report assets, stage checkpoints, retries, and leases used by single-pass production.

#### Scenario: Restart after interruption
- **WHEN** a continuous task is cancelled, the process exits, or the service restarts
- **THEN** a later continuous invocation continues durable unfinished work without duplicating completed work or redownloading verified annual-report assets

#### Scenario: Stale stop request
- **WHEN** a new run starts after an earlier run was stopped
- **THEN** a stop request targeted to the earlier run does not stop the new run

### Requirement: Operators SHALL be able to stop continuous work cooperatively
The system SHALL expose a control action that targets the active run and SHALL stop claiming new item batches after current in-flight item work and coordinated writes settle.

#### Scenario: Stop during active work
- **WHEN** an operator requests stop while a download, parse, semantic, or publish batch is active
- **THEN** the progress state becomes stop-requested and the controller exits at the next safe batch boundary without corrupting queue or database state

#### Scenario: No active run
- **WHEN** an operator requests stop and no current run is active
- **THEN** the control task reports that no active target exists and does not create a wildcard stop request

### Requirement: Continuous progress SHALL be persistent and observable
The system SHALL atomically persist a progress snapshot and SHALL expose a read-only status action containing run state, heartbeat, phase, cycles, cumulative stage counts, queue health, discovery state, coverage, readiness, failures, and stop information.

#### Scenario: Live status query
- **WHEN** an operator requests status during a continuous run
- **THEN** the response reflects the latest completed item batch or pass and identifies heartbeat age and pending queue work

#### Scenario: Status after restart
- **WHEN** the service restarts before a run writes a terminal state
- **THEN** status preserves the last durable snapshot and the next run records that it superseded the stale run

### Requirement: Continuous mode SHALL stop on a deterministic terminal condition
The controller SHALL mark the run completed when current-phase readiness passes and SHALL mark it blocked when terminal failures exist or configured no-progress limits are reached.

#### Scenario: Active phase complete
- **WHEN** discovery is complete and all current-phase queue, coverage, exception, and field-family gates pass
- **THEN** the controller records `completed` and does not activate another rollout phase or daily scheduling

#### Scenario: Pipeline cannot advance automatically
- **WHEN** terminal failures are present or repeated cycles produce no progress with no claimable work
- **THEN** the controller records `blocked` with reason codes and exits instead of busy-looping indefinitely

### Requirement: Long-running control SHALL preserve production safety gates
Continuous execution SHALL NOT enable daily cron, promotion, additional field families, or concurrent SQLite writers beyond the active rollout configuration.

#### Scenario: Structured shadow continuous run
- **WHEN** continuous mode runs while `structured_shadow` is active
- **THEN** only the configured structured field families are processed and no approved-write or daily switch is changed
