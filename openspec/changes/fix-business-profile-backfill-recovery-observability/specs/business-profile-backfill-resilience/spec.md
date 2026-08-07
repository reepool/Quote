## ADDED Requirements

### Requirement: Stable work knowledge cutoff
The system SHALL bind each durable business-profile work item to one normalized knowledge cutoff and SHALL use that cutoff for every processing stage and resumed invocation.

#### Scenario: Work resumes on a later calendar day
- **WHEN** a pending work item is processed by a backfill invocation whose current date differs from the item's bound knowledge cutoff
- **THEN** the stage scope uses the work-bound cutoff and remains compatible with its existing checkpoint

#### Scenario: Existing work lacks cutoff metadata
- **WHEN** a reused work item has no bound cutoff in metadata
- **THEN** the system derives it from a readable checkpoint or assigns it before creating the first checkpoint

### Requirement: Automated stale-checkpoint recovery
The system SHALL automatically recover work whose sole recorded failure is a stale semantic production checkpoint scope without consuming a content-attempt budget.

#### Scenario: Existing checkpoint has a valid cutoff
- **WHEN** stale-scope recovery reads a valid knowledge cutoff from the existing checkpoint
- **THEN** it binds that cutoff to the work item, resets the queue attempt state, and preserves the current stage and checkpoint

#### Scenario: Existing checkpoint cannot be reused safely
- **WHEN** stale-scope recovery cannot read a valid cutoff from the existing checkpoint
- **THEN** it rotates to a new checkpoint, invalidates stale stage results, and restarts from acquire while allowing the archived annual report to be reused

### Requirement: Long-running backfill progress logs
The system SHALL emit structured logs at key backfill lifecycle and worker progress points without logging filing text, LLM content, or credentials.

#### Scenario: Backfill advances normally
- **WHEN** discovery, recovery, enqueue, and worker stages run
- **THEN** logs expose run scope, stage budgets, discovery pages and backlog, recovery and enqueue counts, per-stage claimed/completed/retried/failed counts, writer pressure, queue health, and elapsed time

#### Scenario: A worker batch is still running
- **WHEN** a worker batch has not completed before the configured progress interval
- **THEN** a heartbeat log reports the stage, elapsed time, in-flight count, and current counters

#### Scenario: Work fails or the run is degraded
- **WHEN** a work item fails or discovery retains resumable backlog
- **THEN** warning logs include safe work identity, stage, retry disposition, failure category, and backlog boundaries sufficient for automated diagnosis

### Requirement: Existing concurrency and writer isolation
The system SHALL retain asynchronous parse and semantic concurrency while serializing SQLite write transactions through the shared business-profile writer coordinator.

#### Scenario: Concurrent workers emit telemetry
- **WHEN** parse and semantic workers run concurrently and emit progress logs
- **THEN** logging does not acquire or bypass the database writer channel and write concurrency remains one
