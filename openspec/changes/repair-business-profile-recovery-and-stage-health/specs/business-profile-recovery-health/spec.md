## ADDED Requirements

### Requirement: Recovery SHALL preserve only logically compatible checkpoints
The system SHALL preserve a stale-scope checkpoint in place only when its instrument, field families, knowledge cutoff, runtime identities, and promotion manifest hashes match the durable work scope for the active processing identity. Source revision changes alone SHALL remain resumable.

#### Scenario: Runtime identity changed
- **WHEN** a failed checkpoint belongs to a processing identity different from the active invocation
- **THEN** the system SHALL NOT requeue that checkpoint for execution under the active runtime identity

#### Scenario: Complete logical scope matches
- **WHEN** a failed checkpoint and its durable work item have the same active processing identity and complete logical scope
- **THEN** the system SHALL preserve the checkpoint and resume its bound stage without consuming a content retry

#### Scenario: Logical scope is incomplete or incompatible
- **WHEN** an active-identity checkpoint lacks a complete logical scope or any immutable logical-scope field differs
- **THEN** the system SHALL rotate to a new checkpoint, clear invalid downstream stage results, and restart at acquire while allowing the archived annual report to be reused

### Requirement: Obsolete processing identities SHALL retire automatically
The system SHALL keep only the current processing-identity work claimable for a frontier and selection policy while retaining obsolete work rows for audit. Worker claims SHALL be restricted to the active processing identity.

#### Scenario: Current replacement exists
- **WHEN** current-identity work is inserted or reused for a frontier and an obsolete-identity row for the same policy is pending, retryable, or terminally failed
- **THEN** the obsolete row SHALL become superseded and SHALL no longer contribute to claimable or terminal health counts

#### Scenario: Obsolete work holds an active lease
- **WHEN** an obsolete-identity row is running with an active lease
- **THEN** enqueueing SHALL NOT supersede it underneath the worker

#### Scenario: Obsolete lease expires
- **WHEN** an obsolete-identity row has an expired lease or remains pending outside the active enqueue scope
- **THEN** it SHALL NOT be claimed by an active-identity worker and an eligible replacement enqueue SHALL supersede it

### Requirement: Recovered work SHALL NOT monopolize bounded stages
The queue SHALL give work already waiting in a stage precedence over work freshly requeued by recovery within the same report-period priority.

#### Scenario: Freshly recovered and already pending work coexist
- **WHEN** both classes are claimable in a bounded stage invocation
- **THEN** claims SHALL order by stage-entry update time before immutable creation order so the freshly recovered rows do not consume the entire budget ahead of older pending rows

### Requirement: Operational status SHALL include worker health
Daily and backfill reports SHALL derive status consistently from discovery and every attempted worker stage.

#### Scenario: Terminal worker failure
- **WHEN** any stage records one or more terminal failures
- **THEN** the top-level report SHALL be degraded and SHALL include a worker-terminal-failure reason code

#### Scenario: Retry or configuration block
- **WHEN** any stage records retries, configuration blocks, or lease conflicts
- **THEN** the top-level report SHALL be degraded and SHALL include corresponding reason codes

#### Scenario: Healthy bounded progress with remaining backlog
- **WHEN** discovery succeeds and attempted workers complete without retry, terminal failure, configuration block, or lease conflict
- **THEN** the report MAY be successful even when claimable work remains for later bounded invocations
