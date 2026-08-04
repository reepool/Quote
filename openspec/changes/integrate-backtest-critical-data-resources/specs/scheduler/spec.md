## ADDED Requirements

### Requirement: Backtest data stages reuse existing parent workflows
The scheduler SHALL attach backtest-critical maintenance to an existing parent job, governance policy, or dependency-DAG stage when the source, target universe, and cadence overlap.

#### Scenario: Existing parent owns the source surface
- **WHEN** a resource capability record identifies an existing parent workflow for a requested dataset
- **THEN** the maintenance stage SHALL reuse that parent's target scope, trading date, dry-run mode, transport, throttling, freshness evidence, and report context where applicable
- **AND** the scheduler SHALL NOT register a redundant standalone full-market cron

#### Scenario: Stage needs an independent failure policy
- **WHEN** an integrated stage requires a separate timeout, retry, or continuation policy
- **THEN** it SHALL be represented as an explicit parent stage or configured dependency-DAG child
- **AND** it SHALL still inherit only explicitly configured runtime parameters

#### Scenario: No existing workflow can meet the contract
- **WHEN** bounded resource probes prove that no existing owner or provider can be extended
- **THEN** a new job MAY be configured only with the approved resource decision, bounded scope, rate policy, checkpoint, and ownership metadata

### Requirement: Backtest historical acquisition remains operator-scoped
The scheduler SHALL keep backtest-critical historical downloads separate from automatic deployment, startup, schema migration, and ordinary unbounded daily execution.

#### Scenario: Historical backfill is invoked
- **WHEN** an operator requests a supported index-composition, security-state, filing-vintage, or related historical scope
- **THEN** the task SHALL require explicit dataset, market or instrument, and date bounds
- **AND** it SHALL support dry-run, checkpoint, resume, pacing, and coverage reporting through the governed historical-backfill workflow

#### Scenario: Source probe has not proven history
- **WHEN** the resource capability record is current-only, unavailable, or lacks historical evidence
- **THEN** the scheduler SHALL reject production historical acquisition for that scope before broad provider requests or writes

### Requirement: Integrated stage results remain visible
Scheduler reports SHALL identify integrated backtest-data stages, reuse decisions, inherited scopes, provider usage, coverage counters, changed rows, watermarks, skips, and blockers.

#### Scenario: Parent workflow completes with a degraded child stage
- **WHEN** an optional integrated stage is partial, unavailable, or continued by policy
- **THEN** the parent report SHALL preserve the stage's status and reason separately from the parent business result
- **AND** readiness SHALL not be inferred solely from parent success
