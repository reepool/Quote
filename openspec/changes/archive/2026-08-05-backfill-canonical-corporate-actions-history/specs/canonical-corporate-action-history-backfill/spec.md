## ADDED Requirements

### Requirement: Historical Projection Uses Existing Evidence Only
The system SHALL project historical canonical corporate-action revisions only
from current governed observations and their existing resolution, effective-date,
factor-governance and coverage evidence in `quotes.db`.

#### Scenario: Projection has no provider requests
- **WHEN** an operator runs a historical canonical projection
- **THEN** the task SHALL report zero network/provider requests
- **AND** it SHALL not invoke CNInfo, TDX, BaoStock or another acquisition route

#### Scenario: Missing source evidence
- **WHEN** the required observation table is absent
- **THEN** the task SHALL return an explicit unavailable status
- **AND** it SHALL perform no projection writes

### Requirement: Projection Scope Is Bounded And Stable
The system SHALL accept explicit date-independent instrument/event filters and
select source observations in deterministic order. A resumable run SHALL freeze
the selected source-event universe by identity and hash.

#### Scenario: Instrument-scoped projection
- **WHEN** an operator supplies a set of instrument ids
- **THEN** only current observations for those instruments SHALL be considered
- **AND** the report SHALL include the requested and selected scope

#### Scenario: Source universe changes before resume
- **WHEN** the current observation universe hash differs from the checkpoint
- **THEN** the task SHALL reject reuse of the checkpoint
- **AND** it SHALL require a new run identity or explicit operator restart

### Requirement: Dry-Run Does Not Write
The system SHALL support a dry-run mode that computes projection results and
would-write counters without changing canonical tables, change records,
checkpoints or watermarks.

#### Scenario: Dry-run finds ready and blocked events
- **WHEN** dry-run evaluates a scope containing both usable and incomplete events
- **THEN** the report SHALL include considered, ready, blocked and blocker-reason counts
- **AND** canonical row counts and watermarks SHALL remain unchanged

### Requirement: Blocked Events Fail Closed
The system SHALL preserve events with unresolved effective dates, missing economic
terms, lifecycle conflicts, source conflicts or incomplete coverage as blocked
canonical revisions with explicit reasons. Blocked revisions SHALL have
`backtest_ready=0`.

#### Scenario: Factor event has no effective date
- **WHEN** a factor-affecting event has no accepted effective-date evidence
- **THEN** the projected revision SHALL contain `effective_date_missing`
- **AND** strict canonical reads SHALL exclude the revision

#### Scenario: Event has accepted evidence
- **WHEN** an event has accepted lifecycle, terms, effective-date and coverage evidence
- **THEN** the projected revision SHALL have `backtest_ready=1`
- **AND** strict canonical reads SHALL be allowed to return it

### Requirement: Projection Is Append-Only And Idempotent
The system SHALL append a new canonical revision only when the semantic projection
hash changes and SHALL maintain the current compatibility projection for the
latest revision. Repeating the same scope with unchanged evidence SHALL be safe.

#### Scenario: First projection
- **WHEN** a source event has not previously been projected
- **THEN** the task SHALL append one canonical revision and update current state
- **AND** the revision SHALL retain source lineage and decision availability

#### Scenario: Unchanged rerun
- **WHEN** the same source event is projected again with the same evidence
- **THEN** the task SHALL report it as unchanged
- **AND** it SHALL not append a duplicate revision or change watermark

### Requirement: Historical Projection Is Resumable
The system SHALL persist a checkpoint after each successful write batch, including
parameters, source-universe hash, completed batch identities, counters and the
latest database-scoped watermark.

#### Scenario: Batch failure
- **WHEN** a write batch fails or times out
- **THEN** that batch SHALL remain incomplete in the checkpoint
- **AND** a retry with the same parameters SHALL resume from the failed batch

### Requirement: PIT Lineage And Consumer Contract Are Preserved
Every canonical revision SHALL preserve its observation, terms, resolution-state,
effective-date-evidence and coverage identities, projection version and decision
availability. Existing canonical API filters SHALL select revisions by
`known_at` and readiness without exposing later decisions.

#### Scenario: Known-time query before projection decision
- **WHEN** a consumer queries with `known_at` earlier than a projection decision
- **THEN** that later revision SHALL not be returned
- **AND** the API SHALL return an older eligible revision or no row

#### Scenario: Ready-only pagination
- **WHEN** a consumer requests canonical events with `ready_only=true`
- **THEN** blocked revisions SHALL be excluded
- **AND** pagination order SHALL remain deterministic across pages
