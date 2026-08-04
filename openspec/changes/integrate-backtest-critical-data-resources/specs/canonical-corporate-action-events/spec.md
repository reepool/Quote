## ADDED Requirements

### Requirement: Canonical corporate actions project existing governed evidence
The system SHALL materialize immutable canonical corporate-action projection revisions from existing CNInfo observations, TDX reconciliation, resolved-term overlays, effective-date evidence, and factor governance without mutating source evidence.

#### Scenario: Existing closure changes an event
- **WHEN** the CNInfo daily path, TDX weekly refresh, reconciliation, or reviewed promotion materially changes accepted evidence
- **THEN** the same closure SHALL append revisions only for affected canonical events whose semantic projection changed and update a current compatibility projection
- **AND** it SHALL NOT trigger a new corporate-action network acquisition job

#### Scenario: Projection is rebuilt
- **WHEN** canonical output is calculated
- **THEN** it SHALL retain a stable canonical event id, immutable revision id, decision `available_at`, projection version, input identities or semantic hash, source lineage, lifecycle applicability, coverage state, quality state, and blocking reasons

#### Scenario: Late review changes an existing event
- **WHEN** review or reconciliation changes canonical terms, dates, readiness, or factor decisions after an earlier revision was published
- **THEN** the system SHALL append a later projection revision
- **AND** the earlier projection revision SHALL remain queryable for earlier known-time cutoffs

### Requirement: Canonical events preserve economic terms and event dates
The canonical projection SHALL expose supported event terms and distinct announcement, record, ex/effective, payment, and share-arrival dates when known.

#### Scenario: Distribution terms are accepted
- **WHEN** governed evidence supports cash, bonus, capitalization, or rights terms
- **THEN** the canonical row SHALL expose normalized per-share terms, currency where applicable, source units, and supporting lineage

#### Scenario: Event does not affect adjustment factors
- **WHEN** reviewed evidence supports a canonical event that has no factor effect
- **THEN** the row SHALL remain queryable with `factor_effect=false`
- **AND** it SHALL NOT be discarded solely because it does not create an adjustment factor

### Requirement: Backtest readiness fails closed on unresolved corporate actions
The system SHALL mark a canonical event `backtest_ready=true` only when its accepted evidence, effective date, economic terms, lifecycle applicability, and conflict state satisfy the configured event-type contract.

#### Scenario: Event is complete
- **WHEN** an accepted event has a usable effective date, coherent required terms, applicable instrument lifecycle, and no blocking conflict
- **THEN** the canonical projection MAY be marked backtest-ready
- **AND** its lineage and quality SHALL remain visible

#### Scenario: Event is incomplete or conflicting
- **WHEN** required terms or dates are missing, source asymmetry remains unresolved, or factor governance blocks the event
- **THEN** the event SHALL remain queryable with `backtest_ready=false` and explicit blockers
- **AND** strict consumers SHALL be able to exclude it

### Requirement: Canonical corporate-action API is stable, bounded, and point-in-time
The system SHALL expose canonical corporate actions through a backward-compatible, paginated read endpoint with instrument, date, event-type, readiness, known-time, and change-cursor filters.

#### Scenario: Consumer pages through canonical events
- **WHEN** a caller requests canonical events with a bounded page size
- **THEN** rows SHALL use deterministic business-key ordering and stable pagination
- **AND** each row SHALL include revision, effective and availability times, readiness, quality, and source lineage

#### Scenario: Consumer supplies a known-time cutoff
- **WHEN** a caller supplies `known_at`
- **THEN** the API SHALL select the latest canonical projection revision whose decision `available_at <= known_at`
- **AND** it SHALL NOT expose a later review, reconciliation, or readiness decision

#### Scenario: Current projection is requested
- **WHEN** a caller omits `known_at` under the documented current-read policy
- **THEN** the API MAY return the latest compatibility projection
- **AND** the response SHALL identify that it is not a historical known-time read

#### Scenario: Consumer resumes from a watermark
- **WHEN** a caller supplies a valid database-scoped corporate-action cursor
- **THEN** the API SHALL return subsequent semantic changes in watermark order
- **AND** it SHALL identify the owning database and next cursor

### Requirement: Canonical projection readiness is reconciled with evidence coverage
The system SHALL report canonical coverage by event type, market, date range, ready state, conflict state, and unresolved effective-date or term counts.

#### Scenario: Acquisition succeeded but closure is incomplete
- **WHEN** raw observations exist but canonical blockers remain
- **THEN** readiness SHALL distinguish acquisition coverage from consumer-ready coverage
- **AND** it SHALL not describe raw-row presence as canonical completeness
