## ADDED Requirements

### Requirement: Index composition snapshots are immutable and point-in-time qualified
The system SHALL store index constituent and weight observations as immutable source snapshots with effective, publication, and local-availability time semantics.

#### Scenario: Complete source snapshot is ingested
- **WHEN** an existing official index adapter returns a composition snapshot
- **THEN** the system SHALL store the index identity, effective/reference date, publication time when supplied, `available_at`, source profile, artifact identity or hash, weight unit, completeness state, and member rows
- **AND** each member SHALL retain its normalized instrument identity, source symbol, weight, and quality diagnostics

#### Scenario: Published snapshot is corrected
- **WHEN** the same source publishes materially different content for an existing snapshot identity
- **THEN** the system SHALL preserve the earlier observation and store a revision or superseding snapshot
- **AND** it SHALL NOT overwrite the fact that was previously available

### Requirement: Index validity evidence is append-only and knowledge-time qualified
The system SHALL preserve each material validity or continuity conclusion as an immutable revision with its own decision availability, evidence, and input snapshot lineage.

#### Scenario: Later snapshot bounds an earlier snapshot
- **WHEN** an adjacent complete snapshot establishes a validity boundary for an earlier snapshot
- **THEN** the system SHALL append a validity revision with `valid_from`, `valid_to_exclusive`, decision `available_at`, basis, and both snapshot identities
- **AND** it SHALL NOT rewrite the validity conclusion visible before that decision time

#### Scenario: Late correction changes continuity
- **WHEN** a correction or later evidence changes a prior validity or continuity conclusion
- **THEN** the system SHALL append a superseding validity revision
- **AND** all prior validity revisions SHALL remain queryable

### Requirement: Index composition reads enforce effective and known time
The point-in-time index composition resolver SHALL select only complete snapshots that were effective, locally available, and proven valid for the caller's cutoffs.

#### Scenario: Consumer requests an as-of composition
- **WHEN** a caller supplies `as_of_date` and `known_at`
- **THEN** the resolver SHALL filter both snapshot and validity revisions by `available_at <= known_at` and select a complete snapshot whose effective and selected validity intervals contain `as_of_date`
- **AND** it SHALL return source, reference date, availability, revision, completeness, and weight-unit metadata

#### Scenario: Adjacent snapshots prove a validity interval
- **WHEN** two complete adjacent snapshots have accepted effective dates and no unresolved intervening rebalance evidence
- **THEN** a validity revision available no earlier than the bounding evidence MAY mark the earlier snapshot valid until the later snapshot's effective date exclusively
- **AND** the validity basis SHALL remain visible

#### Scenario: No replacement is observed through a bounded date
- **WHEN** source freshness evidence proves that a current snapshot remained the latest through a specific observation date
- **THEN** a validity revision available no earlier than the freshness observation MAY mark the snapshot valid only through that proven date under the configured boundary convention
- **AND** it SHALL NOT receive an unbounded future validity interval

#### Scenario: Validity evidence was learned after the cutoff
- **WHEN** a validity or continuity revision has decision `available_at > known_at`
- **THEN** the resolver SHALL ignore that revision
- **AND** it SHALL return the result supported by earlier knowledge or explicit unavailable when no earlier proof exists

#### Scenario: No qualifying snapshot exists
- **WHEN** no complete snapshot satisfies both time cutoffs
- **THEN** strict mode SHALL return an explicit unavailable result
- **AND** it SHALL NOT forward-fill across an unobserved historical rebalance

#### Scenario: Expected rebalance is missing
- **WHEN** `as_of_date` is beyond the snapshot's proven validity boundary or continuity evidence contains an unresolved expected rebalance
- **THEN** strict mode SHALL return unavailable for that date
- **AND** it SHALL NOT keep returning the preceding snapshot solely because its effective date is earlier

#### Scenario: Weights are reference-date weights
- **WHEN** the source reports weights only for a rebalance or reference date
- **THEN** the API SHALL label that reference date
- **AND** it SHALL NOT describe those weights as daily rebalanced weights

### Requirement: Index composition maintenance reuses existing workflows
Forward composition maintenance SHALL extend index-master governance, and historical acquisition SHALL extend the governed A-share historical backfill workflow.

#### Scenario: Daily index governance is due
- **WHEN** index-master governance determines that a supported composition source is due or changed
- **THEN** it SHALL reuse the existing index provider adapter, transport, freshness policy, and run report to capture the current snapshot
- **AND** it SHALL NOT invoke a duplicate full-index cron

#### Scenario: Operator requests historical composition backfill
- **WHEN** a source probe proves historical composition coverage and an operator selects indexes and dates
- **THEN** the system SHALL run through the existing historical-backfill scope, checkpoint, pacing, dry-run, and resume contracts
- **AND** deployment or schema migration SHALL NOT initiate the download

### Requirement: Index composition readiness is coverage-gated
The system SHALL publish readiness only for index/date scopes whose snapshots meet configured member, weight, identity, and point-in-time quality thresholds.

#### Scenario: Snapshot is partial or current-only
- **WHEN** a snapshot is missing required members, has unresolved instrument identities, or lacks historical semantics
- **THEN** the readiness output SHALL expose the blocking counts and scope
- **AND** strict backtest reads SHALL remain unavailable for that affected scope
