## ADDED Requirements

### Requirement: Backtest-critical datasets emit database-scoped changes
Index-composition snapshots, security-state events and intervals, daily price limits, filing vintages, and canonical corporate-action projections SHALL emit append-only changes in the physical database that owns each dataset.

#### Scenario: Index composition revision is stored
- **WHEN** a new or superseding composition snapshot materially changes effective membership, weights, availability, quality, or lineage
- **THEN** the owning database SHALL append a change record using the snapshot identity and revision as its business key

#### Scenario: Filing vintage becomes locally available
- **WHEN** a filing version and its versioned facts are committed
- **THEN** the financial database SHALL emit changes that identify the source-file version, instrument, report period, fact identity, period semantic, source profile, and availability
- **AND** a later latest-projection update SHALL not erase the filing-version change history

#### Scenario: Canonical action blockers change
- **WHEN** a canonical corporate-action event changes readiness, effective date, terms, factor decision, coverage, or blocking reason
- **THEN** the quote database SHALL append a semantic change for the stable canonical event id

### Requirement: Semantic hashes include point-in-time lineage
Backtest-critical change detection SHALL include fields that can alter a point-in-time result even when the headline value is unchanged.

#### Scenario: Availability or source lineage changes
- **WHEN** effective date, publication time, `available_at`, revision, source profile, quality, completeness, rule version, or supersession lineage changes
- **THEN** the semantic hash SHALL change and a new change record SHALL be emitted

#### Scenario: Integrated overlap fetch is unchanged
- **WHEN** an existing parent workflow re-observes identical business content and point-in-time lineage
- **THEN** the row SHALL be counted unchanged
- **AND** no new watermark SHALL be emitted

### Requirement: Consumers resume within one physical database
Backtest-data change APIs SHALL identify their owning database and SHALL use stable ordering within `database_id + domain + sequence_id` without promising cross-database global order.

#### Scenario: External platform persists a cursor
- **WHEN** the platform resumes index, security-state, financial-vintage, or corporate-action changes
- **THEN** it SHALL submit the database id, domain, and last sequence
- **AND** the response SHALL return later changes and a next cursor for that same scope

#### Scenario: Consumer requests one global sequence
- **WHEN** a caller attempts to interpret sequences from `quotes.db`, `financials.db`, and `research.db` as one global order
- **THEN** the API SHALL expose that ordering as unsupported
- **AND** it SHALL provide database-scoped cursors instead

### Requirement: Read-only probes and dry runs do not advance backtest watermarks
Resource probes, readiness scans, preflight checks, and dry-run backfills SHALL report would-change counts without persisting facts or change records.

#### Scenario: Capability probe discovers new historical rows
- **WHEN** a no-write probe observes rows not present locally
- **THEN** it SHALL report candidate coverage and would-write counts
- **AND** it SHALL not advance any dataset watermark
