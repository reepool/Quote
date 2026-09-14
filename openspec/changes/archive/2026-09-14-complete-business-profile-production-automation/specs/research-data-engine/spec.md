## ADDED Requirements

### Requirement: Business-profile production storage SHALL initialize before enabled execution
Enabled business-profile production SHALL initialize and validate the research fact schema and financial source-manifest schema before discovery, acquisition, or promotion begins.

#### Scenario: Manifest schema is absent
- **WHEN** production is enabled but the configured financial database lacks the required source-file manifest table
- **THEN** initialization creates the compatible schema or the run fails before downloading artifacts with an explicit database-routing diagnostic

### Requirement: Business-profile exceptions SHALL be persistent and idempotent
The research data engine SHALL persist machine-rework and quick-review exceptions with stable target identities and retry metadata for discovery, resolution, derivation, and publication gaps.

#### Scenario: Repeated identical exception
- **WHEN** the same instrument, field family, target, reason, and source revision fail repeatedly
- **THEN** the engine preserves one governed open exception lineage rather than adding duplicate backlog rows

### Requirement: Archive audit SHALL not infer deletion authority from missing database rows
The research data engine SHALL distinguish missing manifest initialization, unreferenced artifacts, and proven orphaned duplicates.

#### Scenario: Production manifest table has never been initialized
- **WHEN** archive files exist but the configured manifest table is absent
- **THEN** the audit reports an ungoverned archive state and prohibits automatic artifact deletion
