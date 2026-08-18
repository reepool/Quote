## ADDED Requirements

### Requirement: Research storage is owned by narrow repositories
Research storage operations SHALL be grouped by stable database and table owner, and application services SHALL receive only the repository capabilities they use.

#### Scenario: Industry service is migrated
- **WHEN** the service reads or writes industry taxonomy and memberships
- **THEN** it depends on the industry repository rather than the full ResearchStorageManager

### Requirement: Existing database isolation is preserved
Repository decomposition MUST preserve all existing database paths, table names, canonical keys, and domain failure boundaries.

#### Scenario: Financial and valuation repositories initialize
- **WHEN** both repositories are constructed
- **THEN** they continue to use their configured `financials.db` and `valuation.db` stores without merging tables

### Requirement: Connection and transaction semantics remain explicit
Repositories SHALL use the existing coordinated connection/database-scope behavior and SHALL preserve commit, rollback, busy timeout, WAL, and read-only semantics relevant to their store.

#### Scenario: Multi-step repository operation fails
- **WHEN** an exception occurs before the owning transaction completes
- **THEN** the operation rolls back with the same visible state as the existing storage contract

### Requirement: Schema and migration ownership is reviewable
Table creation and additive migrations SHALL be split into explicit schema owners while preserving startup order and support for existing local databases.

#### Scenario: Existing database lacks a newer additive column
- **WHEN** the repository schema initialization runs
- **THEN** the owning migration adds the supported column idempotently without destructive rebuild

### Requirement: Storage manager compatibility is temporary
ResearchStorageManager SHALL delegate migrated operations to repositories and SHALL record the replacement and remaining callers for each compatibility method.

#### Scenario: Legacy sync still constructs ResearchStorageManager
- **WHEN** it invokes a migrated storage method
- **THEN** the facade delegates to the same repository used by new application services

### Requirement: Decomposition does not create duplicate writers
Old and new storage implementations MUST NOT independently write the same table during migration.

#### Scenario: Repository takes ownership of valuation writes
- **WHEN** the caller is rebound to the valuation repository
- **THEN** the old manager method delegates to that repository or is removed and contains no separate SQL write path

### Requirement: Shared storage abstractions require proven sameness
The change SHALL extract only connection or transaction behavior that is semantically identical across current repositories and MUST NOT introduce a universal CRUD base or cross-database transaction framework.

#### Scenario: Two repositories have similar upsert SQL
- **WHEN** their business keys or conflict semantics differ
- **THEN** the upserts remain domain-owned instead of being generalized
