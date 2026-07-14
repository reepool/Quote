# data-storage-layout Specification

## Purpose
TBD - created by archiving change relocate-data-mount-to-sda3. Update Purpose after archive.
## Requirements
### Requirement: Quote Data Directory Must Be A Dedicated Mounted Data Volume
The Quote system SHALL support using `/home/python/Quote/data` as the application data directory mounted from the dedicated local data filesystem.

#### Scenario: Data volume is mounted
- **WHEN** the migration is complete
- **THEN** `/home/python/Quote/data` SHALL be backed by the `/dev/sda3` filesystem or its UUID-equivalent mount
- **AND** existing application-relative paths under `data/` SHALL continue to resolve without code changes

#### Scenario: Future financial storage is created
- **WHEN** `financials.db` and financial filing archives are introduced
- **THEN** they SHALL be created under `/home/python/Quote/data` so they reside on the dedicated data volume

### Requirement: NAS Backup Mounts Must Remain Child Mounts Under Data
The Quote system SHALL preserve the existing NAS backup paths as child mount points under `/home/python/Quote/data`.

#### Scenario: NAS backup mounts are available
- **WHEN** the system mounts configured backup shares
- **THEN** `192.168.188.88:/volume2/PVE-Bak` SHALL mount at `/home/python/Quote/data/PVE-Bak`
- **AND** `192.168.188.68:/export/HDD-2/QuoteBak` SHALL mount at `/home/python/Quote/data/QuoteBak`

#### Scenario: NAS is unavailable at boot
- **WHEN** one or more NAS backup shares are unavailable during boot
- **THEN** the local Quote data volume SHALL still mount
- **AND** service startup SHALL NOT be blocked solely by the unavailable NAS share

### Requirement: Migration Must Not Copy NAS Backup Contents Into The Local Data Volume
The migration process MUST copy only local Quote data files and MUST NOT copy mounted NAS backup contents into `/dev/sda3`.

#### Scenario: Data copy is performed
- **WHEN** local data is copied to the new data volume
- **THEN** NAS child mounts SHALL be unmounted first or excluded by a same-filesystem copy rule
- **AND** the copy command SHALL preserve database files, local backups, reports, ownership, permissions, and timestamps

#### Scenario: NAS mount point directories are recreated
- **WHEN** local data copy completes
- **THEN** empty `PVE-Bak` and `QuoteBak` mount-point directories SHALL exist on the new local data volume before remounting NAS shares

### Requirement: SQLite Databases Must Be Migrated Only While Quiescent
The migration process MUST ensure SQLite databases are not actively written during file-system migration.

#### Scenario: Migration preflight runs
- **WHEN** the operator prepares to migrate `quotes.db` and `research.db`
- **THEN** API, scheduler, Telegram task entry points, and manual writers SHALL be stopped or proven inactive
- **AND** SQLite WAL checkpoints and integrity checks SHALL pass before the database files are copied

#### Scenario: Migration validation runs
- **WHEN** databases have been copied and mounted at the new data location
- **THEN** `PRAGMA integrity_check` SHALL pass for `quotes.db` and `research.db`
- **AND** smoke tests SHALL verify quote reads and research readiness reads before normal scheduling resumes

### Requirement: Fstab Changes Must Be Reversible
The migration SHALL define fstab entries and rollback steps that allow restoring the previous root-filesystem data directory if validation fails.

#### Scenario: New mount is configured
- **WHEN** `/etc/fstab` is updated for the migration
- **THEN** the local data volume SHALL be configured by UUID rather than a volatile block-device name
- **AND** the NAS child mounts SHALL depend on the local data mount

#### Scenario: Rollback is required
- **WHEN** validation fails after mounting the new data volume
- **THEN** the operator SHALL be able to stop services, unmount child NAS mounts, unmount `/home/python/Quote/data`, restore the saved root-filesystem `data` directory, restore the previous fstab entries, and restart services

### Requirement: Application Configuration Must Prefer Stable Project-Relative Paths
The migration SHALL avoid unnecessary application configuration churn by preserving existing project-relative data paths.

#### Scenario: Quote and research configuration are loaded
- **WHEN** the application reads existing database configuration after migration
- **THEN** `data/quotes.db` and `data/research.db` SHALL continue to point to the migrated databases through the mounted data directory

#### Scenario: Backup configuration is loaded
- **WHEN** the database backup task reads existing backup configuration after migration
- **THEN** the configured NAS backup path under `data/PVE-Bak/QuoteBak` SHALL continue to resolve through the NAS child mount

### Requirement: Financial Numeric Facts Avoid Physical Duplication
The storage layer SHALL avoid storing a complete duplicate copy of financial numeric facts in both a canonical physical table and a hot/history tier table.

#### Scenario: Numeric facts are written
- **WHEN** financial numeric facts are upserted
- **THEN** the storage layer SHALL write each fact to the selected physical tier table
- **AND** it SHALL NOT require a second physical copy in `financial_numeric_facts`

#### Scenario: Compatibility reads are required
- **WHEN** a reader expects `financial_numeric_facts`
- **THEN** an optimized database SHALL expose `financial_numeric_facts` as a compatibility view over `financial_numeric_facts_hot` and `financial_numeric_facts_history`
- **AND** the view SHALL preserve the same columns as the tier tables

### Requirement: Financial DB Storage Audit Is Lightweight
The system SHALL provide an operator-facing financial database storage audit that explains major storage drivers without requiring a full 79 GiB page-map scan.

#### Scenario: Audit command runs
- **WHEN** the operator runs the financial DB storage audit
- **THEN** the command SHALL report file size, page count, free page count, table count, index count, key financial table row counts, duplicate numeric fact tier evidence, and sampled JSON payload sizes
- **AND** it SHALL avoid expensive full-table JSON-length scans by default

#### Scenario: Detailed audit is requested
- **WHEN** the operator requests detailed or exact sizing
- **THEN** the command MAY run slower full scans
- **AND** it SHALL clearly mark the estimated versus exact metrics

### Requirement: Financial DB Optimization Preserves Database Name
Financial database optimization SHALL preserve the production database path and name.

#### Scenario: Optimized database is cut over
- **WHEN** a financial database optimization migration completes validation
- **THEN** the final production path SHALL remain `data/financials.db`
- **AND** callers SHALL NOT need to change configuration to use a different database name

### Requirement: Financial DB Optimization Requires Backup And Validation
Financial database optimization SHALL be operator-triggered, backed up, and validated before replacing the production database.

#### Scenario: Migration is executed
- **WHEN** the operator executes the optimization migration
- **THEN** the migration SHALL create a timestamped backup under `/home/python/Quote/data/PVE-Bak/QuoteBak` by default
- **AND** it SHALL build an optimized temporary database before replacing the production database
- **AND** it SHALL fail closed if backup, row-count validation, representative read validation, or SQLite integrity checks fail

#### Scenario: Migration is dry-run
- **WHEN** the operator runs migration with dry-run mode
- **THEN** the command SHALL report planned backup path, temporary database path, expected dropped duplicate objects, and validation plan
- **AND** it SHALL NOT modify `data/financials.db`

### Requirement: Changelog Storage Is Non-Destructive
Storage migrations for change watermarks SHALL be additive and SHALL NOT delete, rewrite, or reinterpret existing historical observations.

#### Scenario: Existing database is migrated
- **WHEN** a database with existing quote, futures, FX, commodity, or research rows is upgraded
- **THEN** migration SHALL add changelog metadata structures without dropping existing tables or rows

### Requirement: Semantic Hash Fields Exclude Operational Metadata
Row hashes used for change detection SHALL be computed from canonical business fields and SHALL exclude operational metadata such as `updated_at`, retry count, batch id, and ingestion run id.

#### Scenario: Only updated_at changes
- **WHEN** an upsert would only change operational metadata and no business field changes
- **THEN** the row hash SHALL remain unchanged
- **AND** no material change record SHALL be appended

### Requirement: Existing Hash-Aware Tables Reuse Their Hashes
Storage paths that already maintain raw payload hashes, row hashes, or lineage hashes SHALL reuse them when they are stable semantic identifiers, or SHALL derive a canonical business hash when raw hashes are unstable.

#### Scenario: Existing futures bar has raw payload hash
- **WHEN** a futures bar write path already compares `raw_payload_hash`
- **THEN** the storage layer SHALL reuse that comparison for inserted, changed, and unchanged classification unless a canonical hash is required for stability

### Requirement: Changelog Records Preserve Queryable Keys
Changelog storage SHALL include a lossless JSON business key plus indexed common columns for domains where instrument, series, observation date, or period filters are expected.

#### Scenario: Quote changelog row is stored
- **WHEN** a quote change record is appended
- **THEN** the record SHALL include a business key containing instrument id and trade date
- **AND** it SHALL also populate indexed instrument and observation-date columns for efficient query filters

#### Scenario: Financial changelog row is stored
- **WHEN** a financial fact change record is appended
- **THEN** the record SHALL preserve report period and fact identity in the business key
- **AND** it SHALL NOT force the fact into a trade-date-only schema

