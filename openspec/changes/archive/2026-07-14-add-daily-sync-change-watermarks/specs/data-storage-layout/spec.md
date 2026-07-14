## ADDED Requirements

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
