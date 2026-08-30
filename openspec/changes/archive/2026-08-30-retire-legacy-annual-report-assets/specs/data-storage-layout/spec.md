## ADDED Requirements

### Requirement: Raw Annual Reports Have One Authoritative Asset Store
The system SHALL persist raw annual-report metadata, effective-version lineage, content hashes, and PDF blobs only through the shared announcement asset repository and blob store.

#### Scenario: A consumer needs an annual report
- **WHEN** business profile, broker risk control, or another consumer needs raw annual-report content
- **THEN** it SHALL resolve the report from the shared announcement asset store
- **AND** it SHALL NOT create a second consumer-owned annual-report archive or source-file manifest

#### Scenario: A consumer creates derived data
- **WHEN** a consumer parses pages, selects sections, extracts semantic facts, or publishes domain records
- **THEN** it MAY persist those derived artifacts in its own domain storage
- **AND** the artifacts SHALL reference the immutable shared asset identity and content hash

#### Scenario: A legacy annual-report file exists
- **WHEN** a file exists only in a retired consumer-specific archive
- **THEN** production consumers SHALL NOT read it directly
- **AND** reuse requires verification and registration through the shared announcement asset service
