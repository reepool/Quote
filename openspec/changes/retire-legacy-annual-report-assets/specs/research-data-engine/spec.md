## ADDED Requirements

### Requirement: Business Profile Must Consume Shared Annual-Report Assets Only
The Research Data Engine SHALL bind business-profile work to an immutable effective asset returned by `research.announcement_assets` and SHALL NOT use a legacy annual-report manifest, archive, downloader, or writer.

#### Scenario: Business-profile work is enqueued
- **WHEN** a valid effective shared annual-report asset exists for an instrument and fiscal year
- **THEN** the work item SHALL record the shared asset identity, source identity, content hash, local content handle, report period, and availability time needed for deterministic processing

#### Scenario: Business-profile stage retries
- **WHEN** parse or semantic processing retries after a correction has changed the current effective asset
- **THEN** the retry SHALL remain bound to its original immutable shared asset
- **AND** the corrected asset SHALL be eligible for separately enqueued replacement work

#### Scenario: Shared asset is unavailable or invalid
- **WHEN** no valid shared asset can be resolved or its integrity check fails
- **THEN** business-profile production SHALL expose an explicit not-ready or retryable asset condition
- **AND** it SHALL NOT fall back to a legacy file or download path

#### Scenario: Historical business-profile run resolves source evidence
- **WHEN** a business-profile run specifies a historical knowledge cutoff
- **THEN** the shared announcement asset service SHALL select only report evidence visible at that cutoff
- **AND** a correction first available after the cutoff SHALL NOT replace the earlier valid report for that run

#### Scenario: Production coverage is counted
- **WHEN** business-profile discovery, reconciliation, or corpus coverage lists usable reports
- **THEN** only effective assets with a locally valid, integrity-valid shared blob and usable canonical path SHALL count as covered

### Requirement: Legacy Annual-Report Compatibility Code Must Not Remain Executable
Production code SHALL NOT retain imports, modes, configuration switches, writers, jobs, or commands that can activate the retired annual-report archive implementation.

#### Scenario: Repository reference check runs
- **WHEN** the shared-only migration is complete
- **THEN** production modules SHALL have no imports of the retired compatibility catalog or archive sync module
- **AND** runtime configuration SHALL have no `dual_read`, `legacy_fallback_enabled`, or `legacy_writer_disabled` annual-report switches
