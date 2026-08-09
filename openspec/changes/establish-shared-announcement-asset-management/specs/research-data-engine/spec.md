## ADDED Requirements

### Requirement: Research Consumers Use Shared Annual-Report Assets
The Research Data Engine SHALL provide one stable annual-report asset dependency for business-profile, broker risk-control, and future consumers.

#### Scenario: Business-profile requires an annual report
- **WHEN** a business-profile acquire or parse stage needs a formal annual report
- **THEN** it SHALL request the effective shared asset by instrument and fiscal year or by source filing identity
- **AND** it SHALL not write a business-profile-owned copy of the original attachment

#### Scenario: Consumer requires an exact filing
- **WHEN** a business work item is already bound to a source and announcement id
- **THEN** the shared service SHALL ensure that exact filing or return an explicit unavailable/integrity status
- **AND** it SHALL not substitute a different legal filing solely because its content or title is similar

#### Scenario: Consumer needs only local data
- **WHEN** a research workflow runs with network calls disabled
- **THEN** annual-report lookup SHALL be local-only and SHALL return an explicit missing status when unavailable

### Requirement: DataManager Exposes Annual-Report Asset Operations
DataManager SHALL expose business-neutral annual-report asset lookup, ensure, status, and bounded-operation methods that do not depend on business-profile configuration.

#### Scenario: Caller lists effective reports
- **WHEN** a caller lists annual reports by instrument, fiscal year, source, integrity, or acquisition status
- **THEN** DataManager SHALL query the shared asset repository and return stable source and asset identities

#### Scenario: Caller ensures a report
- **WHEN** a caller requests ensure with permitted network and storage policy
- **THEN** DataManager SHALL invoke the shared service and return local-hit, adopted, downloaded, queued, missing, failed, or blocked status with diagnostics

#### Scenario: Legacy annual-report catalog is called during migration
- **WHEN** an existing caller uses `get_annual_report_assets` or `get_annual_report_asset`
- **THEN** the compatibility method SHALL read through the shared repository
- **AND** it SHALL not remain permanently filtered to business-profile manifests

### Requirement: Annual-Report Asset API Is Additive And Safe
The Research API SHALL provide additive endpoints for effective annual-report metadata, readiness, acquisition requests, operation status, and controlled file delivery.

#### Scenario: Front-facing client queries status
- **WHEN** a client requests annual-report asset status for an instrument
- **THEN** the response SHALL include fiscal year, report period, source, filing id, published time, correction flag, content hash, content length, local availability, integrity, acquisition status, effective status, and last checked time
- **AND** the GET request SHALL perform zero provider calls and zero attachment writes

#### Scenario: Client requests acquisition
- **WHEN** an authorized client requests a missing annual report
- **THEN** the API SHALL create or reuse a bounded operation and return an operation id and state
- **AND** it SHALL not keep the request open for an unbounded market or attachment fetch

#### Scenario: Authorization boundary is unavailable
- **WHEN** the deployment has no configured trusted identity and scoped permissions
- **THEN** acquisition, content delivery, cancellation, repair, and operator endpoints SHALL remain disabled or fail closed

#### Scenario: Client follows operation status
- **WHEN** a client polls an acquisition operation
- **THEN** the API SHALL return queued, discovering, downloading, validating, completed, missing, failed, or blocked state plus bounded diagnostics
- **AND** it SHALL separately return asset availability, ensure disposition, and downstream consumer-processing state where applicable

#### Scenario: Client downloads an asset
- **WHEN** a client requests an available asset by id
- **THEN** the API SHALL validate the effective record and file integrity before streaming
- **AND** it SHALL not accept a caller-supplied filesystem path

#### Scenario: Client requests a superseded or corrupt asset
- **WHEN** a content request resolves to a superseded, missing, or integrity-failed file
- **THEN** the API SHALL reject delivery with a stable conflict or gone response and SHALL NOT stream stale bytes

### Requirement: Consumer Outputs Surface Asset Lineage
Business-facing results that depend on annual reports SHALL expose sufficient shared asset lineage for audit without leaking internal archive paths.

#### Scenario: Business-profile result uses annual-report evidence
- **WHEN** a business-profile result is returned
- **THEN** its evidence lineage SHALL identify the shared asset id, source filing, report period, content hash, and effective correction status

#### Scenario: Broker fact uses annual-report evidence
- **WHEN** a broker regulatory fact is returned or inspected
- **THEN** its source lineage SHALL identify the shared asset id and broker processing manifest
