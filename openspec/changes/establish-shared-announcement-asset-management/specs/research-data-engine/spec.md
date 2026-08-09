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

#### Scenario: Consumer requests a superseded exact filing
- **WHEN** a business work item requests a known predecessor whose bytes were deleted under version 1 retention
- **THEN** the shared service SHALL return historical metadata with local content unavailable
- **AND** ordinary consumer acquisition SHALL NOT redownload the superseded attachment

#### Scenario: Consumer needs only local data
- **WHEN** a research workflow runs with network calls disabled
- **THEN** annual-report lookup SHALL be local-only and SHALL return an explicit missing status when unavailable

#### Scenario: Historical knowledge cutoff is requested
- **WHEN** a consumer requests annual-report evidence with a knowledge cutoff
- **THEN** selection SHALL exclude announcements and corrections published after that cutoff
- **AND** if the eligible predecessor bytes were deleted under version 1 retention, the service SHALL report historical metadata with local content unavailable rather than substitute a later correction

#### Scenario: Structured financial facts are available
- **WHEN** an existing official JSON, XBRL, or canonical numeric-fact source satisfies a financial-data request
- **THEN** the annual-report asset capability SHALL NOT replace that structured source or trigger PDF parsing during valuation reads

#### Scenario: Consumer migration gate is enabled
- **WHEN** business-profile or broker annual-report acquisition has cut over to shared assets
- **THEN** a shared miss SHALL use only the shared ensure path
- **AND** the consumer SHALL NOT fall back to its legacy provider downloader or business-owned original archive writer

#### Scenario: Adopted asset path changes during migration
- **WHEN** a verified source asset is moved, linked, or projected from a legacy path to the canonical blob pool without changing source identity or content hash
- **THEN** business-profile and broker consumer-processing identity SHALL remain stable
- **AND** the path change alone SHALL NOT force redownload or alter derived business facts

### Requirement: DataManager Exposes Annual-Report Asset Operations
DataManager SHALL expose business-neutral annual-report asset lookup, ensure, status, and bounded-operation methods that do not depend on business-profile configuration.

#### Scenario: Caller lists effective reports
- **WHEN** a caller lists annual reports by instrument, fiscal year, source, integrity, or acquisition status
- **THEN** DataManager SHALL query the shared asset repository and return stable source and asset identities

#### Scenario: Caller ensures a report
- **WHEN** a caller requests ensure with permitted network and storage policy
- **THEN** DataManager SHALL invoke the shared service and return `local_hit`, `local_miss`, `operation_created`, or `operation_reused` disposition
- **AND** asset availability, durable operation status/stage, and final `adopted|downloaded|repaired` origin SHALL be represented separately

#### Scenario: Legacy annual-report catalog is called during migration
- **WHEN** an existing caller uses `get_annual_report_assets` or `get_annual_report_asset`
- **THEN** the compatibility method SHALL read through the shared repository
- **AND** it SHALL not remain permanently filtered to business-profile manifests

#### Scenario: Legacy compatibility response is produced
- **WHEN** an existing internal caller uses a legacy catalog signature or identifier during migration
- **THEN** DataManager SHALL preserve compatible call parameters and key legacy identities while adding the shared asset id
- **AND** any controlled local handle SHALL remain internal and SHALL be removed by external API serializers

#### Scenario: Caller inspects operations and readiness
- **WHEN** a caller requests annual-report operation status or service readiness
- **THEN** DataManager SHALL expose durable progress plus coverage, integrity, storage, backup, scheduler, and consumer-migration state without constructing business-profile services

### Requirement: Annual-Report Asset API Is Additive And Safe
The Research API SHALL provide additive endpoints for effective annual-report metadata, readiness, acquisition requests, operation status, and controlled file delivery.

#### Scenario: Front-facing client queries status
- **WHEN** a client requests annual-report asset status for an instrument
- **THEN** the response SHALL include fiscal year, report period, source, filing id, published time, correction flag, content hash, content length, local availability, integrity, acquisition status, effective status, and last checked time
- **AND** the GET request SHALL perform zero provider calls and zero attachment writes

#### Scenario: Client requests acquisition
- **WHEN** an authorized client requests a missing annual report
- **THEN** the API SHALL accept only one instrument/fiscal-year or exact source-filing scope and create or reuse a bounded operation
- **AND** it SHALL return HTTP 200 with `local_hit` for an immediately valid asset or HTTP 202 with operation id, `Location`, and `Retry-After` for created or reused work
- **AND** it SHALL not keep the request open for an unbounded market or attachment fetch

#### Scenario: Acquisition request is repeated
- **WHEN** the same normalized scope and policy is submitted again with the same idempotency identity while work is active
- **THEN** the API SHALL return the same durable operation and SHALL NOT issue a second provider request or physical write

#### Scenario: Authorization boundary is unavailable
- **WHEN** the deployment has no configured trusted identity and scoped permissions
- **THEN** acquisition, content delivery, cancellation, repair, and operator endpoints SHALL remain disabled or fail closed

#### Scenario: Caller exceeds an API scope or rate bound
- **WHEN** a client requests a market-wide scope, an unsafe parameter, or exceeds configured request/rate limits
- **THEN** the API SHALL reject the request with a stable bounded error (such as 400/422 or 429 with `Retry-After`)
- **AND** it SHALL NOT create an unbounded operation or contact a provider

#### Scenario: Provider or storage is temporarily unavailable
- **WHEN** an authorized bounded ensure cannot proceed because the provider, archive mount, or storage reserve is unavailable
- **THEN** the API SHALL return or expose a retryable/blocked reason with stable diagnostics (such as 503)
- **AND** it SHALL preserve any durable operation and never publish partial bytes

#### Scenario: Client follows operation status
- **WHEN** a client polls an acquisition operation
- **THEN** the API SHALL return `queued|running|completed|missing|failed|blocked|cancelled|expired` status, current stage, retry metadata, timestamps, progress, result asset id, stable reason codes, and bounded diagnostics
- **AND** it SHALL separately return asset availability, ensure disposition, and downstream consumer-processing state where applicable

#### Scenario: Client polls another caller's operation
- **WHEN** a caller lacks ownership/read scope for the requested operation
- **THEN** the API SHALL deny access without disclosing the operation scope, diagnostics, or existence beyond the configured authorization policy

#### Scenario: Effective report is not locally available
- **WHEN** a metadata GET cannot resolve a local valid effective report
- **THEN** the API SHALL return structured missing, metadata-only, corrupt, ambiguous, or blocked availability with last-checked evidence
- **AND** it SHALL NOT convert a normal absence into an internal-server error or trigger acquisition

#### Scenario: A served predecessor is provisional
- **WHEN** a newer complete correction is known but has not passed acquisition and validation
- **THEN** the API SHALL expose the predecessor's local availability separately from a provisional effective-decision state, pending correction identity, and stable reason code
- **AND** business-facing clients SHALL NOT label the predecessor as an unqualified final latest-effective report

#### Scenario: Client downloads an asset
- **WHEN** a client requests an available asset by id
- **THEN** the API SHALL validate the effective record and file integrity before streaming with a safe filename, `application/pdf`, and verified Content-Length
- **AND** it SHALL not accept a caller-supplied filesystem path

#### Scenario: Client requests a superseded or corrupt asset
- **WHEN** a content request resolves to a superseded, missing, or integrity-failed file
- **THEN** the API SHALL reject delivery with a stable conflict or gone response and SHALL NOT stream stale bytes

### Requirement: Consumer Outputs Surface Asset Lineage
Business-facing results that depend on annual reports SHALL expose sufficient shared asset lineage for audit without leaking internal archive paths.

#### Scenario: Business-profile result uses annual-report evidence
- **WHEN** a business-profile result is returned
- **THEN** its evidence lineage SHALL identify the shared asset id, source filing, report period, content hash, and effective correction status
- **AND** its consumer-processing status SHALL distinguish current results from stale or reprocessing results after a source change

#### Scenario: Broker fact uses annual-report evidence
- **WHEN** a broker regulatory fact is returned or inspected
- **THEN** its source lineage SHALL identify the shared asset id and broker processing manifest
