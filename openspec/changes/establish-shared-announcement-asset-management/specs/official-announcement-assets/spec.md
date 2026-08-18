## ADDED Requirements

### Requirement: Shared Annual-Report Asset Catalog

The system SHALL maintain source-qualified annual-report announcement metadata,
attachment versions, SHA-256 blob references, and one effective asset projection
per `(instrument_id, fiscal_year)` in the shared announcement-asset database.

#### Scenario: Two business modules request the same report

- **WHEN** one module has already acquired a valid annual report
- **THEN** another module receives the same local asset
- **AND** no provider request or duplicate blob is created

### Requirement: Full-Report Classification And Correction Selection

The system SHALL exclude summaries and notice-only documents and SHALL select the
newest valid complete correction over the original for the same fiscal year.

#### Scenario: A correction is published

- **WHEN** a valid full corrected annual report is discovered
- **THEN** it becomes the only current asset for that stock and fiscal year
- **AND** the predecessor is no longer returned as current

#### Scenario: A summary or correction notice is discovered

- **WHEN** the title or verified PDF identifies a summary or notice-only document
- **THEN** the attachment is retained as metadata but is not an eligible current
  annual-report asset

### Requirement: Local-First On-Demand Acquisition API

The system SHALL expose an API that checks local metadata and attachment integrity
first and, when authorized, acquires a missing requested annual report through the
shared module.

#### Scenario: Requested report exists locally

- **WHEN** a caller requests an available report
- **THEN** the API returns its metadata/content handle without network access

#### Scenario: Requested report is missing locally

- **WHEN** acquisition is allowed and a provider has the report
- **THEN** the system persists metadata, downloads and verifies the PDF, and
  returns the newly local asset

### Requirement: Latest-Only Full-Market Bootstrap

The system SHALL support a bounded historical bootstrap that records the latest
effective annual report for every currently active SSE, SZSE, and BSE A-share
instrument, without downloading adjacent older fiscal years after a winner is
found.

#### Scenario: Bootstrap resumes

- **WHEN** a prior run stopped after some instruments completed
- **THEN** the next run reuses completed metadata/files and continues remaining
  instruments without redownloading valid assets

### Requirement: Existing Asset Reuse

The system SHALL identify and register valid existing annual-report files when
their instrument, fiscal year, source identity, PDF signature, length, and SHA-256
can be verified.

#### Scenario: Existing file matches discovered metadata

- **WHEN** a valid local file matches the required annual report
- **THEN** it is reused without a provider attachment download

### Requirement: API-Only Consumer Integration

The system SHALL be callable independently by business-profile, broker risk
control, and future modules; consumer parser completion SHALL NOT block asset
discovery, storage, or local reads.

#### Scenario: Consumer parser is unavailable

- **WHEN** the shared asset is available but a consumer parser is unfinished
- **THEN** the asset API remains available and the scheduler continues normally
