## ADDED Requirements

### Requirement: Business-profile discovery SHALL cover the active market without existing manifests
The system SHALL discover relevant official disclosures for active in-scope issuers even when an issuer has no prior business-profile manifest or semantic run.

#### Scenario: First annual report for an unprocessed issuer
- **WHEN** an active issuer has no business-profile source manifest and an official annual report exists before the knowledge cutoff
- **THEN** the discovery frontier includes the issuer and announcement without requiring an operator-supplied instrument id

#### Scenario: Newly published correction
- **WHEN** an official correction or replacement report appears for an already processed issuer
- **THEN** the changed announcement identity enters scope and preserves supersession lineage

### Requirement: Acquisition SHALL use the minimum sufficient disclosure set
The system SHALL download only disclosures selected for an incomplete, changed, stale, or retry-due field family and SHALL reuse verified content-addressed artifacts.

#### Scenario: Existing annual report covers stable fields
- **WHEN** the latest verified annual report already covers a stable field family and its hash and runtime identities are unchanged
- **THEN** no newer unrelated disclosure is downloaded and no LLM extraction is repeated

#### Scenario: Interim report closes a time-sensitive gap
- **WHEN** a newer semiannual report contains a required time-sensitive field missing from the annual base
- **THEN** the planner may add that semiannual report without downloading the issuer's full filing history

### Requirement: Extraction SHALL minimize PDF pages and LLM calls
The system SHALL run deterministic table and keyword extraction before bounded semantic analysis and SHALL provide the LLM only selected sections needed for unresolved field-family assertions.

#### Scenario: Deterministic table is complete
- **WHEN** an identified table reconciles and contains all required structured fields
- **THEN** the system produces verified candidates without an LLM call for those fields

#### Scenario: Semantic context is missing
- **WHEN** selected pages do not contain sufficient issuer-scoped evidence
- **THEN** the system expands context deterministically within configured budgets or records machine rework rather than prompting the whole report

### Requirement: Production maintenance SHALL use layered incremental frequencies
The scheduler SHALL expose separate bounded discovery, semantic-processing, reconciliation, and annual-coverage jobs that share hashes, freshness rules, and checkpoints.

#### Scenario: Filing-season daily discovery
- **WHEN** the daily discovery job runs during a configured filing season
- **THEN** it scans only the official announcement index window and does not download PDFs or invoke the LLM

#### Scenario: Weekly semantic maintenance
- **WHEN** the weekly semantic job runs
- **THEN** it processes changed frontier items, coverage gaps, stale field families, and due machine retries within configured document, time, error, concurrency, and cost budgets

#### Scenario: Annual reconciliation
- **WHEN** annual coverage reconciliation runs after the annual-report season
- **THEN** it reports and rotates through active issuers missing required current field families without reprocessing unchanged complete issuers

### Requirement: Archive maintenance SHALL be evidence safe
The system SHALL audit content hashes and manifest references before classifying an archived PDF as removable and SHALL never automatically delete an unreferenced or superseded official artifact solely because it is old.

#### Scenario: Exact duplicate with canonical reference
- **WHEN** two paths have identical content hashes and all active manifests can be atomically repointed to one verified canonical path
- **THEN** the duplicate may be quarantined or removed with an immutable cleanup audit

#### Scenario: File has no current manifest
- **WHEN** an official PDF exists but no production manifest references it
- **THEN** the system reports or quarantines it and does not automatically delete it
