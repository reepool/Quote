## ADDED Requirements

### Requirement: Source-Neutral Announcement Contract
The system SHALL represent official announcement queries, records, attachments, scan results, and retrieval results through source-neutral contracts that do not require a caller to import a source-specific record type.

#### Scenario: Consumer queries announcements without CNInfo types
- **WHEN** a disclosure-driven business workflow requests official announcements
- **THEN** it SHALL construct a source-neutral query and receive source-neutral announcement records regardless of the selected provider

#### Scenario: Source-specific evidence is retained
- **WHEN** a provider normalizes an announcement response
- **THEN** the normalized record SHALL retain the source name, source announcement id, source-qualified key, raw payload, and normalization diagnostics

### Requirement: Normalized Point-in-Time Metadata
The announcement contract SHALL preserve the metadata needed for point-in-time research, including title, publication time, market or exchange, instrument identifiers when available, and normalized attachment metadata.

#### Scenario: Publication timestamp is available
- **WHEN** a source returns a publication timestamp
- **THEN** the provider SHALL expose a timezone-aware normalized `published_at` value and retain the raw source value

#### Scenario: Publication timestamp is missing or ambiguous
- **WHEN** a source does not provide a reliable publication timestamp or timezone
- **THEN** the record SHALL preserve the raw value, mark the normalization limitation, and SHALL NOT invent a precise availability time

#### Scenario: Announcement contains multiple attachments
- **WHEN** a source returns more than one attachment for an announcement
- **THEN** the normalized record SHALL preserve each attachment separately rather than silently selecting only one

### Requirement: Stable Source-Qualified Identity
The system SHALL identify announcement evidence by source and source announcement identity and SHALL NOT automatically merge announcements from different sources based only on title or publication date.

#### Scenario: Provider supplies a stable announcement id
- **WHEN** a provider response contains a stable announcement id
- **THEN** the announcement key SHALL include the source and that source announcement id

#### Scenario: Provider lacks a stable announcement id
- **WHEN** a provider has no stable announcement id
- **THEN** the provider MAY generate a deterministic source-qualified fallback key from normalized evidence fields and SHALL mark the identity as derived

#### Scenario: Similar announcements exist on two official sites
- **WHEN** two sources return similar titles and dates
- **THEN** the acquisition layer SHALL preserve two source records unless a separate governed reconciliation process links them

### Requirement: Provider Capability Declaration
Each announcement provider SHALL declare its supported exchanges, query scopes, filters, cursor behavior, page limits, and attachment retrieval capability before it is eligible for routing.

#### Scenario: Query requests unsupported capability
- **WHEN** a query requests an instrument scope, date filter, keyword filter, category filter, or attachment operation that the selected provider does not support
- **THEN** the acquisition service SHALL reject or explicitly degrade the query before implying successful coverage

#### Scenario: Provider-specific identity is required
- **WHEN** a provider requires an internal organization id or similar source identity for an instrument query
- **THEN** the provider SHALL resolve or validate that identity internally and SHALL expose a typed not-found or failed diagnostic to the caller

### Requirement: Registry and Configuration-Driven Routing
The system SHALL separate provider registration and capability eligibility from configuration-driven route priority by purpose and exchange.

#### Scenario: Primary source succeeds
- **WHEN** the configured primary provider completes with a usable result
- **THEN** the service SHALL return that result without calling lower-priority providers

#### Scenario: Configured fallback condition occurs
- **WHEN** the primary result matches a configured fallback condition such as failed, degraded, identity-not-found, or successful-empty
- **THEN** the service SHALL attempt only the eligible configured fallback providers in order and SHALL retain diagnostics for every attempt

#### Scenario: Route references ineligible provider
- **WHEN** configuration routes a purpose and exchange to a provider that is unavailable or lacks the required capability
- **THEN** route validation SHALL fail with a clear configuration error before the workflow starts

### Requirement: CNInfo Provider Implementation
CNInfo SHALL be implemented as a concrete official announcement provider behind the source-neutral contract while preserving its bounded pagination, stock identity lookup, retry, pacing, TLS, raw payload, and effective page-size behavior.

#### Scenario: CNInfo page size exceeds upstream limit
- **WHEN** a caller requests a CNInfo page size above the verified upstream maximum
- **THEN** the provider SHALL cap the effective request size and report the effective bound in diagnostics

#### Scenario: CNInfo returns a supported response variant
- **WHEN** CNInfo returns announcements through any supported response container
- **THEN** the provider SHALL normalize the records consistently and retain the original response rows

#### Scenario: CNInfo request fails after retries
- **WHEN** a CNInfo request exhausts its configured retries
- **THEN** the scan SHALL be failed or degraded with an explicit error and SHALL NOT be reported as a confirmed empty result

### Requirement: Reusable Official Exchange Providers
The existing SSE, SZSE, and BSE announcement endpoint implementations SHALL be reusable provider implementations independent of company business-profile classification.

#### Scenario: Exchange provider discovers an announcement
- **WHEN** an SSE, SZSE, or BSE provider returns a source row for the requested instrument and interval
- **THEN** it SHALL normalize the row into the common announcement contract before any business-purpose classifier runs

#### Scenario: Business-profile workflow uses an exchange fallback
- **WHEN** the business-profile route selects an official exchange fallback
- **THEN** the common provider SHALL return normalized announcements and the business-profile module SHALL perform its own document classification downstream

### Requirement: Business Semantics Remain Caller-Owned
The acquisition layer SHALL NOT infer corporate-action terms, shareholder facts, company business activities, broker regulatory facts, financial facts, or approval status from announcement titles or content.

#### Scenario: One announcement serves multiple purposes
- **WHEN** the same normalized announcement is evaluated by multiple business workflows
- **THEN** each workflow SHALL apply its own selector, parser, purpose key, and selection reasons without modifying the source record

#### Scenario: Announcement is downloaded successfully
- **WHEN** an attachment is retrieved and validated by the common layer
- **THEN** retrieval success SHALL NOT by itself mark any derived business fact as valid, approved, or available for production calculations

### Requirement: Source-Neutral Scan State
The system SHALL persist scan state by purpose, source, and deterministic normalized scan scope rather than by CNInfo-only market and column fields.

#### Scenario: Two sources scan the same market for one purpose
- **WHEN** CNInfo and an exchange provider scan the same market for the same purpose
- **THEN** each source SHALL maintain an independent cursor and diagnostics record

#### Scenario: Instrument-scoped and market-scoped scans coexist
- **WHEN** a purpose performs both instrument-scoped and market-scoped scans
- **THEN** their deterministic scope keys SHALL prevent either scan from overwriting the other's state

### Requirement: Conservative Cursor Commit
The system SHALL advance a committed announcement cursor only after a complete successful bounded scan of the requested scope and SHALL retain the previous cursor for partial, failed, indeterminate, or prematurely bounded scans.

#### Scenario: A later page fails
- **WHEN** earlier pages succeed but a requested later page fails
- **THEN** the scan SHALL expose the records already observed for diagnostics but SHALL retain the previously committed cursor

#### Scenario: Scan reaches configured bound before prior cursor
- **WHEN** the maximum page or request bound is reached before the provider reaches the prior cursor or completes the requested interval
- **THEN** the scan SHALL be marked incomplete and SHALL NOT advance the committed cursor

#### Scenario: Complete scan succeeds
- **WHEN** all required pages for the bounded query succeed and the provider reports completion
- **THEN** the system SHALL commit the new provider cursor and maximum normalized publication time atomically with scan diagnostics

### Requirement: Empty and Failure Semantics
The system SHALL distinguish a successful empty result from provider failure, malformed payload, unsupported coverage, and identity-not-found.

#### Scenario: Provider confirms zero records
- **WHEN** a provider successfully completes the requested scope and explicitly returns zero announcement records
- **THEN** the result MAY be `success_empty` and SHALL retain the completed-scan evidence

#### Scenario: Payload cannot be normalized
- **WHEN** a response is malformed or its supported record container cannot be established
- **THEN** the result SHALL be failed or indeterminate and SHALL NOT be converted to `success_empty`

### Requirement: Purpose-Specific Announcement Audit
The system SHALL persist selected announcement metadata in a source-neutral audit store with purpose, source-qualified identity, instrument linkage when known, selection reasons, raw payload, ingestion lineage, and timestamps.

#### Scenario: Announcement is selected for a purpose
- **WHEN** a caller selector accepts a normalized announcement
- **THEN** the system SHALL idempotently store a purpose-specific audit row without changing the source announcement record

#### Scenario: Announcement is not selected
- **WHEN** an announcement does not match a purpose selector
- **THEN** the system SHALL count it in scan diagnostics but SHALL NOT be required to persist a purpose audit row

#### Scenario: Same announcement is selected for two purposes
- **WHEN** one source-qualified announcement is selected for two different purpose keys
- **THEN** the audit store SHALL preserve two independent purpose selections and their reasons

### Requirement: Governed Attachment Retrieval
The system SHALL provide source-aware attachment URL resolution and bounded retrieval with configured TLS, headers, timeout, retry, pacing, redirects, byte limits, content diagnostics, and content hashing.

#### Scenario: Relative attachment URL is returned
- **WHEN** a provider returns a relative attachment URL
- **THEN** the provider or attachment resolver SHALL produce an absolute approved-source URL without business modules hard-coding the source host

#### Scenario: Attachment exceeds configured size
- **WHEN** a download exceeds the configured byte limit
- **THEN** retrieval SHALL stop and return an explicit failure without writing a partial document as a valid artifact

#### Scenario: Retrieved content is a valid PDF
- **WHEN** a caller requires PDF content and the response has a valid PDF signature
- **THEN** the retrieval result SHALL expose the bytes, content hash, length, final URL, media-type diagnostics, and retrieval timestamp

#### Scenario: Retrieval redirects to unapproved host
- **WHEN** an attachment request redirects outside the provider's approved host policy
- **THEN** retrieval SHALL fail explicitly and SHALL NOT follow the redirect as trusted official evidence

### Requirement: Business Archives Remain Independent
The common acquisition capability SHALL return retrieval evidence without controlling business-specific archive paths, source-file manifests, correction lineage, PDF parsing, OCR, page extraction, or fact storage.

#### Scenario: Business-profile document is retrieved
- **WHEN** the common layer downloads a business-profile attachment
- **THEN** the business-profile archive service SHALL continue to own its immutable path, manifest, correction handling, and PDF artifact workflow

#### Scenario: Corporate-action document is retrieved
- **WHEN** the common layer downloads a corporate-action attachment
- **THEN** the corporate-action service SHALL continue to own its page selection, parser version, evidence storage, and derived-term workflow

### Requirement: Finite Migration and Mandatory Legacy Retirement
The source-neutral persistence and interfaces SHALL preserve existing CNInfo scan state, audit evidence, archived documents, and business outputs during a finite migration window, and the completed change SHALL remove superseded runtime implementations and storage containers.

#### Scenario: Existing CNInfo rows are migrated
- **WHEN** the generic announcement storage is initialized against a database containing legacy CNInfo state or audit rows
- **THEN** the migration SHALL backfill equivalent source-neutral rows idempotently with `source=cninfo` and SHALL preserve their cursor, selection-reason, ingestion-lineage, and raw-payload evidence

#### Scenario: Legacy storage method is called during transition
- **WHEN** an unmigrated consumer calls a legacy CNInfo announcement storage method
- **THEN** a temporary compatibility adapter SHALL preserve its observable behavior while delegating to generic storage and SHALL be tracked for removal before change completion

#### Scenario: Consumer migration changes internal types
- **WHEN** an existing business workflow moves from CNInfo-specific to source-neutral records
- **THEN** its public result schema and persisted business facts SHALL remain unchanged unless a separately documented correctness fix requires a change

#### Scenario: Migrated data is verified
- **WHEN** all consumers have switched to the source-neutral acquisition and storage interfaces
- **THEN** the rollout SHALL reconcile legacy and generic row counts, source-qualified keys, cursor values, selection reasons, and raw-payload hashes before deleting legacy storage

#### Scenario: Legacy runtime is retired
- **WHEN** migration verification succeeds
- **THEN** the same change SHALL remove legacy CNInfo scanner facade types, legacy storage methods, direct consumer imports, dual-write or fallback branches, obsolete configuration keys, duplicated exchange or attachment transport, and superseded tests or exports

#### Scenario: Legacy tables are dropped
- **WHEN** migrated announcement state and audit evidence has passed reconciliation and a pre-cleanup backup has been verified
- **THEN** the migration SHALL drop `cninfo_announcement_scan_state` and `cninfo_announcement_audit` and the final runtime schema SHALL use only the source-neutral announcement tables

#### Scenario: Completed rollout is inspected for residue
- **WHEN** the change is evaluated for completion
- **THEN** repository-wide code and configuration scans plus a clean-database schema test SHALL find no active legacy announcement implementation outside versioned migration history and historical documentation

#### Scenario: Any active legacy residue remains
- **WHEN** an old scanner import, storage wrapper, runtime table, dual-write or fallback branch, obsolete configuration key, scheduler or script parameter, duplicated source transport, duplicated attachment downloader, or superseded test remains active
- **THEN** rollout SHALL be blocked until the common implementation replaces it and the residue is removed

#### Scenario: Rollback is required after cleanup
- **WHEN** the new release must be rolled back after legacy tables and runtime implementations have been removed
- **THEN** operators SHALL restore the verified pre-cleanup database backup together with the preceding application release rather than retaining legacy code or tables in the new release

### Requirement: Bounded and Observable Operation
Announcement acquisition SHALL be bounded, rate-limited, retry-aware, and observable at key stages without silently spawning untracked work.

#### Scenario: Bounded scan runs
- **WHEN** a provider scan starts
- **THEN** logs and result diagnostics SHALL identify purpose, source, scope, effective page/request limits, pages attempted, records seen, selected count, stop reason, elapsed time, and error summary without exposing secrets

#### Scenario: Request times out
- **WHEN** a synchronous provider request reaches its configured timeout
- **THEN** the provider SHALL terminate or fail that request before starting dependent work and SHALL NOT leave an untracked background request running

### Requirement: Offline and Parity Validation
The implementation SHALL have fixture-based unit tests for provider normalization, routing, cursor behavior, persistence migration, attachment retrieval, and migrated consumer parity, while live official-site checks remain bounded integration validation.

#### Scenario: Unit test suite runs without network access
- **WHEN** announcement acquisition unit tests execute
- **THEN** providers and attachment retrieval SHALL use injected sessions or fixture transports and SHALL NOT require live official websites

#### Scenario: Existing consumer is migrated
- **WHEN** a disclosure consumer switches to the common acquisition service
- **THEN** parity tests SHALL compare selected identities, timestamps, attachment URLs, counts, diagnostics, and watermark decisions against the pre-migration behavior

#### Scenario: Live source validation is requested
- **WHEN** an operator runs official-site validation
- **THEN** the validation SHALL use explicit bounded instruments, dates, pages, pacing, dry-run behavior, and source diagnostics rather than a full-market crawl
