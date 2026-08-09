## ADDED Requirements

### Requirement: Announcement Asset Management Is Business-Neutral
The system SHALL provide an official announcement asset capability that is independently configured, persisted, scheduled, and callable without enabling business-profile, broker risk-control, or any other consuming business module.

#### Scenario: Business-profile is disabled
- **WHEN** business-profile acquisition and daily processing are disabled
- **THEN** annual-report backfill, daily discovery, attachment download, integrity management, and consumer access SHALL remain available

#### Scenario: A new consumer needs annual reports
- **WHEN** a future business module needs a formal annual-report attachment
- **THEN** it SHALL use the shared asset contract without implementing a provider scanner, attachment downloader, archive layout, or revision store

#### Scenario: Application process starts
- **WHEN** an API, scheduler, or business process initializes the asset capability
- **THEN** startup SHALL NOT trigger an unbounded market scan, bulk attachment download, destructive migration, or physical deletion

### Requirement: Existing Announcement Acquisition Infrastructure Is Reused
The asset capability SHALL use the existing source-neutral announcement providers, route configuration, normalized records, conservative cursors, and governed attachment retrieval rather than introducing parallel source transports.

#### Scenario: CNInfo annual-report discovery runs
- **WHEN** the asset service searches A-share annual reports
- **THEN** it SHALL construct a normalized announcement query and use configured provider capabilities and routes
- **AND** it SHALL NOT hard-code CNInfo `column`, `plate`, `orgId`, artifact hosts, headers, TLS behavior, or fallback logic in the asset service

#### Scenario: Provider fallback is attempted
- **WHEN** the primary source fails and the configured route permits fallback
- **THEN** all attempts and diagnostics SHALL be retained while the selected source record remains source-qualified

### Requirement: Canonical Announcement And Attachment Identities Are Preserved
The system SHALL distinguish legal announcement identity, attachment observation identity, physical content identity, and consumer-processing identity.

#### Scenario: Announcement has a stable provider id
- **WHEN** an official source returns a stable announcement id
- **THEN** the announcement key SHALL be `source + source_announcement_id`

#### Scenario: One announcement has multiple attachments
- **WHEN** an announcement exposes multiple attachments
- **THEN** each attachment SHALL have a stable source-qualified identity using attachment id when available or a deterministic normalized source URL identity otherwise

#### Scenario: Two filings contain identical bytes
- **WHEN** two source-qualified attachments have the same SHA-256 content
- **THEN** they SHALL share one physical blob after canonical migration or acquisition converges
- **AND** their legal announcement and attachment identities SHALL remain distinct

#### Scenario: A known announcement is observed again
- **WHEN** the same source-qualified announcement or attachment is rediscovered
- **THEN** the canonical record SHALL be updated idempotently with first/last-observed evidence and raw metadata identity
- **AND** it SHALL NOT create a duplicate legal announcement or overwrite prior attachment-version evidence

#### Scenario: One asset is parsed by two consumers
- **WHEN** business-profile and broker risk-control process the same annual-report asset
- **THEN** they SHALL share the source asset and SHALL retain separate consumer, parser-version, parameter-hash, status, and derived-output records

### Requirement: Formal Full Annual Reports Are Classified Centrally
Version 1 SHALL centrally classify A-share formal full annual reports, annual-report corrections, summaries, translations, correction notices, and related non-report announcements using a versioned deterministic policy.

#### Scenario: Formal annual report is found
- **WHEN** a title denotes a complete `年度报告` or accepted `年报` abbreviation and a PDF attachment is present
- **THEN** the record SHALL be classified as a formal full annual report with its fiscal year and report period

#### Scenario: Annual-report summary is found
- **WHEN** the title denotes an annual-report summary
- **THEN** it SHALL NOT be eligible as the effective full-report asset

#### Scenario: Translation or visual edition is found
- **WHEN** the title denotes an English translation, illustrated edition, or another non-primary rendition
- **THEN** it SHALL NOT replace the primary Chinese full report

#### Scenario: Related material or unresolvable identity is found
- **WHEN** an attachment is an audit or assurance report, inquiry or reply, briefing material, semiannual or quarterly report, or lacks a reliable instrument or fiscal-year identity
- **THEN** it SHALL NOT be eligible as the version 1 effective annual-report asset

#### Scenario: Correction notice has no full replacement report
- **WHEN** a correction announcement contains only a notice or changed pages and not a complete corrected annual-report attachment
- **THEN** it SHALL remain correction evidence and SHALL NOT replace the effective full-report attachment

#### Scenario: Complete corrected report is found
- **WHEN** a later announcement contains a complete corrected or revised annual-report PDF for the same instrument and fiscal year
- **THEN** it SHALL be eligible to supersede the prior full report

#### Scenario: One announcement has mixed attachments
- **WHEN** one announcement contains a complete primary Chinese report, summary, translation, appendix, and correction notice attachments
- **THEN** classification SHALL occur at attachment level
- **AND** only the complete primary Chinese report SHALL be eligible

#### Scenario: Cross-source candidates conflict
- **WHEN** official-source candidates for the same instrument and fiscal year have different verified content and no governed mirror or legal-precedence evidence
- **THEN** selection SHALL fail closed as ambiguous
- **AND** lexical source or filing-id order SHALL NOT choose the effective report

#### Scenario: Effective correction is withdrawn or silently changed
- **WHEN** a source withdraws an effective correction or changes the attachment observed under an existing announcement id
- **THEN** the system SHALL retain a new observation and reevaluate the effective asset
- **AND** it SHALL only fall back to a legally valid locally available predecessor or SHALL become blocked

### Requirement: One Effective Annual-Report Attachment Is Retained Per Fiscal Year
For each instrument and fiscal year, the system SHALL expose exactly one effective formal annual-report attachment after convergence, preferring a valid complete correction over a non-correction and the latest valid correction over earlier corrections.

#### Scenario: Only an original report exists
- **WHEN** one valid original full annual report exists for an instrument and fiscal year
- **THEN** it SHALL be the effective asset

#### Scenario: Correction arrives after a predecessor
- **WHEN** a complete corrected report is downloaded and passes identity, PDF signature, byte-length, and SHA-256 verification
- **THEN** the system SHALL atomically activate the correction
- **AND** it SHALL mark the original or earlier-correction attachment record superseded
- **AND** it SHALL invalidate or requeue consumer processing bound to the predecessor asset

#### Scenario: New correction fails verification
- **WHEN** a discovered correction cannot be downloaded or fails integrity validation
- **THEN** the existing effective report SHALL remain active and physically present
- **AND** the correction SHALL remain retryable with explicit diagnostics

#### Scenario: Superseded file is no longer referenced
- **WHEN** the corrected report is active and the superseded physical blob has no remaining retention pin
- **THEN** the superseded physical file SHALL be deleted
- **AND** its announcement metadata, content hash, size, prior archive path, replacement edge, deletion reason, and deletion timestamp SHALL remain auditable

#### Scenario: Superseded blob is shared
- **WHEN** another active source-qualified attachment references the same physical blob
- **THEN** the system SHALL remove only the superseded annual-report reference and SHALL retain the shared blob until all retention pins are released

#### Scenario: A physical retention pin remains
- **WHEN** a blob is held by another effective attachment, a managed legacy alias, a not-yet-migrated consumer, or an active read or processing lease
- **THEN** the physical file SHALL remain even though historical metadata does not itself require physical retention

#### Scenario: Process stops during predecessor deletion
- **WHEN** a process stops before or after unlinking a predecessor file
- **THEN** a durable `planned`, `deleting`, `deleted`, or `failed` deletion intent SHALL allow idempotent reconciliation
- **AND** audit state SHALL NOT claim physical deletion before it is confirmed

### Requirement: Local-First Ensure Is The Consumer Contract
The system SHALL provide a local-first `ensure` contract that returns a verified effective annual-report asset when available and performs bounded discovery and download only when permitted and necessary.

#### Scenario: Valid local asset exists
- **WHEN** a consumer requests an instrument and fiscal year whose effective attachment exists and passes configured integrity validation
- **THEN** the service SHALL return the local asset without a provider request or attachment download

#### Scenario: Metadata exists but attachment is absent
- **WHEN** a matching effective annual-report record exists but its attachment has not been downloaded
- **THEN** an authorized ensure request SHALL download, validate, atomically archive, and return the attachment

#### Scenario: Neither metadata nor attachment exists
- **WHEN** an authorized consumer requests a missing annual report with network acquisition allowed
- **THEN** the service SHALL execute bounded instrument-scoped discovery, register all relevant normalized metadata, select the effective full report, download it, and return the asset

#### Scenario: Caller prohibits network access
- **WHEN** a consumer requests a missing asset with network acquisition disabled
- **THEN** the service SHALL return `local_miss` disposition with explicit missing availability and SHALL NOT create an operation or contact a provider

#### Scenario: Requested period is ambiguous
- **WHEN** discovery cannot determine one effective full report for the requested instrument and fiscal year
- **THEN** the service SHALL fail closed with candidate identities and reasons rather than returning an arbitrary attachment

#### Scenario: Exact legal filing is requested
- **WHEN** a consumer requests a source-qualified announcement or filing identity
- **THEN** ensure SHALL return or acquire that exact filing under the caller's network and integrity policy
- **AND** it SHALL NOT substitute a merely similar or newer legal filing

### Requirement: Latest-Only Historical Backfill Covers The Active A-Share Universe
The system SHALL provide a resumable bootstrap that targets current active stock instruments on SSE, SZSE, and BSE and stores only the latest available effective annual-report attachment for each instrument.

#### Scenario: Instrument has several historical annual reports
- **WHEN** the bootstrap discovers multiple fiscal years for an instrument
- **THEN** it SHALL create one bootstrap effective record and physical attachment only for the latest available fiscal-year winner
- **AND** it MAY retain non-winning discovery metadata for audit without downloading those attachments

#### Scenario: Older valid files already exist before bootstrap
- **WHEN** migration inventory finds valid annual reports from earlier fiscal years that are not the latest-only bootstrap winner
- **THEN** bootstrap SHALL NOT delete them merely because they are outside its acquisition target
- **AND** any later cleanup SHALL require an independently specified per-file retention or duplicate decision

#### Scenario: Latest fiscal year has corrections
- **WHEN** the latest available fiscal year has an original and one or more complete corrections
- **THEN** the bootstrap SHALL retain only the newest valid correction as the effective attachment

#### Scenario: Instrument has no published annual report
- **WHEN** a newly listed or otherwise uncovered active instrument has no valid full annual report
- **THEN** the bootstrap SHALL record a confirmed missing or successful-empty coverage state with the searched bounds
- **AND** it SHALL remain eligible for bounded later repair and daily discovery

#### Scenario: A newer fiscal-year search is incomplete
- **WHEN** any provider, page, mapping, or candidate download needed to prove the newest fiscal year remains incomplete
- **THEN** bootstrap SHALL NOT select an older fiscal year and report it as the latest
- **AND** the instrument SHALL remain incomplete, retryable, or blocked

#### Scenario: Confirmed missing evidence expires
- **WHEN** a successful-empty decision reaches its configured expiry or its source/query fingerprint changes
- **THEN** the instrument SHALL reenter bounded coverage repair

#### Scenario: Latest correction cannot be verified during bootstrap
- **WHEN** an original is available but the legally newer complete correction cannot be downloaded or validated
- **THEN** bootstrap SHALL NOT claim final latest-effective coverage
- **AND** it SHALL expose the original only as provisional where policy permits

#### Scenario: Bootstrap is interrupted
- **WHEN** the process stops after completing part of the universe
- **THEN** a subsequent run SHALL resume from durable scope and acquisition state without redownloading verified assets or restarting completed instruments

#### Scenario: Bootstrap reaches an overall terminal result
- **WHEN** every target instrument is evaluated
- **THEN** the run SHALL report `success` only when every target is available or has unexpired confirmed-missing evidence
- **AND** any incomplete, retryable, or blocked target SHALL make the run `partial` or `blocked`, never falsely complete

#### Scenario: Delisted history is requested
- **WHEN** an operator explicitly requests an inactive or delisted instrument outside the default active-universe bootstrap
- **THEN** the same local-first ensure contract SHALL perform the permitted bounded lookup or acquisition without broadening the default scheduled universe

### Requirement: Daily Discovery Is Windowed Efficient And Fail-Closed
The daily annual-report update SHALL use category-filtered market discovery, durable provider cursors, overlap windows, bounded date partitions, and targeted missing-instrument repair.

#### Scenario: Normal daily run resumes
- **WHEN** a committed annual-report cursor exists for a source, exchange, and normalized category scope
- **THEN** the run SHALL begin from the committed watermark minus the configured calendar-day overlap and end at the run cutoff

#### Scenario: Daily discovery finds a new complete original report
- **WHEN** the version 1 daily window discovers an eligible complete annual-report original for an active A-share instrument
- **THEN** the daily workflow SHALL register its metadata and proactively ensure, validate, and archive the effective attachment

#### Scenario: Daily discovery finds an older-fiscal-year correction
- **WHEN** the daily or reconciliation window discovers a complete correction for an older fiscal year
- **THEN** it SHALL reevaluate and replace only that instrument and fiscal year
- **AND** it SHALL NOT change the effective asset for a later fiscal year

#### Scenario: No cursor exists after bootstrap
- **WHEN** daily mode starts without a valid committed cursor
- **THEN** it SHALL derive a bounded current filing-season window and SHALL NOT silently scan unbounded market history

#### Scenario: Provider page bound is exceeded
- **WHEN** a market-wide date window cannot be completed within configured page or request limits
- **THEN** selected records from completed pages SHALL be durably registered
- **AND** the window SHALL be split into smaller date partitions and resumed
- **AND** the cursor SHALL NOT advance past the incomplete interval

#### Scenario: One publication day remains too dense
- **WHEN** a single-day window still exceeds the provider bound
- **THEN** the system SHALL continue through durable page ranges or stable provider-supported subscopes under the fixed run cutoff
- **AND** the parent day SHALL complete only after every child scope completes
- **AND** if no stable completion path exists, the day SHALL remain pending with an explicit blocker

#### Scenario: Cursor query semantics change
- **WHEN** source routing, exchange scope, category, time-boundary semantics, or classification policy changes incompatibly
- **THEN** the persisted cursor SHALL NOT be reused without explicit migration or bounded rediscovery

#### Scenario: Publication timestamps meet the run boundary
- **WHEN** records share timestamps across pages, carry future timestamps, or arrive while the run is executing
- **THEN** the run SHALL preserve raw timestamps, normalize them to the configured project timezone, and apply explicit inclusive/exclusive boundaries against one fixed cutoff
- **AND** it SHALL not advance past records that cannot be proven inside the completed window

#### Scenario: A correction is indexed late
- **WHEN** a correction whose publication time falls outside the normal overlap appears later at the provider
- **THEN** a bounded rotating long-lookback reconciliation of already-covered instruments SHALL discover it within the configured maximum reconciliation period

#### Scenario: Active universe changes
- **WHEN** a stock lists, delists, or changes active state after bootstrap
- **THEN** an auditable universe refresh SHALL add new listings to coverage repair and remove delistings from the active denominator
- **AND** delisting SHALL NOT delete retained assets

#### Scenario: Market scan misses an expected current report
- **WHEN** an active instrument is expected to have a latest annual report but coverage is absent
- **THEN** a bounded rotating repair cohort SHALL run instrument-scoped annual-category discovery without forcing one query per instrument in every daily run

#### Scenario: A discovery page fails
- **WHEN** metadata discovery fails before every page or child scope in the requested window completes
- **THEN** completed metadata SHALL remain reusable
- **AND** the prior committed discovery cursor SHALL be retained
- **AND** the incomplete discovery work SHALL enter bounded retry state

#### Scenario: Attachment acquisition fails after discovery completes
- **WHEN** every metadata page in a discovery window completes but one or more selected attachments fail acquisition
- **THEN** the discovery cursor MAY advance for the completed metadata window
- **AND** attachment failures SHALL remain in a separate bounded retry queue without losing their metadata

### Requirement: Attachment Acquisition Is Atomic Idempotent And Concurrency-Safe
The asset service SHALL ensure that concurrent schedulers, API requests, and business consumers cannot download or publish duplicate physical assets for the same attachment observation.

#### Scenario: Two consumers request the same missing asset
- **WHEN** two callers concurrently ensure the same source-qualified attachment
- **THEN** one caller SHALL hold the acquisition lease and perform the download
- **AND** the other caller SHALL wait for or reuse the committed result
- **AND** only one physical blob SHALL be published

#### Scenario: Process stops during download
- **WHEN** a process terminates while writing a temporary attachment
- **THEN** no partial file SHALL be visible as a valid asset
- **AND** an expired lease SHALL allow a later retry to clean or replace the temporary file

#### Scenario: Existing file has matching evidence
- **WHEN** an existing path has the registered byte length, valid PDF signature, and SHA-256
- **THEN** it SHALL be adopted or reused without network acquisition

#### Scenario: Archive mount identity is unsafe
- **WHEN** the filings NFS is missing, read-only, remounted to an unapproved source, or has become a local fallback directory
- **THEN** attachment writes and destructive file operations SHALL be blocked

#### Scenario: Concurrent operations reserve storage
- **WHEN** several attachment acquisitions pass individual space preflight concurrently
- **THEN** atomic filesystem-scoped byte reservations SHALL prevent their combined temporary and final bytes from violating the hard reserve

#### Scenario: Existing file is corrupt
- **WHEN** an existing registered file is missing, unreadable, not a PDF, size-mismatched, or hash-mismatched
- **THEN** it SHALL not satisfy local-first access
- **AND** the service SHALL quarantine or mark it corrupt before bounded reacquisition

### Requirement: Asset Operations Are Durable Idempotent And Recoverable
Backfill, daily, ensure, migration, integrity, deletion, and backup work SHALL use durable operation state and leases rather than process-local background-task state.

#### Scenario: The same ensure scope is requested concurrently
- **WHEN** scheduler, API, or consumers request the same normalized instrument/fiscal-year or exact-filing scope under the same policy version
- **THEN** the service SHALL create at most one active operation for the idempotency scope
- **AND** every caller SHALL receive the existing operation identity or the completed local asset

#### Scenario: A worker stops during an operation
- **WHEN** a process exits after persisting progress or holding a lease
- **THEN** operation status, stage, checkpoint, attempts, heartbeat, retry time, and bounded diagnostics SHALL remain queryable
- **AND** lease expiry SHALL permit safe resume without repeating verified work

#### Scenario: Operation state is exposed
- **WHEN** an operation is queried
- **THEN** `queued|running|completed|missing|failed|blocked|cancelled|expired` status SHALL be separate from its discovery/download/validation/activation/backup stage
- **AND** batch `success|partial|blocked|failed` outcomes, `local_hit|local_miss|operation_created|operation_reused` ensure disposition, and asset availability SHALL remain separate concepts

#### Scenario: Cancellation is unsupported or unsafe
- **WHEN** a caller requests cancellation for a non-cancellable stage or version 1 cancellation is disabled
- **THEN** the API SHALL reject the request explicitly while lease expiry and bounded recovery remain defined

#### Scenario: Shared rollout is rolled back before physical cleanup
- **WHEN** an operator disables shared consumer routing or daily writes before legacy files are removed
- **THEN** additive canonical metadata, replacement lineage, operation history, and audit records SHALL remain intact
- **AND** rollback SHALL use feature gates rather than deleting shared database state

### Requirement: Effective-Asset Changes Are Durable And Replayable
The system SHALL append a durable monotonic change event or watermark whenever an effective annual-report asset is added, replaced, repaired, withdrawn, or physically removed.

#### Scenario: A consumer is offline during a correction
- **WHEN** a correction becomes effective while a registered consumer is not running
- **THEN** the consumer SHALL resume from its own checkpoint and receive the affected instrument, fiscal year, predecessor asset, and replacement asset
- **AND** idempotent replay SHALL NOT require announcement rediscovery or attachment redownload

### Requirement: Existing Annual-Report Files Are Reconciled And Reused
Migration SHALL inventory existing annual-report manifests and paths, validate their identities and content, and adopt valid files before enabling new downloads.

#### Scenario: Initial migration inventory runs
- **WHEN** an operator inventories existing business-profile and broker archives
- **THEN** the default operation SHALL be read-only and SHALL NOT download, move, link, quarantine, or delete files
- **AND** it SHALL report adoptable, duplicate, missing, corrupt, conflicting, orphan, derived, and out-of-scope entries

#### Scenario: Business-profile archive contains a valid latest report
- **WHEN** an existing business-profile manifest and file identify the selected latest effective annual report
- **THEN** migration SHALL register that file as the shared asset without redownloading it

#### Scenario: Broker archive contains the only valid copy
- **WHEN** a broker annual-report manifest contains the selected latest effective annual report and no business-profile copy exists
- **THEN** migration SHALL register and reuse the broker file as the shared asset

#### Scenario: Duplicate valid copies exist
- **WHEN** business-profile and broker archives contain the same source filing and content hash at different paths
- **THEN** migration SHALL select one canonical physical file or create one verified canonical link
- **AND** it SHALL switch consumers before deleting the redundant copy

#### Scenario: Existing manifests disagree
- **WHEN** source id, instrument, report period, content hash, or file contents conflict
- **THEN** migration SHALL report the conflict and SHALL NOT delete or silently merge either file

#### Scenario: Legacy directories contain unrelated files
- **WHEN** an archive contains semiannual reports, other fiscal years, derived artifacts, orphans, or conflicting files beside adoptable annual reports
- **THEN** migration cleanup SHALL use an explicit per-file manifest/hash allowlist and default dry-run
- **AND** it SHALL NOT delete any excluded file or perform directory-level cleanup

### Requirement: Archive Layout And Storage Gates Are Governed
The shared archive SHALL reside below the remounted `data/filings` volume, use safe project-relative configuration, include immutable content identity in filenames or blob paths, and enforce disk and attachment-size gates.

#### Scenario: New annual-report file is stored
- **WHEN** a verified attachment is committed
- **THEN** its canonical path SHALL be derived from a content-addressed SHA-256 blob pool beneath the configured root
- **AND** it SHALL not be written beneath a business-owned archive root

#### Scenario: Two identities share one content hash
- **WHEN** two legal attachment identities resolve to identical verified content
- **THEN** the blob pool SHALL retain one physical SHA-256 object
- **AND** legal identities SHALL remain database projections rather than duplicate physical files

#### Scenario: Free-space warning threshold is crossed
- **WHEN** the configured warning utilization is reached
- **THEN** the job SHALL emit an operational warning with planned and available bytes

#### Scenario: Stop threshold is crossed
- **WHEN** available storage is below the configured hard reserve or utilization stop threshold
- **THEN** scheduled prefetch SHALL stop before downloading new attachments
- **AND** metadata synchronization SHALL remain allowed
- **AND** explicit on-demand requests SHALL return an actionable storage blocker unless an operator override is authorized

#### Scenario: Attachment exceeds configured limit
- **WHEN** Content-Length or streamed bytes exceed the annual-report attachment limit
- **THEN** acquisition SHALL fail explicitly without publishing a partial file

### Requirement: Source Assets And Business Processing Remain Separate
The shared asset state SHALL not be overwritten by business parser outcomes, and consumer processing SHALL be independently retryable and versioned.

#### Scenario: Broker parser fails
- **WHEN** broker risk-control parsing fails for a valid shared annual-report asset
- **THEN** the shared asset SHALL remain valid and reusable
- **AND** only the broker processing record SHALL be marked failed

#### Scenario: Business-profile parser upgrades
- **WHEN** business-profile changes its PDF, section, or semantic parser version
- **THEN** it SHALL reuse the same verified source asset and create new derived processing identity without downloading the annual report again

#### Scenario: Effective asset changes
- **WHEN** a correction supersedes the asset used by a consumer
- **THEN** affected consumer processing records SHALL be marked superseded or requeued according to consumer policy
- **AND** old derived facts SHALL not remain silently current

### Requirement: Stable Internal And API Access Is Provided
The system SHALL expose shared annual-report asset access through DataManager/service contracts and additive FastAPI endpoints without exposing arbitrary filesystem access.

#### Scenario: Business service requests an asset
- **WHEN** an internal workflow requests the effective annual report
- **THEN** it SHALL receive a structured asset reference containing asset id, instrument, fiscal year, report period, source, filing id, publication time, correction status, content hash, length, integrity status, and controlled local handle or path abstraction

#### Scenario: Front-facing client checks availability
- **WHEN** a client queries annual-report status for an instrument
- **THEN** the API SHALL return effective-version metadata, local availability, integrity, correction status, acquisition status, and last checked time
- **AND** it SHALL not disclose an unrestricted server filesystem path

#### Scenario: Client requests missing asset acquisition
- **WHEN** an authorized client requests ensure/download for a missing report
- **THEN** the API SHALL create or reuse a bounded acquisition job and return its stable task identity and current status rather than holding an unbounded HTTP request open

#### Scenario: A read-only API is called
- **WHEN** a client invokes an annual-report metadata GET or an existing business-profile GET
- **THEN** the request SHALL perform zero provider calls and zero attachment writes

#### Scenario: Mutation authorization is not configured
- **WHEN** no trusted identity and scoped authorization boundary exists
- **THEN** ensure, content-streaming, cancellation, and operator endpoints SHALL remain disabled or fail closed

#### Scenario: Frontend observes progress
- **WHEN** a client inspects an acquisition and its downstream business result
- **THEN** asset availability, ensure disposition, durable operation state, and consumer-processing state SHALL be returned as separate fields

#### Scenario: Client downloads an available report
- **WHEN** an authorized endpoint streams a local annual report
- **THEN** it SHALL resolve the asset by identifier, validate current availability, set a safe filename and media type, and prevent path traversal

### Requirement: Asset Operations Are Observable And Auditable
Backfill, daily, on-demand, migration, deletion, and integrity operations SHALL emit structured results and stage logs sufficient to prove coverage and diagnose failures.

#### Scenario: Daily update completes
- **WHEN** the daily task finishes
- **THEN** its result SHALL include target exchanges, discovery and reconciliation windows, pages and requests, records seen, formal reports selected, corrections selected, local hits, downloads, reused existing files, bytes reserved/written, superseded files, deletion states, retries, missing coverage, cursor decisions, storage/backup gates, elapsed time, and errors by source

#### Scenario: Latest-only backfill completes
- **WHEN** the bootstrap reaches terminal coverage
- **THEN** it SHALL report target active instruments, instruments with effective assets, confirmed missing, retryable, incomplete, and blocked instruments, corrections chosen, existing files adopted, downloaded files, conflicts, validation failures, and checkpoint completeness

#### Scenario: Physical predecessor is deleted
- **WHEN** any superseded original or earlier-correction file is deleted
- **THEN** an append-only audit SHALL identify the old asset, replacement asset, hashes, paths, reason, retention-pin decision, actor or job, and timestamp

#### Scenario: Operator inspects readiness
- **WHEN** readiness is queried
- **THEN** it SHALL report active-universe coverage, attachment readiness, integrity failures, pending discovery/reconciliation windows, retry queues, storage reservations/gates, backup freshness and unprotected bytes, bootstrap completion, scheduler enablement/last result, and consumer migration status

### Requirement: File Backup Protects The Shared Archive
Canonical attachment files SHALL have a governed incremental backup or replication workflow separate from SQLite online database backup.

#### Scenario: Archive backup runs
- **WHEN** new or changed canonical blobs exist
- **THEN** the backup workflow SHALL copy only missing content-addressed files to the configured backup mount and SHALL verify size and hash for copied content

#### Scenario: Backup mount is unavailable
- **WHEN** the configured NAS mount is missing or resolves to an unsafe local fallback
- **THEN** the backup SHALL fail and alert without writing a full archive copy to the local data volume

#### Scenario: Backup shares the primary failure domain
- **WHEN** backup and primary filings resolve to the same storage server or otherwise non-independent failure domain
- **THEN** that copy SHALL NOT satisfy the predecessor-deletion backup gate

#### Scenario: File and catalog recovery watermarks disagree
- **WHEN** a replacement blob backup cannot be paired with a recoverable catalog database snapshot or consistent manifest watermark
- **THEN** predecessor physical deletion SHALL remain blocked

#### Scenario: Asset has not been backed up
- **WHEN** a canonical asset is locally valid but has no verified backup copy
- **THEN** readiness SHALL expose the backup gap
- **AND** physical deletion of its predecessor SHALL remain blocked until the replacement backup is verified in an independent failure domain

#### Scenario: Database and attachment assets are restored
- **WHEN** an operator restores the announcement-asset capability after data loss
- **THEN** a compatible catalog database snapshot and attachment backup watermark SHALL be restored together
- **AND** hash/integrity reconciliation SHALL complete before consumer reads, destructive cleanup, or daily writes are re-enabled

### Requirement: The Capability Is Extensible Beyond Annual Reports
The data model and service boundaries SHALL permit future semiannual and other announcement attachment types without weakening version 1 annual-report rules.

#### Scenario: Semiannual support is added later
- **WHEN** a future change enables semiannual reports
- **THEN** it SHALL reuse announcement, attachment, blob, acquisition, integrity, API, and processing contracts while defining its own effective-version and retention policy

#### Scenario: Version 1 discovers a non-annual announcement type
- **WHEN** semiannual or another announcement metadata record is observed before its attachment policy is enabled
- **THEN** version 1 SHALL NOT proactively download that attachment as part of annual-report maintenance
- **AND** it MAY retain normalized metadata according to the existing announcement-layer policy

#### Scenario: Domain-specific announcement is added later
- **WHEN** another announcement type gains multiple consumers
- **THEN** its raw attachment MAY use the shared asset store
- **AND** its business classification, parsing, and fact approval SHALL remain outside the generic asset layer unless separately specified
