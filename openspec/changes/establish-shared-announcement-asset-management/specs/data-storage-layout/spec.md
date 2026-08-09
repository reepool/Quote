## ADDED Requirements

### Requirement: Shared Announcement Assets Reside On The Filings Data Volume
The system SHALL store shared official announcement attachments beneath a configurable project-relative root under `/home/python/Quote/data/filings` so the remounted data volume provides the physical capacity.

#### Scenario: Default annual-report archive is configured
- **WHEN** the shared asset service uses its default layout
- **THEN** it SHALL store annual reports in a content-addressed blob pool under a business-neutral root such as `data/filings/announcements/blobs/{hash_prefix}/{sha256}.pdf`
- **AND** business-profile and broker-specific directory names SHALL not define ownership of new source assets

#### Scenario: Unsafe archive path is configured
- **WHEN** an archive template resolves outside the configured filings root or contains traversal components
- **THEN** configuration validation SHALL fail before any file is written

### Requirement: Annual-Report Files Are Content-Verified And Effectively Unique
The storage layer SHALL use immutable content identity, atomic publication, and reference-aware deletion so each instrument and fiscal year retains one effective local annual-report attachment without duplicating identical bytes across consumers.

#### Scenario: New file is published
- **WHEN** attachment validation succeeds
- **THEN** the file SHALL be written through a temporary path, flushed, hash-verified, and atomically published
- **AND** the temporary and final paths SHALL be on the same verified filings mount
- **AND** the published file SHALL be reopened and verified after rename

#### Scenario: Same content already exists
- **WHEN** the SHA-256 blob already exists and passes integrity validation
- **THEN** the attachment SHALL reference the existing content rather than write another consumer-owned copy

#### Scenario: Superseded original is removed
- **WHEN** a verified corrected report becomes effective and the original blob has no remaining retention pin
- **THEN** a durable deletion intent SHALL be committed before unlink and finalized as deleted or failed afterward
- **AND** the old physical file SHALL be deleted only after all retention pins are released and the replacement has a verified independent-failure-domain backup

### Requirement: Existing Filings Are Adopted Without Unnecessary Copying
The migration SHALL recognize valid existing annual-report files and MAY retain an existing verified path or create a verified hard link during cutover before converging on the canonical layout.

#### Scenario: Existing file is on the filings volume
- **WHEN** an existing business-profile or broker file matches its manifest identity, length, PDF signature, and hash
- **THEN** the migration SHALL adopt it without network download

#### Scenario: Existing duplicate files are found
- **WHEN** identical valid content exists at multiple business-owned paths
- **THEN** migration SHALL preserve all copies until consumer references are switched and reconciliation passes
- **AND** it SHALL then remove redundant files according to the verified deletion plan

#### Scenario: Existing archive has mixed content
- **WHEN** annual reports share directories with semiannual reports, historical periods, derived artifacts, orphan files, or conflicts
- **THEN** cleanup SHALL default to dry-run and use a per-file manifest/hash allowlist
- **AND** no excluded file or directory SHALL be deleted

### Requirement: Filings Storage Has Capacity And Backup Gates
The archive SHALL enforce configurable free-space thresholds, planned-download preflight, and incremental backup state independently from SQLite database backup.

#### Scenario: Daily plan fits available space
- **WHEN** planned attachment bytes plus reserve fit below the warning threshold
- **THEN** scheduled acquisition MAY proceed and SHALL report planned and actual bytes

#### Scenario: Hard reserve would be violated
- **WHEN** planned or streamed bytes would violate the configured hard reserve
- **THEN** attachment prefetch SHALL stop fail-closed while metadata synchronization continues

#### Scenario: Concurrent downloads plan space
- **WHEN** multiple acquisitions reserve temporary, replacement, or unknown-length bytes concurrently
- **THEN** atomic filesystem-scoped reservations SHALL prevent aggregate use from crossing the hard reserve

#### Scenario: Filings mount identity changes
- **WHEN** the configured NFS source is unavailable, read-only, or replaced by an unapproved or local fallback mount
- **THEN** archive writes, links, moves, and deletions SHALL be blocked

#### Scenario: SQLite backup runs
- **WHEN** the existing database backup job matches `data/*.db`
- **THEN** it SHALL not be considered proof that `data/filings` source assets are backed up

#### Scenario: Filings backup is verified
- **WHEN** canonical attachment files are replicated to the configured NAS target
- **THEN** backup state SHALL record independent failure-domain identity, mount source, size/hash verification, catalog snapshot or manifest watermark, completion time, and errors
