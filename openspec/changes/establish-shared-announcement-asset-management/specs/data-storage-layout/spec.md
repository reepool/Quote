## ADDED Requirements

### Requirement: Shared Announcement Assets Reside On The Filings Data Volume
The system SHALL store shared official announcement attachments beneath a configurable project-relative root that resolves within the configured filings volume, defaulting to `data/filings`, so the remounted data volume provides the physical capacity without hard-coding a machine-specific project path.

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

#### Scenario: Existing file cannot be hard-linked
- **WHEN** the verified NFS mount does not support a safe hard link or returns a cross-filesystem or unsupported-operation error
- **THEN** migration SHALL either keep the adopted path or use copy, flush, hash verification, and same-filesystem atomic rename
- **AND** it SHALL NOT fall back to an unverified move or duplicate network download

#### Scenario: Existing archive has mixed content
- **WHEN** annual reports share directories with semiannual reports, historical periods, derived artifacts, orphan files, or conflicts
- **THEN** cleanup SHALL default to dry-run and use a per-file manifest/hash allowlist
- **AND** no excluded file or directory SHALL be deleted

#### Scenario: Legacy duplicate path is removed
- **WHEN** a verified business-owned duplicate is approved for cleanup after cutover
- **THEN** a versioned rollback manifest SHALL map its prior path and consumer identity to the shared asset and content hash
- **AND** reconstruction from a verified canonical or backup blob SHALL be tested before unlink

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
- **AND** completion, failure, cancellation, or expired lease SHALL release or reconcile the reservation idempotently

#### Scenario: Filings mount identity changes
- **WHEN** the configured NFS source is unavailable, read-only, or replaced by an unapproved or local fallback mount
- **THEN** archive writes, links, moves, and deletions SHALL be blocked

#### Scenario: SQLite backup runs
- **WHEN** the existing database backup job matches `data/*.db`
- **THEN** it SHALL not be considered proof that `data/filings` source assets are backed up

#### Scenario: Filings backup is verified
- **WHEN** canonical attachment files are replicated to the configured NAS target
- **THEN** each missing blob SHALL be copied through a target-side temporary file, flushed, length/hash verified, and atomically published before it is eligible for the backup watermark
- **AND** an existing hash-named target SHALL be reverified rather than trusted by path alone
- **AND** a present target with mismatched length or hash SHALL remain unprotected, SHALL NOT advance the paired backup watermark, and SHALL keep its path, bytes, and modification time unchanged during an ordinary backup run
- **AND** quarantine or replacement SHALL occur only inside an operator-authorized, auditable repair operation that preserves original path/hash evidence
- **AND** backup state SHALL record independent failure-domain identity, mount source, size/hash verification, a file manifest watermark paired with a recoverable catalog database snapshot, completion time, and errors

#### Scenario: Backup target shares the primary storage host
- **WHEN** the configured destination resolves to the same server or storage failure domain as the primary filings mount
- **THEN** the copy SHALL be reported as non-independent and SHALL NOT satisfy physical-deletion readiness

#### Scenario: Backup target capacity is low
- **WHEN** planned or streamed backup bytes would cross the target warning threshold, hard stop, or absolute free-space reserve
- **THEN** the backup SHALL warn, checkpoint, or fail according to the configured gate without publishing partial content
- **AND** local source assets SHALL remain valid while destructive readiness remains blocked

#### Scenario: Backup retention is evaluated
- **WHEN** superseded content-addressed blobs remain on the backup target
- **THEN** version 1 SHALL retain them as non-consumer-visible recovery content
- **AND** it SHALL NOT automatically garbage-collect them without a separately approved retention specification

#### Scenario: A paired restore is performed
- **WHEN** catalog and filing assets are restored from backup
- **THEN** the catalog database snapshot and paired file-manifest watermarks SHALL be compatible and restored blobs SHALL pass size/hash verification
- **AND** all current-effective, retention-pinned, and pending-deletion replacement blobs SHALL be reconciled rather than sampled before enablement
- **AND** consumers and destructive maintenance SHALL remain disabled until reconciliation completes without a required-blob gap
