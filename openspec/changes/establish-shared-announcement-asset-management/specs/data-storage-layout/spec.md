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

#### Scenario: Unsafe dynamic path segment is supplied
- **WHEN** a hash, operation id, managed alias, source identity, or other dynamic value is expanded into a controlled archive, temporary, quarantine, or backup path
- **THEN** every segment SHALL match the canonical allowlist for its declared type, including exactly 64 lowercase hexadecimal characters for a SHA-256 blob name
- **AND** validation SHALL reject empty, `.`, `..`, absolute, separator-containing, encoded-traversal, control-character, or otherwise non-canonical segments before path resolution or filesystem mutation

### Requirement: Annual-Report Files Are Content-Verified And Effectively Unique
The storage layer SHALL use immutable content identity, atomic publication, and reference-aware deletion so each instrument and fiscal year with a legally valid available candidate retains one effective local annual-report attachment without duplicating identical bytes across consumers.

A governed no-winner period SHALL retain zero consumer-visible current attachments while preserving its decision, recovery, and audit evidence.

#### Scenario: New file is published
- **WHEN** attachment validation succeeds
- **THEN** the file SHALL be written through a temporary path, flushed, hash-verified, and atomically published
- **AND** the temporary and final paths SHALL be on the same verified filings mount
- **AND** the published file SHALL be reopened and verified after rename

#### Scenario: Same content already exists
- **WHEN** the SHA-256 blob already exists and passes integrity validation
- **THEN** the attachment SHALL reference the existing content rather than write another consumer-owned copy

#### Scenario: Superseded original is removed
- **WHEN** a verified corrected report becomes effective and the original blob has no remaining non-recovery retention pin that blocks primary unlink
- **THEN** a durable deletion intent SHALL be committed before unlink and finalized as deleted or failed afterward
- **AND** the deletion intent SHALL retain a recovery pin and reserved `recovery_pair_id`
- **AND** its predecessor SHALL remain in the backup required set while the intent is `planned|deleting`, even before the immutable recovery-manifest row is committed
- **AND** the old physical file SHALL be deleted only after every non-recovery/deletion-blocking pin is released and both predecessor and replacement have verified independent-failure-domain backup paired with the catalog recovery watermark
- **AND** the predecessor SHALL first have an immutable indefinitely-active version 1 recovery-manifest entry binding its prior legal/attachment identity, path/hash, replacement, backup object, verified file-manifest watermark, and reserved `recovery_pair_id`
- **AND** that manifest SHALL record the later catalog snapshot as not yet closed
- **AND** a recoverable catalog snapshot SHALL then include the manifest
- **AND** an append-only recovery-pair closure record SHALL bind the pair id to the catalog snapshot identity/hash and file-manifest watermark after bidirectional verification
- **AND** only after that closure record is durable SHALL compare-and-swap convert the recovery pin from primary-unlink protection into a non-primary-blocking permanent backup `required_set_hold`
- **AND** crash recovery SHALL preserve either primary protection or the durable required-set hold at every boundary
- **AND** the immutable manifest SHALL remain unchanged when the pair is closed

#### Scenario: Replacement legal filings share one physical blob
- **WHEN** a verified correction changes the effective legal filing but predecessor and replacement attachment observations reference the same SHA-256 blob
- **THEN** activation SHALL persist the legal decision/replacement/outbox transition and dereference the predecessor attachment without creating a physical-unlink deletion intent for the still-effective shared blob
- **AND** deletion audit SHALL use a versioned `not_applicable_shared_blob` outcome or equivalent non-unlink evidence and SHALL NOT claim that bytes were removed

#### Scenario: Deletion finalization observes a mount change
- **WHEN** a predecessor unlink has been attempted but the approved filings mount changes or becomes unverifiable before the deletion intent is finalized
- **THEN** the reconciler SHALL NOT infer success from path absence on a fallback or different mount
- **AND** it SHALL keep the intent `deleting` while readiness/cleanup reports a `blocked` mount-finalization condition until the operation-captured approved mount identity is re-established and absence is confirmed on that same mount

#### Scenario: A read or processing lease appears expired
- **WHEN** deletion encounters a retention lease whose TTL has elapsed
- **THEN** a reconciler SHALL compare owner, heartbeat, generation, and safety-grace evidence before releasing the pin
- **AND** a newer heartbeat, lease generation, or uncertain live reader SHALL continue to block deletion
- **AND** crash recovery and stale-lease reclamation SHALL be idempotent so an abandoned lease cannot retain a blob indefinitely

#### Scenario: Temporary or quarantine storage becomes stale
- **WHEN** `.part` or quarantine bytes remain after failure, cancellation, or lease expiry
- **THEN** the storage layer SHALL track their operation owner, lease generation, age, and actual bytes separately from byte reservations
- **AND** a stale `.part` SHALL be reclaimed only after heartbeat/generation reconciliation and a safety grace period
- **AND** quarantine age/byte limits SHALL affect readiness while physical cleanup requires an operator-authorized audited command

#### Scenario: Temporary or quarantine thresholds are crossed
- **WHEN** actual `.part` or quarantine age/bytes reach configured warning thresholds
- **THEN** readiness SHALL become degraded and report redacted totals while operator diagnostics retain owner/generation/evidence detail
- **AND** when hard thresholds are crossed, sidecar evidence is invalid, or owner death cannot be proven, new scheduled attachment writes and destructive cleanup SHALL be blocked while metadata discovery and verified local reads remain available

### Requirement: Existing Filings Are Adopted Without Unnecessary Copying
The migration SHALL recognize valid existing annual-report files and MAY retain an existing verified path or create a verified hard link during cutover before converging on the canonical layout.

#### Scenario: Default legacy inventory roots are loaded
- **WHEN** the version 1 inventory is created without an explicit root override
- **THEN** the required base roots SHALL be exactly `data/filings/business_profile` and `data/filings/financial_statements/broker_risk_control`
- **AND** their versioned default path templates SHALL be `business_profile/{fiscal_year}/{exchange}/` and `broker_risk_control/{exchange}/{symbol}/`, with `symbol` mapped to the canonical instrument identity by the inventory policy
- **AND** captured business-profile fiscal-year/exchange segments SHALL match the normalized report period and canonical exchange
- **AND** captured broker exchange/symbol segments SHALL match canonical exchange/instrument symbol
- **AND** any directory, filename, manifest, or normalized-identity mismatch SHALL fail closed rather than become adoptable
- **AND** the base-root registry, path-template version, and exclusion policy SHALL be persisted in the inventory fingerprint
- **AND** the business-profile `derived/` subtree, broker semiannual files, unrelated fiscal years, and other document families SHALL remain excluded from annual-report adoption
- **AND** an explicit root override SHALL be versioned
- **AND** that explicit root override SHALL cover both required roots before production adoption or cleanup is allowed
- **AND** unknown directories below `data/filings` SHALL be reported read-only as out of scope
- **AND** unknown directories below `data/filings` SHALL remain ineligible for recursive adoption or cleanup

#### Scenario: Existing file is on the filings volume
- **WHEN** an existing business-profile or broker file matches its manifest identity, length, PDF signature, and hash
- **THEN** the migration SHALL adopt it without network download

#### Scenario: A broker archive contains a complete correction
- **WHEN** a broker path has an annual period end and is classified as a verifiable complete original or complete corrected annual-report body
- **THEN** migration SHALL consider it adoptable under normal effective-version policy rather than exclude it merely because it is not an original variant
- **AND** semiannual reports, correction notices without a complete body, and derived files SHALL remain excluded from version 1 adoption

#### Scenario: A file is registered in shadow state
- **WHEN** migration adopts bytes before source identity, report period, classification, content, and latest-effective reconciliation have all passed
- **THEN** the record SHALL remain excluded from production effective lookup, bootstrap coverage, and consumer parsing
- **AND** only an explicit asset-adoption promotion gate after conflict-free reconciliation SHALL make it production-visible
- **AND** promotion SHALL NOT require business-profile or broker consumer cutover

#### Scenario: Legacy-path custody cannot be proven
- **WHEN** a shadow file remains writable or removable by a legacy writer/cleaner and shared custody of the exact hash-qualified path cannot be enforced
- **THEN** the asset SHALL NOT be promoted at that path
- **AND** migration SHALL keep it shadow or first create a verified controlled canonical link/copy before production visibility

#### Scenario: Existing duplicate files are found
- **WHEN** identical valid content exists at multiple business-owned paths
- **THEN** migration SHALL preserve all copies until consumer references are switched and reconciliation passes
- **AND** it SHALL then remove redundant files according to the verified deletion plan

#### Scenario: Existing file cannot be hard-linked
- **WHEN** the verified NFS mount does not support a safe hard link or returns a cross-filesystem or unsupported-operation error
- **THEN** migration SHALL either keep the adopted path or use copy, flush, hash verification, and same-filesystem atomic rename
- **AND** it SHALL NOT fall back to an unverified move or duplicate network download

#### Scenario: A human-readable alias or hard link is managed
- **WHEN** an operator-approved alias or hard link is created for a canonical blob
- **THEN** a transactional database retention pin SHALL record alias path, owner/consumer, content hash, creation time, expiry or cutover condition, and lifecycle state
- **AND** database references and pins SHALL be the deletion truth; `st_nlink`, directory scans, or manually maintained reference counts SHALL NOT authorize unlink
- **AND** alias publication and removal SHALL use the same path containment, mount-identity revalidation, hash verification, lease, recovery-manifest, and deletion-audit gates as other managed file mutations

#### Scenario: The filings mount changes after preflight
- **WHEN** a publish, link, copy, move, or unlink reaches its filesystem mutation boundary after earlier mount preflight
- **THEN** the operation SHALL revalidate the configured mount source, approved NFS identity, read/write mode, and permitted root immediately before mutation
- **AND** an unavailable, local-fallback, read-only, or changed mount SHALL fail closed without mutating the fallback path or advancing catalog, backup, or deletion watermarks

#### Scenario: Existing archive has mixed content
- **WHEN** annual reports share directories with semiannual reports, historical periods, derived artifacts, orphan files, or conflicts
- **THEN** cleanup SHALL default to dry-run and use a per-file manifest/hash allowlist
- **AND** no excluded file or directory SHALL be deleted
- **AND** dry-run and execution SHALL preserve every excluded item's path, bytes, hash, modification time, and permissions without touch, chmod, move, link, quarantine, or rewrite side effects

#### Scenario: Migration cleanup runs in dry-run
- **WHEN** migration convergence or cleanup runs with `dry_run=true`
- **THEN** it SHALL emit only a bounded per-file plan and diagnostics without mutating database business state, recovery manifests, files, or catalog, backup, and deletion watermarks
- **AND** it SHALL NOT create or project a durable operation as executed; ordinary non-business logging and returned in-memory diagnostics MAY occur

#### Scenario: Legacy duplicate path is removed
- **WHEN** a verified business-owned duplicate is approved for cleanup after cutover
- **THEN** a versioned `manifest_kind=legacy_path_rollback` entry inside the common recovery manifest SHALL map its prior path and consumer identity to the shared asset and content hash
- **AND** reconstruction from a verified canonical or backup blob SHALL be tested before unlink

### Requirement: Filings Storage Has Capacity And Backup Gates
The archive SHALL enforce configurable free-space thresholds, planned-download preflight, and incremental backup state independently from SQLite database backup.

#### Scenario: Measured V1 capacity baseline is evaluated
- **WHEN** rollout evaluates a timestamped, read-only measurement of `data/filings` and the configured backup target against the latest-only active A-share bootstrap estimate
- **THEN** the measurement SHALL persist mount identity, total/used/free bytes, active-universe count, attachment size P95/P99/max, estimation assumptions, primary and backup required-set actual bytes, permanently retained recovery-manifest bytes, expected annual growth, temporary and old-plus-new peak bytes, planning horizon/headroom, backup-target capacity, configuration fingerprint, and measurement time
- **AND** preflight MAY report the current estimate as supportable only when runtime warning, hard-reserve, temporary-byte, backup-target, mount-identity, required-blob, and concurrent-reservation checks also pass
- **AND** a historical, expired, or configuration-mismatched measurement SHALL not authorize a later download, scheduler enablement, or deletion

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
- **THEN** the source set SHALL be enumerated from catalog-required blobs rather than only the canonical directory, including promoted adopted blobs whose controlled current path remains under a legacy business directory, every correction-predecessor, withdrawal-tombstone, or legacy-duplicate blob in the immutable version 1 recovery manifest, and every predecessor retained by a `planned|deleting` deletion intent before its manifest row exists
- **AND** each missing blob SHALL be copied through a target-side temporary file, flushed, length/hash verified, and atomically published before it is eligible for the backup watermark
- **AND** an existing hash-named target SHALL be reverified rather than trusted by path alone
- **AND** a present target with mismatched length or hash SHALL remain unprotected
- **AND** the paired backup watermark SHALL remain unchanged for that mismatched target
- **AND** the mismatched target SHALL keep its path, bytes, and modification time unchanged during an ordinary backup run
- **AND** quarantine or replacement SHALL occur only inside an operator-authorized, auditable repair operation that preserves original path/hash evidence
- **AND** backup state SHALL record independent failure-domain identity, mount source, size/hash verification, a file manifest watermark paired with a recoverable catalog database snapshot, completion time, and errors
- **AND** post-snapshot catalog/outbox/operation/lineage/audit increments required by the declared RPO SHALL be copied to an append-only recovery journal in the independent backup failure domain with ordered ids, integrity hashes, predecessor/coverage watermarks, and truncation detection

#### Scenario: Backup target shares the primary storage host
- **WHEN** the configured destination resolves to the same server or storage failure domain as the primary filings mount
- **THEN** the copy SHALL be reported as non-independent
- **AND** the copy SHALL remain ineligible for physical-deletion readiness

#### Scenario: Backup failure-domain identity cannot be verified
- **WHEN** independence is inferred only from a path, host alias, or operator label, or runtime mount/server/export/filesystem evidence conflicts with the configured failure-domain identity
- **THEN** the target SHALL be treated as non-independent
- **AND** the target SHALL remain ineligible for physical-deletion readiness
- **AND** readiness SHALL expose the bounded identity evidence and verification failure without leaking it to unauthorized clients

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
- **THEN** the catalog database snapshot and paired file-manifest watermarks SHALL be compatible
- **AND** restored blobs SHALL pass size/hash verification
- **AND** all current-effective, retention-pinned, pending-deletion replacement and predecessor blobs, plus every version 1 correction-predecessor, withdrawal-tombstone, or legacy-duplicate recovery blob, SHALL be reconciled rather than sampled before enablement
- **AND** recovery-only predecessor and legacy-duplicate bytes SHALL remain in the backup required set or an isolated non-consumer-visible restore area and SHALL NOT be republished as a second primary canonical attachment
- **AND** effective-version decisions, replacement/deletion edges, outbox events, recovery-manifest entries, and consumer lineage/current-result bindings SHALL reconcile with the restored catalog; any dangling or contradictory reference SHALL keep reads, writes, and cleanup blocked
- **AND** the restore SHALL verify and replay the complete append-only recovery-journal interval through its coverage watermark, or prove from a persisted write-freeze watermark that no post-snapshot increment exists; an older snapshot alone SHALL NOT prove absence
- **AND** a missing middle increment, out-of-order increment, tail truncation, or payload/integrity-hash tampering SHALL keep reads, writes, consumer startup, and destructive cleanup blocked
- **AND** derived retention-pin projections and readiness SHALL be rebuilt from authoritative rows, compared with the restored snapshot where applicable, and persisted before enablement
- **AND** consumers and destructive maintenance SHALL remain disabled until reconciliation completes without a required-blob gap or derived-projection mismatch

#### Scenario: Post-cleanup legacy rollback is executed
- **WHEN** rollback occurs after a legacy duplicate or correction predecessor path has been removed
- **THEN** the operator SHALL validate the paired application version, catalog snapshot, and attachment/file-manifest watermark in an isolated temporary root without overwriting the live catalog
- **AND** each required path SHALL be rebuilt in a temporary root from its corresponding immutable recovery entry (`manifest_kind=legacy_path_rollback` for a legacy alias and the correction-predecessor kind for superseded source bytes) and a verified canonical or backup blob, then validated through the legacy consumer before publication
- **AND** legacy consumers SHALL remain stopped on any application/catalog/blob/path mismatch
- **AND** consumer rollback SHALL preserve shared catalog and audit records
- **AND** only a true data-loss recovery MAY overwrite the catalog through paired restore, after freezing writes, declaring snapshot RPO, and replaying or proving absence of all post-snapshot outbox, operation, lineage, and audit increments

#### Scenario: Recovery-manifest retention is evaluated
- **WHEN** backup or restore enumerates version 1 correction-predecessor, withdrawal-tombstone, or legacy-duplicate recovery-manifest entries after primary cleanup
- **THEN** every approved manifest entry SHALL remain immutable and active indefinitely
- **AND** its predecessor bytes SHALL remain in the required recovery set
- **AND** no automatic retirement or garbage collection SHALL occur without a later separately approved retention specification
