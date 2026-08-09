## 1. Contracts And Configuration

- [ ] 1.1 Define business-neutral announcement, attachment, blob, effective-annual-report, operation, deletion-audit, and consumer-processing models with stable schema versions.
- [ ] 1.2 Add an independently enabled annual-report asset configuration section covering archive root, providers, classifier policy, bootstrap scope, daily overlap, request limits, storage gates, leases, retries, and backup target.
- [ ] 1.3 Validate that configured archive, temporary, quarantine, and backup paths cannot escape their permitted roots or silently resolve a missing NAS mount onto local storage.
- [ ] 1.4 Add clean-database schema creation and migration tests without changing existing business-profile or financial-fact table contracts.

## 2. Canonical Repository And Classification

- [ ] 2.1 Implement repositories for source-qualified announcements, attachments, content blobs, attachment observations, effective annual reports, durable operations, change events, deletion audit, and optional consumer-processing links.
- [ ] 2.2 Implement deterministic idempotent upsert, unique-key, retention-pin, lease, cursor, and operation-transition behavior with concurrency tests.
- [ ] 2.3 Implement the versioned formal annual-report classifier for originals, complete corrections, summaries, translations, visual editions, notices, and unrelated announcements.
- [ ] 2.4 Implement fiscal-year/report-period extraction and deterministic latest-effective winner selection, including multiple corrections and fail-closed ambiguity diagnostics.

## 3. File Acquisition And Lifecycle

- [ ] 3.1 Build the shared service on existing `research.announcements` discovery and governed attachment retrieval contracts without adding provider-specific transport logic.
- [ ] 3.2 Implement attachment-scoped single-flight leases, bounded retries, `.part` writes, PDF signature and size checks, SHA-256 verification, flush, and atomic publication.
- [ ] 3.3 Implement local integrity levels, corruption/missing-file handling, safe quarantine state, and bounded reacquisition.
- [ ] 3.4 Implement correction activation plus a durable `planned -> deleting -> deleted|failed` deletion-intent state machine, consumer invalidation/change events, retention pins, and append-only audit while preserving the old effective file on any failed replacement precondition.
- [ ] 3.5 Add crash-injection and concurrent-reader tests around database commit, NFS unlink, audit finalization, shared blobs, aliases, and expired deletion leases.
- [ ] 3.6 Implement storage warning/stop/reserve, atomic concurrent byte reservations, unknown-size budgeting, and annual-report stream limits so metadata discovery continues when scheduled attachment acquisition is blocked.

## 4. Existing Archive Adoption

- [ ] 4.1 Build a read-only inventory tool for business-profile and broker annual-report manifests and files that reports valid, missing, corrupt, duplicate, conflicting, superseded, and adoptable entries.
- [ ] 4.2 Register valid existing annual-report files in shadow state using source identity, report period, PDF signature, length, and SHA-256 without redownloading or moving them.
- [ ] 4.3 Reconcile latest-effective decisions against the existing `AnnualReportAssetCatalog`, business-profile manifests, and broker source manifests, and require explicit conflict resolution before cutover.
- [ ] 4.4 Add a separately gated convergence tool whose default dry-run emits a per-file manifest/hash allowlist and excludes derived files, semiannual reports, other fiscal years, orphans, and conflicts from cleanup.
- [ ] 4.5 Probe NFS mount identity and link/rename behavior, then create verified links or copies only after shared reads, reference checks, independent-failure-domain backup checks, and reconciliation succeed.

## 5. Latest-Only Historical Bootstrap

- [ ] 5.1 Snapshot the current active SSE, SZSE, and BSE stock universe with listing metadata and durable per-instrument coverage state.
- [ ] 5.2 Implement bounded market-wide annual-category discovery over current and prior filing seasons with adaptive date-window partitioning and durable progress.
- [ ] 5.3 Select and acquire only each instrument's latest available fiscal-year winner, preferring the newest verified complete correction and reusing adopted files.
- [ ] 5.4 Implement rotating targeted repair for uncovered instruments with fixed `as_of`, expected fiscal-year/listing bounds, expiring confirmed-missing evidence, retry states, and restart-safe checkpoints.
- [ ] 5.5 Add bootstrap reports and tests proving latest-only retention, attachment-level correction precedence, cross-source conflict blocking, zero redownload on resume, and `success` only when no incomplete/blocked coverage remains.

## 6. Independent Daily Update

- [ ] 6.1 Add an independent manual latest-backfill scheduler job and an independently enabled daily annual-report update job with no business-profile or broker module dependency.
- [ ] 6.2 Implement cursor-driven market discovery keyed by source/exchange/category/scope, a default three-calendar-day overlap, run cutoff, complete-window cursor commits, and adaptive partitioning.
- [ ] 6.3 Preserve discovered metadata across attachment failures and complete dense single days through durable page/subscope continuation under a fixed cutoff; retain the prior cursor and explicit blocker when no safe completion path exists.
- [ ] 6.4 Download effective formal annual-report attachments during the V1 daily job, execute governed correction/withdrawal handling, and run separate bounded cohorts for missing coverage and long-lookback late-revision reconciliation.
- [ ] 6.5 Refresh and audit the active-universe snapshot so new listings enter repair, delistings leave the coverage denominator without deleting assets, and older-fiscal-year corrections stay period-scoped.
- [ ] 6.6 Emit stage logs, metrics, durable job results, affected-asset events, and tests for 1,500-record dense days, seven-day-late corrections, cursor/config fingerprints, and scheduled/manual/API overlap.

## 7. Local-First Consumer Service

- [ ] 7.1 Implement lookup and `ensure_annual_report` for instrument/fiscal-year and exact source-filing requests with explicit network, wait/queue, and integrity policies.
- [ ] 7.2 Return verified local hits with zero provider calls, acquire metadata-only records, run bounded instrument discovery for absent metadata, and fail closed for ambiguous or blocked requests.
- [ ] 7.3 Implement durable asynchronous acquisition operations with idempotency keys, authorization/rate bounds, status polling, cancellation/expiry policy, and actionable diagnostics.
- [ ] 7.4 Publish effective-asset change events or watermarks so consumers can process only added, replaced, repaired, or deleted annual-report assets.

## 8. Business Consumer Migration

- [ ] 8.1 Change broker annual-report acquisition to request shared assets while preserving listed-broker scope, embedded-table parser, supplementary report path, financial facts, and parser manifests.
- [ ] 8.2 Prove broker parsing produces equivalent facts and performs zero provider requests and zero duplicate archive writes when a shared asset is present.
- [ ] 8.3 Change business-profile acquisition and exact-filing reuse to request shared assets while preserving page artifacts, semantic extraction, review, promotion, and knowledge-cutoff behavior.
- [ ] 8.4 Convert `AnnualReportAssetCatalog` and existing DataManager methods into read-through compatibility adapters, then disable legacy business-owned annual-report writes after dual-read reconciliation.
- [ ] 8.5 Link all affected business outputs to shared asset id, source filing, report period, content hash, and effective-correction state without coupling shared validity to parser status.

## 9. DataManager API And Frontend Integration

- [ ] 9.1 Add DataManager list, get, ensure, readiness, operation-status, and controlled-stream methods backed only by the shared asset service.
- [ ] 9.2 Fix additive FastAPI/OpenAPI contracts for zero-network GET, single-scope ensure, operation polling, readiness, and asset-id content streaming, including HTTP 200/202/400/403/404/409/410/422/429/503 semantics.
- [ ] 9.3 Implement a real trusted identity/permission boundary for acquire, content, operation ownership, and operator readiness; keep mutation/content endpoints disabled when that boundary is not configured.
- [ ] 9.4 Separate asset availability, ensure disposition, durable operation, and consumer-processing states, and expose optional shared lineage in business-profile and broker responses without local paths.
- [ ] 9.5 Integrate status, explicit acquire action, idempotent polling, safe content access, and correction-stale behavior into front-facing contracts; register the external UI repository/owner/version as an enablement gate because this repository has no UI source.
- [ ] 9.6 Add OpenAPI snapshot and API/consumer tests proving every GET is zero-network, repeated/concurrent POST reuses one operation, restart recovery and owner isolation work, responses leak no path, streams reject superseded/corrupt assets, knowledge cutoffs hold, and legacy fields remain compatible.

## 10. Backup Observability And Operations

- [ ] 10.1 Implement incremental content-addressed archive backup to a verified independent storage failure domain with missing-only copy, size/hash validation, mount-source checks, catalog snapshot/manifest watermarks, freshness state, and explicit mount-failure handling.
- [ ] 10.2 Add readiness and operational reports for active-universe coverage, discovery gaps, attachment readiness, integrity, retries, storage, unprotected bytes, backup freshness, scheduler state, and consumer migration.
- [ ] 10.3 Add integrity audit and repair commands that default to read-only and require explicit bounded flags for network repair, quarantine, linking, moving, or deletion.
- [ ] 10.4 Document backup/restore ordering for database plus files and verify a sampled paired-watermark restore preserves asset identities, hashes, effective selection, and consumer lineage before enabling predecessor deletion.

## 11. Validation Rollout And Cleanup

- [ ] 11.1 Run all repository, classifier, lifecycle, backfill, daily, API, scheduler, storage, backup, and consumer unit tests on temporary databases and archive roots.
- [ ] 11.2 Run bounded live provider probes for SSE, SZSE, and BSE originals and corrections, recording classification, pagination, cursor, attachment, and rate-limit evidence without broad production writes.
- [ ] 11.3 Run production archive inventory and shadow adoption, then reconcile database rows, file hashes, active-universe coverage, duplicate bytes, and estimated/free storage before any download or deletion.
- [ ] 11.4 Enable shared reads for broker and business-profile behind migration gates, process affected assets, and require zero duplicate network/archive activity plus compatible business outputs.
- [ ] 11.5 Verify canonical archive backup and restore, enable the latest-only bootstrap, and enable daily scheduling only after configured coverage, integrity, storage, and backup gates pass.
- [ ] 11.6 Disable legacy annual-report writes and remove redundant files/code only after the deletion plan is reviewed, references are zero, backups are verified, and rollback artifacts are recorded.
