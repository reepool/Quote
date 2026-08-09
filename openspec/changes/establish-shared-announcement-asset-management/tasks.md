## 1. Contracts And Configuration

- [ ] 1.1 Define business-neutral announcement, attachment, blob, effective-annual-report, operation, deletion-audit, and consumer-processing models with stable schema versions.
- [ ] 1.2 Add an independently enabled annual-report asset configuration section and config-template defaults covering archive root, providers, classifier policy, bootstrap scope, daily overlap, reconciliation, request limits, storage gates, leases, retries, backup target, and all jobs default-disabled/manual-only as specified.
- [ ] 1.3 Validate that configured archive, temporary, quarantine, and backup paths cannot escape their permitted roots or silently resolve a missing NAS mount onto local storage.
- [ ] 1.4 Add clean-database schema creation and migration tests without changing existing business-profile or financial-fact table contracts.
- [ ] 1.5 Add import/startup tests proving service, API, and scheduler initialization perform zero market scans, downloads, migration moves, or deletions.

## 2. Canonical Repository And Classification

- [ ] 2.1 Implement repositories for source-qualified announcements, attachments, content blobs, immutable attachment observations, effective annual reports, durable operations, replayable change events, deletion audit, and optional consumer-processing links, preserving first/last observed and raw metadata identity.
- [ ] 2.2 Implement deterministic idempotent upsert, unique-key, retention-pin, lease, per-source item cursor, range `covered_until`, and operation-transition behavior with concurrency tests.
- [ ] 2.3 Implement the versioned attachment-level formal annual-report classifier for originals, complete corrections, summaries, translations, visual editions, audit/inquiry/briefing material, periodic reports outside V1, notices, and unresolvable identities.
- [ ] 2.4 Implement fiscal-year/report-period extraction, a versioned `as_of`/listing/disclosure-calendar/provider-coverage search-bound policy, and deterministic latest-effective winner selection, including multiple corrections, withdrawal/silent-update observations, cross-source legal evidence, provisional state, overdue coverage gaps, and fail-closed ambiguity diagnostics.

## 3. File Acquisition And Lifecycle

- [ ] 3.1 Build the shared service on existing `research.announcements` discovery and governed attachment retrieval contracts without adding provider-specific transport logic.
- [ ] 3.2 Implement attachment-scoped single-flight leases, bounded retries, `.part` writes, PDF signature and size checks, SHA-256 verification, flush, and atomic publication.
- [ ] 3.3 Implement local integrity levels, corruption/missing-file handling, safe quarantine state, and bounded reacquisition.
- [ ] 3.4 Implement correction activation plus a durable `planned -> deleting -> deleted|failed` deletion-intent state machine, consumer invalidation/change events, retention pins, and append-only audit while preserving the old effective file on any failed replacement precondition.
- [ ] 3.5 Add crash-injection and concurrent-reader tests around database commit, NFS unlink, audit finalization, shared blobs, aliases, and expired deletion leases.
- [ ] 3.6 Implement storage warning/stop/reserve, atomic concurrent byte reservations, unknown-size budgeting, and annual-report stream limits so metadata discovery continues when scheduled attachment acquisition is blocked.
- [ ] 3.7 Serialize effective-version activation at `instrument + fiscal_year`, reselect the winner transactionally, and test reverse completion of concurrent corrections, stale compare-and-swap, withdrawal races, and deletion-intent safety.

## 4. Existing Archive Adoption

- [ ] 4.1 Build a read-only inventory tool for business-profile and broker annual-report manifests and files that reports valid, missing, corrupt, duplicate, conflicting, superseded, and adoptable entries.
- [ ] 4.2 Register valid existing annual-report files in shadow state using source identity, report period, PDF signature, length, and SHA-256 without redownloading or moving them.
- [ ] 4.3 Reconcile latest-effective decisions against the existing `AnnualReportAssetCatalog`, business-profile manifests, and broker source manifests, and require explicit conflict resolution before cutover.
- [ ] 4.4 Add a separately gated convergence tool whose default dry-run emits a per-file manifest/hash allowlist, persists a versioned `legacy_path -> content_hash/shared_asset/consumer` rollback manifest for approved cleanup, and excludes derived files, semiannual reports, other fiscal years, orphans, and conflicts.
- [ ] 4.5 Probe NFS mount identity and link/rename behavior, then create verified links or copies only after shared reads, reference checks, independent-failure-domain backup checks, and reconciliation succeed.
- [ ] 4.6 Prove the inventory-only mode performs zero network, move, link, quarantine, or delete operations and that latest-only bootstrap never deletes valid older-fiscal-year files merely because they are outside its target.
- [ ] 4.7 Adopt verifiable older-fiscal-year local reports for zero-network period-specific reuse without counting them toward latest-only coverage or triggering adjacent historical network backfill, applying normal correction governance within each adopted period.

## 5. Latest-Only Historical Bootstrap

- [ ] 5.1 Implement a versioned A-share eligibility policy and snapshot the current active SSE, SZSE, and BSE universe with listing metadata, master-data version, policy version, and durable per-instrument coverage state; include main-board/STAR/ChiNext/BSE, ST, and suspended-active stocks while excluding B shares and non-stock security types.
- [ ] 5.2 Implement bounded market-wide annual-category discovery over current and prior filing seasons with adaptive date-window partitioning and durable progress.
- [ ] 5.3 Select and acquire only each instrument's latest available fiscal-year winner, preferring the newest verified complete correction and reusing adopted files.
- [ ] 5.4 Implement rotating targeted repair for uncovered instruments with fixed `as_of`, deterministic candidate/due/earliest fiscal-year bounds, listing and provider-coverage evidence, expiring confirmed-missing evidence, overdue-but-older-asset-usable state, retry states, and restart-safe checkpoints.
- [ ] 5.5 Add bootstrap reports and tests proving latest-only physical acquisition, audit-only retention of non-winning metadata, attachment-level correction precedence, cross-source conflict blocking, January and April-30 boundary behavior, post-period listings, delayed filings, zero redownload on resume, and `success` only when no incomplete/retryable/blocked coverage remains.

## 6. Independent Daily Update

- [ ] 6.1 Add independent `annual_report_asset_latest_backfill` and `annual_report_asset_daily_update` scheduler jobs with no business-profile or broker module dependency.
- [ ] 6.2 Implement cursor-driven market discovery keyed by source/exchange/category/scope/config fingerprint, separate provider item cursors and range `covered_until`, a default three-calendar-day overlap, fixed project-timezone cutoff, explicit boundary semantics, complete-window coverage commits including success-empty windows, and adaptive partitioning.
- [ ] 6.3 Preserve discovered metadata across attachment failures and complete dense single days through durable page/subscope continuation under a fixed cutoff; retain prior range coverage only for incomplete discovery, keep primary/fallback source gaps independent under route policy, and expose an explicit blocker when no safe completion path exists.
- [ ] 6.4 Proactively download both newly effective originals and corrections during the V1 daily job, execute governed period-scoped correction/withdrawal handling, and run separate bounded cohorts for missing coverage, long-publication-window reconciliation, and oldest-first managed-period reconciliation with persisted fairness/retry state.
- [ ] 6.5 Refresh and audit the active-universe snapshot so new listings enter repair, delistings leave the coverage denominator without deleting assets, and older-fiscal-year corrections stay period-scoped.
- [ ] 6.6 Emit stage logs, metrics, durable job results, affected-asset events, and tests for 1,500-record dense days, consecutive success-empty windows, equal/future timestamps, seven-day-late and years-late managed-period corrections, primary/fallback partial coverage, cursor/config fingerprints, bootstrap-to-daily no-gap handoff, non-annual metadata with zero V1 prefetch, and scheduled/manual/API overlap.
- [ ] 6.7 Separate discovery-window completion from attachment retry so a fully discovered window may advance its cursor while failed attachments remain durably queued.
- [ ] 6.8 Add an authenticated durable scheduler control plane for bounded manual start, status, cooperative stop, resume, duplicate-scope operation reuse, restart recovery, actor audit, and retained recent-run history; prove disabled cron does not disable local reads or on-demand ensure.

## 7. Local-First Consumer Service

- [ ] 7.1 Implement lookup and `ensure_annual_report` for instrument/fiscal-year and exact source-filing requests with explicit network, wait/queue, integrity, and version 1 retention policies.
- [ ] 7.2 Return verified local hits with zero provider calls, acquire eligible metadata-only records, run bounded instrument discovery for absent metadata, fail closed for ambiguous or blocked requests, and return metadata-only unavailable without redownloading a deleted superseded filing.
- [ ] 7.3 Implement durable asynchronous acquisition operations with one active operation per normalized scope/policy, separate status/stage/outcome/disposition enums, authorization/rate bounds, polling, cancellation/expiry policy, restart recovery, and actionable diagnostics.
- [ ] 7.4 Publish durable monotonic effective-asset change events or watermarks with consumer checkpoints and idempotent replay so offline consumers process only added, replaced, repaired, withdrawn, or deleted annual-report assets.

## 8. Business Consumer Migration

- [ ] 8.1 Change broker annual-report acquisition to request shared assets while preserving listed-broker scope, embedded-table parser, supplementary report path, financial facts, and parser manifests.
- [ ] 8.2 Prove broker parsing produces equivalent facts and performs zero provider requests and zero duplicate archive writes when a shared asset is present.
- [ ] 8.3 Change business-profile acquisition and exact-filing reuse to request shared assets while preserving page artifacts, semantic extraction, review, promotion, and knowledge-cutoff behavior, including explicit metadata-only historical results and zero redownload when predecessor bytes were deleted.
- [ ] 8.4 Convert `AnnualReportAssetCatalog` and existing DataManager methods into read-through compatibility adapters, then disable legacy business-owned annual-report writes after dual-read reconciliation.
- [ ] 8.5 Link all affected business outputs to shared asset id, source filing, report period, content hash, and effective-correction state without coupling shared validity to parser status; persist consumer-specific continuations so a front-facing business command queues exactly one consumer operation after asset readiness and exposes its state separately.

## 9. DataManager API And Frontend Integration

- [ ] 9.1 Add DataManager list, get, ensure, readiness, operation-status, and controlled-stream methods backed only by the shared asset service.
- [ ] 9.2 Fix additive FastAPI/OpenAPI contracts for zero-network GET, single-scope ensure, operation polling, readiness, and asset-id content streaming with a versioned error envelope and deterministic HTTP 200/202/401/403/404/409/410/422/429/503 mappings, including the configured 404 non-disclosure policy.
- [ ] 9.3 Implement a real trusted identity/permission boundary for acquire, content, operation ownership, and operator readiness; keep mutation/content endpoints disabled when that boundary is not configured.
- [ ] 9.4 Separate asset availability, provisional/final effective-decision state, ensure disposition, operation status, operation stage, batch outcome, result origin, and consumer-processing states, and expose optional shared lineage in business-profile and broker responses without local paths.
- [ ] 9.5 Integrate status, explicit acquire action, idempotent polling, safe content access, and correction-stale behavior into front-facing contracts; register the external UI repository/owner/version as an enablement gate because this repository has no UI source.
- [ ] 9.6 Add OpenAPI snapshot and API/consumer tests proving every GET is zero-network; selector forms are mutually exclusive, complete, identity-consistent, and path/URL-free; same-key/same-fingerprint POST reuses one operation while same-key/different-fingerprint returns 409 per principal; restart recovery and owner isolation work; responses leak no path; streams reject superseded/corrupt assets; knowledge cutoffs hold; and legacy fields remain compatible.

## 10. Backup Observability And Operations

- [ ] 10.1 Implement incremental content-addressed archive backup to a verified independent storage failure domain with missing-only copy, mandatory revalidation of existing hash-named targets, atomic temporary-file publication, size/hash validation, mount-source checks, target-side warning/hard-reserve/planned-byte gates, crash recovery before/after publication and watermark commit, a file manifest watermark paired with a recoverable catalog database snapshot, freshness state, unprotected bytes, and explicit mount/capacity failure handling; version 1 performs no automatic backup-blob GC.
- [ ] 10.2 Add persisted readiness and operational reports for active-universe coverage, discovery gaps, attachment readiness, integrity, retries, storage, unprotected bytes, backup freshness, scheduler state, and consumer migration, including recent-run retention, last successful cutoff, stale heartbeat, consecutive failures, cursor lag, oldest backlog age, alert thresholds, and separate redacted frontend versus operator diagnostics.
- [ ] 10.3 Add `annual_report_asset_integrity_audit` and `annual_report_asset_backup` jobs plus repair commands that default to read-only and require explicit bounded flags for network repair, quarantine, linking, moving, or deletion.
- [ ] 10.4 Implement and document backup/restore ordering for database plus files and legacy-path rollback-manifest reconstruction; require full presence/length/hash reconciliation of every current-effective, retention-pinned, and pending-deletion replacement blob before re-enabling reads, writes, or predecessor deletion, while keeping sampled restore drills as additional routine evidence.

## 11. Validation Rollout And Cleanup

- [ ] 11.1 Run all repository, classifier, lifecycle, backfill, daily, API, scheduler, storage, backup, and consumer unit tests on temporary databases and archive roots.
- [ ] 11.2 Run bounded live provider probes for SSE, SZSE, and BSE originals and corrections, recording classification, pagination, cursor, attachment, and rate-limit evidence without broad production writes.
- [ ] 11.3 Run production archive inventory and shadow adoption, then reconcile database rows, file hashes, active-universe coverage, duplicate bytes, calibrate annual-report size limits from measured P95/P99/max, and verify estimated/free storage before any download or deletion.
- [ ] 11.4 Enable shared reads for broker and business-profile behind migration gates, process affected assets, and require zero duplicate network/archive activity plus compatible business outputs.
- [ ] 11.5 Verify canonical archive backup and restore, including a required blob outside a sampling cohort that must still block enablement; complete the latest-only bootstrap; and prove daily cron enablement fails closed until configured coverage, integrity, storage, backup, and migration gates pass, then starts from a compatible handoff watermark without a discovery gap.
- [ ] 11.6 Disable legacy annual-report writes and remove redundant files/code only after the deletion plan is reviewed, all retention pins are released, backups are verified, a temporary-root drill reconstructs legacy paths from the rollback manifest, and legacy consumer reads pass against those reconstructed paths.
