## Context

The repository already has a source-neutral `research.announcements` layer for CNInfo and exchange discovery, provider routing, normalized announcement/attachment records, conservative checkpoints, and governed byte retrieval. It intentionally stops before archive ownership. Business-profile consequently owns an immutable annual-report archive and `AnnualReportAssetCatalog`, while broker risk-control independently downloads the same formal reports, writes a different path, and mixes download state with parser state in `financial_source_files`.

Production data proves the ownership split creates duplicate files. Business-profile is also still under active rollout, so it cannot be the upstream producer on which unrelated consumers depend. The remounted `data/filings` volume has sufficient near-term capacity, but current database backup covers `data/*.db` rather than attachment files. The new capability must therefore centralize source assets, preserve business-specific parsing, adopt current files, and add independent scheduling and file operations.

Version 1 follows the user's latest-only retention policy: for an instrument and fiscal year, only the most effective annual-report attachment remains physically retained after a verified complete correction replaces the original. Metadata, hashes, replacement lineage, processing history, and deletion audit remain. Reconstructing a pre-correction historical PDF from local storage is explicitly outside version 1.

Stakeholders include scheduler operations, research storage, business-profile, broker risk-control, DataManager/FastAPI clients, future announcement consumers, backup operations, and research users who require reliable source lineage.

## Goals / Non-Goals

**Goals:**

- Establish business-neutral announcement and attachment asset ownership.
- Bootstrap one latest effective annual report for every active A-share stock.
- Run independent daily discovery and attachment maintenance after bootstrap.
- Prefer the latest verified complete correction and retain one physical effective annual report per instrument/fiscal year.
- Provide local-first lookup and bounded ensure/download for every consumer.
- Reuse valid files already under business-profile and broker archive roots.
- Separate source-asset validity from each consumer's parser and fact status.
- Integrate through DataManager, FastAPI, scheduler reporting, and consumer lineage.
- Add storage capacity, backup, integrity, retry, and deletion governance.
- Keep the model extensible to semiannual and other announcement types.

**Non-Goals:**

- Do not download every attachment for every announcement type in version 1.
- Do not move business-profile section selection, PDF extraction, LLM semantics, broker table parsing, or fact promotion into the asset service.
- Do not replace structured financial JSON/XBRL sources with annual-report PDFs.
- Do not preserve all superseded annual-report physical files for historical point-in-time reconstruction.
- Do not treat title similarity or identical content as sufficient to merge legal filing identities across sources.
- Do not require a Web frontend implementation where the repository has no frontend source; define stable API contracts and UI-visible states instead.
- Do not perform an unbounded full-market scan, uncontrolled parallel download, or destructive migration during startup.

## Decisions

### Build an asset layer on top of `research.announcements`

Add a package such as `research/announcement_assets` with repository, service, annual-report policy, migration, and operation components. It consumes normalized announcement records and `AnnouncementAttachmentRetriever`; it does not implement provider HTTP logic.

```text
provider discovery / normalized records / safe byte retrieval
                           |
                           v
announcement asset repository and service
  metadata -> attachment observation -> blob -> effective annual report
                           |
                           v
consumer processing manifests and derived facts
```

Alternative: expand `BusinessProfileDocumentArchiveService` and let other consumers call it. Rejected because ownership and configuration would still depend on a non-production-ready business domain.

Alternative: put persistence directly into `AnnouncementAcquisitionService`. Rejected because metadata discovery and binary asset lifecycle have different retention, storage, concurrency, and failure semantics.

### Use separate canonical tables for metadata, blobs, policy, and processing links

The target model uses business-neutral structures conceptually equivalent to:

- `official_announcements`: source-qualified announcement identity, normalized/raw publication time, title, symbols, raw payload hash, first/last observed time, and current metadata.
- `official_announcement_attachments`: source-qualified attachment identity, URL metadata, media hints, and relationship to announcement.
- `official_document_blobs`: SHA-256, content length, media/signature state, canonical path, integrity state, first downloaded time, and verified backup state.
- `official_attachment_versions`: attachment observation plus content hash, final URL, retrieval evidence, validity, and download attempt lineage.
- `effective_annual_reports`: instrument, fiscal year, report period, selected attachment version, correction flag, classification version, effective state, and replacement link.
- `official_asset_operations`: durable discovery/download/backfill/on-demand operation state and leases.
- `official_asset_deletion_audit`: append-only physical deletion decisions.
- `official_asset_consumer_processing`: optional shared index that links consumer/parser identities to source assets while consumer-owned manifests remain authoritative for domain outputs.

`announcement_audit` remains purpose-specific operational evidence and is not promoted into the canonical asset store. `financial_source_files` remains compatible for financial parser lineage but no longer owns physical annual-report identity.

Alternative: keep adding schema versions to `financial_source_files`. Rejected because the row identity and mutable status currently mix source files with parser purpose, preventing one source asset from supporting independent consumers cleanly.

### Keep four identities distinct

- Legal announcement: `source + source_announcement_id`.
- Attachment: `announcement_key + attachment_id`, or a deterministic normalized source-URL key when the provider lacks an attachment id.
- Physical content: SHA-256.
- Consumer processing: `asset_id + consumer + parser_version + parameter_hash`.

Identical bytes can share storage without merging legal identities. Cross-source mirrors require explicit evidence or policy if a later capability wants a logical relationship.

### Centralize a strict formal annual-report classifier

Version 1 accepts complete primary Chinese annual-report PDFs and complete corrected/revised annual-report PDFs. It excludes summaries, translations, visual editions, audit reports, inquiry material, correction notices without a complete replacement, and related announcements. Classification is attachment-level because one announcement can contain the full report, summary, translation, appendix, and notice together.

The classifier produces `document_family=annual_report`, fiscal year/report period, `variant=original|correction`, `is_full_report`, reasons, and policy version. Provider categories narrow discovery traffic but do not replace local fail-closed title/attachment classification.

Alternative: reuse business-profile classification through an import. Rejected because the shared service must own the common periodic-report subset; business-profile can layer specialist classifications downstream.

### Converge to one effective asset and delete predecessors safely

Winner order for one instrument/fiscal year is:

1. valid complete correction over valid original;
2. latest normalized publication time among complete corrections;
3. stable filing/attachment identity only inside a proven equivalent legal chain.

Cross-source candidates can converge automatically only when their content hash matches or governed mirror/legal-precedence evidence exists. Different verified bytes with no precedence evidence are ambiguous, never resolved by lexical source or filing-id order. Withdrawal, cancellation, or a changed attachment under the same announcement id creates a new observation and triggers reevaluation.

Replacement is a staged transaction:

1. discover and register correction metadata;
2. acquire the correction under an attachment lease;
3. validate source identity, PDF signature, byte length, SHA-256, and safe path;
4. atomically activate the correction, persist a `planned` deletion intent, and publish a source-change event;
5. mark prior consumer processing stale/superseded or enqueue reprocessing;
6. remove the predecessor asset reference;
7. delete the predecessor blob only when all physical retention pins are released and independent-failure-domain replacement backup plus a paired catalog recovery watermark are verified;
8. transition the deletion intent through `deleting` to `deleted|failed` and let an idempotent reconciler recover crashes.

The old file remains effective if replacement validation or activation fails. After activation, a failed unlink leaves a truthful pending/failed cleanup state and never rolls back the valid replacement. Metadata and lineage are never deleted. Physical retention pins include active assets, managed legacy aliases, not-yet-migrated consumers, and active read/processing leases; historical metadata references do not permanently pin bytes.

Alternative: retain every physical revision. Rejected because the user explicitly requires one most-effective attachment per instrument/fiscal year. The trade-off is that version 1 cannot locally replay a pre-correction PDF.

Alternative: delete original as soon as correction metadata appears. Rejected because an incomplete or corrupt replacement would leave no usable report.

### Make local-first ensure the only consumer acquisition path

`ensure_annual_report()` accepts instrument/fiscal year or exact source filing identity, an `allow_network` flag, wait/queue policy, and integrity level.

```text
verified local effective asset -> return local hit
metadata only                  -> acquire attachment
no matching metadata          -> bounded instrument discovery -> acquire
ambiguous candidates           -> fail closed
storage/network disabled       -> explicit blocked/missing
```

Internal batch consumers may wait within configured bounds. API calls create or reuse an asynchronous durable operation and return an operation id rather than block indefinitely.

### Use latest-only full-market bootstrap with targeted completeness repair

The default universe is active instruments with `type=stock` on SSE, SZSE, and BSE. Inactive/delisted instruments remain available through explicit ensure but are not part of the default bootstrap.

Bootstrap phases:

1. snapshot the target universe and listing metadata;
2. adopt and classify existing valid local annual-report files;
3. scan annual-report categories market-wide through bounded date partitions covering current and prior filing seasons;
4. select each instrument's latest available fiscal year and correction winner;
5. acquire winners only;
6. create a rotating targeted repair queue for uncovered instruments;
7. search each missing instrument backwards through bounded annual-category windows until a valid latest report is found or listing/coverage bounds prove empty;
8. finish successfully only when every target is `available` or unexpired `confirmed_missing`; any incomplete/retryable/blocked scope makes the run partial.

Market-wide discovery prevents a normal one-request-per-stock strategy. Targeted scanning is reserved for the shrinking missing cohort. The run fixes `as_of`, source/query fingerprint, expected fiscal-year and listing/search bounds. It cannot select an older fiscal year as latest while the newer window is incomplete, and a discovered but unverifiable latest correction blocks final coverage or leaves an already-serving predecessor explicitly provisional.

### Use cursor-driven daily windows with adaptive date partitioning

Daily state is keyed by source, exchange, normalized `annual_report` category, and market scope. The normal start is committed maximum publication time minus a configurable overlap, defaulting to three calendar days. The end is the run cutoff.

If a window exceeds provider bounds, selected metadata is retained and the interval is bisected. A single dense day continues through durable page ranges or provider-supported subscopes under the fixed cutoff; its parent completes only when every child completes, otherwise it remains an explicit blocker. Cursor advancement occurs only after the full requested interval completes successfully. Attachment failure does not discard discovered metadata and creates a retryable asset operation. Cursor state is bound to a query/configuration fingerprint.

After market discovery, one bounded rotating cohort repairs instruments missing the expected latest annual report and a separate long-lookback cohort reconciles already-covered instruments for provider-late or backdated corrections. The active-universe snapshot refreshes on a bounded cadence so new listings enter repair and delistings leave the denominator without deleting files. The daily run reports metadata discovery, late-revision reconciliation, and attachment readiness separately.

Alternative: fixed multi-year daily scan. Rejected because it repeats high-volume historical pages.

Alternative: query all instruments daily. Rejected because roughly 5,500 active stocks would create unnecessary identity and negative-result traffic.

### Provide hybrid download policy

The independent daily job downloads effective formal annual-report attachments in version 1, as explicitly required. The architecture still separates metadata and attachment stages so future announcement types can use metadata-only or lazy policies.

On-demand ensure remains a correctness fallback for missed, manually requested, or historical assets. A scheduler run is an optimization and coverage mechanism, not a prerequisite for business use.

### Adopt current files before creating the canonical root

Migration inventories business-profile and broker manifests and validates source identity, instrument, report period, PDF signature, content length, and SHA-256. Valid current winners are registered without network download.

The default target root is a business-neutral content-addressed physical pool:

```text
data/filings/announcements/blobs/
  {content_hash_prefix}/{content_hash}.pdf
```

Instrument, fiscal-year, exchange, source, and filing identities remain database projections. During shadow migration, a canonical record may reference an existing path. A later verified move, hard link, or copy-verify-rename can converge storage without unnecessary network download. NFS mount identity and filesystem capabilities are probed first. Duplicate paths are deleted only through a per-file manifest/hash allowlist after both consumers read the shared asset and reconciliation passes; derived files, semiannual reports, other years, orphans, and conflicts are excluded.

### Treat storage capacity and backup as rollout gates

Current observed PDFs average approximately 4.6 MiB, with P95 approximately 12.9 MiB and a measured maximum approximately 43.8 MiB, projecting roughly 24-25 GiB for one full active A-share annual-report year. The remounted filings NFS has approximately 2.1 TiB free, so capacity is sufficient, but the service must not rely on a static estimate.

Before download, operations atomically reserve planned bytes on the target filesystem using Content-Length or a configured unknown-size budget, accounting for `.part`, quarantine, and old/new overlap. They also check free bytes, warning utilization, hard stop utilization, and absolute reserve. Metadata sync can continue after the attachment hard stop. Attachment size limits are separately configurable for annual reports because the current 50 MiB generic default is close to observed large files.

SQLite online backup does not cover `data/filings`. Add an incremental content-addressed archive backup to a verified independent storage failure domain, copying only missing blobs, validating hashes, and pairing its watermark with a recoverable catalog database snapshot or manifest. The current filings and PVE-Bak mounts share server `192.168.188.88`, so PVE-Bak alone cannot satisfy the deletion disaster-recovery gate; QuoteBak is currently on a different server. Backup freshness and unprotected bytes are readiness inputs. Never fall back to writing a NAS backup into the local data mount when the share is absent.

### Integrate front-facing workflows through DataManager and FastAPI

DataManager exposes shared list/get/ensure/readiness/operation methods. Existing annual-report catalog methods become read-through compatibility surfaces during migration.

Additive API resources expose:

- effective annual-report metadata and integrity for an instrument;
- asset-management readiness and coverage;
- bounded ensure operation creation and status;
- controlled file streaming by asset id.

Responses expose stable asset lineage, not unrestricted local paths. All metadata and existing business-profile GETs remain zero-network. Asset availability, ensure disposition, durable operation state, and consumer-processing state are separate. Business-profile and broker responses retain existing schemas while adding optional shared lineage. No current API must synchronously crawl the market.

The repository currently has rate limiting but no complete authentication middleware. Acquire, content, cancellation, repair, and operator endpoints therefore remain disabled unless a trusted identity and scoped permission boundary is configured. Durable SQLite operations, not FastAPI `BackgroundTasks`, are the source of truth. The repository supplies DataManager/FastAPI/OpenAPI contracts and UI state definitions; the actual external UI repository and owner are an enablement dependency.

### Publish asset-change events or watermarks for consumers

When an effective asset is added, replaced, repaired, or deleted, append a change event or watermark keyed by instrument/fiscal year/asset id. Consumers can process only affected assets. A correction marks old consumer runs stale and drives domain-specific reprocessing; the asset service does not delete or rewrite business facts itself.

### Make operations resumable and single-flight

Durable operations use scopes, checkpoints, leases, attempts, retry times, and bounded errors. A unique active lease prevents scheduler, API, and consumer requests from downloading the same attachment concurrently. Files are written to `.part` on the same verified NFS mount, validated, fsynced where supported, atomically renamed, reopened, and reverified. Lease expiry permits cleanup and retry after process failure.

## Risks / Trade-offs

- [Deleting predecessor PDFs weakens historical reproducibility] -> Preserve complete metadata, hashes, replacement/deletion audit, and explicitly scope version 1 to latest-effective local assets; require a later policy change before point-in-time source replay is claimed.
- [A correction notice is mistaken for a complete replacement] -> Require a complete full-report classification and validated PDF attachment before winner promotion.
- [Provider categories omit a report] -> Combine category-filtered market scans with bounded targeted repair of missing expected instruments.
- [A dense filing date exceeds page bounds] -> Persist completed metadata, continue stable page ranges/subscopes under a fixed cutoff, and advance the parent cursor only after all children complete; otherwise expose an unsplittable-day blocker.
- [Late or backdated corrections are missed] -> Add a rotating long-lookback reconciliation cohort over already-covered assets, separate from the three-day low-latency overlap.
- [Incomplete newest-year evidence is mistaken for an older latest year] -> Fix `as_of` and fiscal-year/search bounds, classify incomplete/blocked separately from confirmed missing, and never downgrade silently.
- [Cross-source or withdrawn corrections select the wrong winner] -> Require attachment-level classification, governed mirror/legal precedence, withdrawal observations, and fail-closed ambiguity/provisional states.
- [Concurrent callers duplicate downloads] -> Use attachment-scoped leases, unique constraints, content-addressed blobs, and atomic writes.
- [Migration deletes a valid existing file] -> Adopt read-only first, reconcile manifests/hash/consumers, produce a per-file allowlist, switch reads, verify independent-failure-domain backup, and only then delete explicitly managed duplicates; never clean mixed directories.
- [Storage fills during filing season] -> Estimate planned bytes, enforce warning/stop thresholds and absolute reserve, stop attachment prefetch while continuing metadata.
- [NAS is unavailable] -> Keep local service operational, fail backup explicitly, expose readiness degradation, and prevent unsafe local fallback. Same-server exports do not count as independent disaster recovery.
- [SQLite and NFS deletion are not one transaction] -> Persist deletion intent before unlink, finalize `deleted|failed` afterward, and run an idempotent reconciler with crash-injection tests.
- [Business parser status corrupts shared asset status] -> Separate asset and consumer-processing stores.
- [API-triggered acquisition causes unbounded work] -> Require authorization, bounded single-instrument scopes, durable asynchronous operations, rate limits, and no caller-supplied paths.
- [Active business-profile changes overlap migration files] -> Land shared contracts/storage/service first, use adapters, and migrate business-profile in a bounded later step after rebasing current work.

## Migration Plan

1. Add canonical asset tables, models, repository, configuration, and clean-database tests without changing current consumers.
2. Add inventory/audit tooling that reads existing business-profile and broker manifests and files, reports valid/adoptable/duplicate/conflicting/missing assets, and performs no deletion by default.
3. Register existing valid files in shadow state, establish latest-effective annual-report decisions, and compare current catalog/broker identities and hashes.
4. Implement local-first ensure, atomic archive writing, leases, correction convergence, deletion audit, storage gates, and backup state with offline tests.
5. Implement the latest-only active-universe backfill in a temporary database/archive, validate coverage and restart behavior, then run production adoption before network acquisition.
6. Add the independent daily job and bounded live metadata/attachment probes. Keep consumer migration gates disabled initially.
7. Migrate broker annual-report acquisition to shared assets and verify identical facts with zero duplicate download/archive writes.
8. Migrate business-profile annual-report acquisition and its existing catalog APIs to shared assets while leaving parsing and semantic workflows unchanged.
9. Add DataManager and FastAPI surfaces, consumer lineage, operation status, and front-facing integration tests.
10. Run dual-read reconciliation, asset backup verification, storage readiness, and affected consumer regression suites.
11. Stop legacy annual-report writes, remove duplicate business-owned files and code only after verified cutover, and enforce repository residue checks.
12. Enable daily scheduling after bootstrap reaches the configured coverage gate.

Rollback before legacy-write removal disables shared consumer routing and daily scheduling while leaving additive records and adopted files intact. Rollback after physical cleanup restores the verified archive backup plus the preceding database backup and application version; code rollback alone is insufficient after predecessor or duplicate file deletion.

## Open Questions

- Version 1 assumes the scheduled/bootstrap universe is current active SSE/SZSE/BSE stocks; inactive and delisted instruments are on-demand only.
- Version 1 intentionally deletes superseded physical annual-report PDFs after safe replacement. A later requirement is needed if point-in-time reconstruction of source documents becomes mandatory.
- Semiannual reports reuse the architecture but are not enabled in the first scheduled rollout.
