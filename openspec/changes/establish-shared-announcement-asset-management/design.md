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
- `official_asset_operation_subscriptions`: principal/consumer-scoped idempotency, authorization, response projection, and continuation links to a globally single-flight asset operation.
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

The same fail-closed rule applies inside one source when publication timestamps have equal normalized value but different verified bytes: only an explicit replacement edge, official revision sequence, or other versioned legal-precedence evidence can choose a winner. Original timestamp values and precision are retained. A withdrawal affects a candidate only when provider status, target announcement/attachment identity, an official relation, or a versioned deterministic rule binds it to that candidate; generic withdrawal wording alone records unresolved evidence and cannot deactivate the current asset.

Attachment acquisition leases do not by themselves serialize the effective-version decision. Activation therefore uses an `instrument + fiscal_year` decision lease or row-version compare-and-swap, and reselects the winner from all committed observations inside the transaction. A worker that downloaded an older correction after a newer correction committed cannot downgrade the effective row or create a deletion intent for the true winner.

Replacement is a staged transaction:

1. discover and register correction metadata;
2. acquire the correction under an attachment lease;
3. validate source identity, PDF signature, byte length, SHA-256, and safe path;
4. atomically activate the correction, persist a `planned` deletion intent, and publish a source-change event;
5. mark prior consumer processing stale/superseded or enqueue reprocessing;
6. remove the predecessor asset reference;
7. delete the predecessor blob only when all physical retention pins are released and independent-failure-domain replacement backup plus a paired catalog recovery watermark are verified;
8. transition the deletion intent through `deleting` to `deleted|failed` and let an idempotent reconciler recover crashes.

The old file remains effective if replacement validation or activation fails. After activation, a failed unlink leaves a truthful pending/failed cleanup state and never rolls back the valid replacement. Metadata and lineage are never deleted. Physical retention pins include active assets, managed legacy aliases, not-yet-migrated consumers, and active read/processing leases; historical metadata references do not permanently pin bytes. Read and processing leases carry owner, TTL, heartbeat, generation, and a safety grace period. An expired lease releases its pin only through compare-and-swap reconciliation after no newer heartbeat is present; stale leases cannot pin bytes forever, but uncertain or live readers remain fail-closed deletion blockers.

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

Internal batch consumers may wait within configured bounds. API calls that need asynchronous acquisition create or reuse a durable operation and return the caller's opaque `asset_request_id` subscription handle; an immediate local hit or explicit network-disabled local miss returns without a subscription rather than blocking indefinitely or exposing the internal operation id.

Exact identity prevents legal-filing substitution, but it does not override version 1 retention. A known superseded or withdrawn predecessor whose bytes were deleted returns its metadata and explicit local-content-unavailable state; ordinary consumer/API ensure cannot redownload it. Restoring historical predecessor bytes would require a separately authorized future operator policy, because allowing routine exact-filing acquisition would defeat the one-effective-attachment rule.

An exact-filing selector may additionally pin an attachment id plus expected content hash or observation version. A pin must match that immutable observation; an unpinned request means the current observation of that legal filing. Every attachment/withdrawal observation carries `version_available_at`, preferring an official effective time and otherwise using `first_observed_at`, so knowledge-cutoff queries cannot see bytes or withdrawal state that appeared after the cutoff merely because the parent announcement was older.

### Use latest-only full-market bootstrap with targeted completeness repair

The default universe is produced by a versioned eligibility policy over active RMB-denominated A-share instruments on SSE, SZSE, and BSE. It includes main-board, STAR Market, ChiNext, BSE, ST, and temporarily suspended-but-not-delisted stocks; it excludes B shares, funds/ETFs, bonds, indices, and other non-A-share security types. The snapshot stores instrument identities, policy/master-data versions, the master-data last-success time, and snapshot time. A failed refresh retains the last complete snapshot rather than replacing it with empty or partial data. Stale snapshots and missing/conflicting eligibility fields become explicit `eligibility_indeterminate` readiness evidence and prevent a claim of complete full-market coverage, while market announcement discovery may continue. Inactive/delisted instruments remain available through explicit ensure but are not part of the default bootstrap.

Bootstrap phases:

1. snapshot the target universe and listing metadata;
2. adopt and classify existing valid local annual-report files;
3. scan annual-report categories market-wide through bounded date partitions covering current and prior filing seasons;
4. select each instrument's latest available fiscal year and correction winner;
5. acquire winners only, retaining non-winning discovery metadata for audit without downloading their attachments;
6. create a rotating targeted repair queue for uncovered instruments;
7. search each missing instrument backwards through bounded annual-category windows until a valid latest report is found or listing/coverage bounds prove empty;
8. finish successfully only when every target is `available` or unexpired `confirmed_missing`; any incomplete/retryable/blocked scope makes the run partial.

Market-wide discovery prevents a normal one-request-per-stock strategy. Targeted scanning is reserved for the shrinking missing cohort. The run fixes `as_of`, source/query fingerprint, expected fiscal-year and listing/search bounds. A versioned fiscal-year policy derives the candidate upper year, disclosure-due year, and earliest searchable year from `as_of`, project timezone, fiscal-year end, listing date, configured disclosure-calendar boundary, provider coverage start, and bounded lookback. Version 1 defaults calendar-year A-share reports to an April 30 deadline in the following year; any market-calendar override changes the policy fingerprint. Candidate years are searched newest first. A completely empty not-yet-due year may allow the newest actually published older report to remain the latest available report, but an incomplete newer-year scope cannot. Asset availability is recorded separately from expected-period coverage (`not_due|current|overdue_missing|incomplete`). Once a report is due, complete absence becomes `overdue_missing` even when an older report is locally available. By default this degrades readiness and remains in repair but does not block daily enablement, because daily discovery is the mechanism that must detect a delayed filing; a versioned deployment policy may make it a stricter blocker. `confirmed_missing` requires complete successful coverage across the eligible search range, listing/source-bound evidence, and an expiry. A discovered but unverifiable latest correction blocks final coverage or leaves an already-serving predecessor explicitly provisional.

### Use cursor-driven daily windows with adaptive date partitioning

Daily state is keyed by source, exchange, normalized `annual_report` category, market scope, and query-policy fingerprint. It keeps provider item position separate from a range-coverage watermark. The normal start is committed `covered_until` minus a configurable overlap, defaulting to three calendar days, and the end is one fixed run cutoff. A fully successful window advances `covered_until` to its fixed end even when it contains no eligible records or only records older than the end; item timestamps alone never define completed time coverage.

If a window exceeds provider bounds, selected metadata is retained and the interval is bisected. A single dense day continues through durable page ranges or provider-supported subscopes under the fixed cutoff; its parent completes only when every child completes, otherwise it remains an explicit blocker. Discovery coverage advancement occurs only after the full metadata window completes successfully. Attachment failure after metadata completion does not roll back the discovery coverage watermark; it creates a separate retryable attachment operation. Primary and fallback sources keep independent item and coverage cursors. A fallback result cannot hide an incomplete required primary source unless the versioned route policy explicitly declares equivalent substitution; route-level completeness is the governed union of required source scopes and exposes every source gap.

After market discovery, one bounded rotating cohort repairs instruments missing the expected latest annual report. Reconciliation has two bounded layers: a longer publication-window scan and an oldest-first period-level queue over every managed `instrument + fiscal_year`. The latter persists `last_reconciled_at`, retry/checkpoint state, and a maximum cycle so failures do not silently advance an item and old fiscal years cannot starve; it can discover a correction first indexed years after its nominal publication window. The active-universe snapshot refreshes on a bounded cadence so new listings enter repair and delistings leave the denominator without deleting files. The daily run reports metadata discovery, late-revision reconciliation, and attachment readiness separately.

Bootstrap-to-daily handoff persists the bootstrap cutoff, query fingerprint, and only those per-source coverage watermarks whose equivalent daily discovery scopes completed through that cutoff. Once readiness gates pass, daily discovery starts from each valid handoff watermark minus overlap. Any scope without compatible complete coverage uses the configured bounded initial filing-season window, so enabling daily mode creates neither an unbounded rescan nor a gap between bootstrap and daily maintenance.

Alternative: fixed multi-year daily scan. Rejected because it repeats high-volume historical pages.

Alternative: query all instruments daily. Rejected because roughly 5,500 active stocks would create unnecessary identity and negative-result traffic.

### Provide hybrid download policy

The independent daily job proactively downloads both newly selected complete originals and complete corrections in version 1, as explicitly required. The architecture still separates metadata and attachment stages so future announcement types can use metadata-only or lazy policies.

On-demand ensure remains a correctness fallback for missed, manually requested, or historical assets. A scheduler run is an optimization and coverage mechanism, not a prerequisite for business use.

### Adopt current files before creating the canonical root

Migration inventories business-profile and broker manifests and validates source identity, instrument, report period, PDF signature, content length, and SHA-256. Valid current winners are registered without network download.

The default target root is a business-neutral content-addressed physical pool:

```text
data/filings/announcements/blobs/
  {content_hash_prefix}/{content_hash}.pdf
```

Instrument, fiscal-year, exchange, source, and filing identities remain database projections. During shadow migration, a canonical record may reference an existing path, including a broker-held complete original or complete correction. Shadow records remain excluded from production effective lookup, bootstrap coverage, and consumer parsing until source/period/classification/hash/effective-decision reconciliation passes and an explicit consumer cutover gate promotes them. A later verified move, hard link, or copy-verify-rename can converge storage without unnecessary network download. NFS mount identity and filesystem capabilities are probed first. Duplicate paths are deleted only through a per-file manifest/hash allowlist after both consumers read the shared asset and reconciliation passes; derived files, semiannual reports, other years, orphans, and conflicts are excluded.

### Treat storage capacity and backup as rollout gates

Current observed PDFs average approximately 4.6 MiB, with P95 approximately 12.9 MiB and a measured maximum approximately 43.8 MiB, projecting roughly 24-25 GiB for one full active A-share annual-report year. The remounted filings NFS has approximately 2.1 TiB free, so capacity is sufficient, but the service must not rely on a static estimate.

Before download, operations atomically reserve planned bytes on the target filesystem using Content-Length or a configured unknown-size budget, accounting for `.part`, quarantine, and old/new overlap. They also check free bytes, warning utilization, hard stop utilization, and absolute reserve. Metadata sync can continue after the attachment hard stop. Attachment size limits are separately configurable for annual reports because the current 50 MiB generic default is close to observed large files.

SQLite online backup does not cover `data/filings`. Add an incremental content-addressed archive backup to a verified independent storage failure domain, copying only missing blobs, validating hashes, and pairing its file manifest watermark with a recoverable catalog database snapshot that includes the replacement transaction. Independence is verified from a configured unambiguous failure-domain identity plus runtime mount source, server, export, and available filesystem identity; path names, host aliases, or labels alone are insufficient, and unverifiable independence fails closed. Existing hash-named targets are reverified rather than trusted by name. New or repaired targets use same-directory temporary files, flush, hash validation, and atomic publication; restart reconciles crashes before file publication or watermark commit without treating uncommitted bytes as protected. The current filings and PVE-Bak mounts share server `192.168.188.88`, so PVE-Bak alone cannot satisfy the deletion disaster-recovery gate; QuoteBak is currently on a different server with approximately 384 GiB free. That is sufficient for the approximately 24-25 GiB version 1 bootstrap and near-term annual increments, but not an unlimited-retention assumption. The backup target therefore has its own free-space warning, hard reserve, planned-byte preflight, temporary-file cleanup, freshness, and unprotected-byte state. Backup capacity failure degrades readiness and blocks predecessor deletion but never invalidates the local replacement. Version 1 performs no automatic backup-blob garbage collection; superseded backup blobs remain disaster-recovery-only and are not consumer-visible. A later audited retention/GC policy is required before reclaiming them. Never fall back to writing a NAS backup into the local data mount when the share is absent.

Restore enablement performs a complete presence, length, and SHA-256 reconciliation of every current-effective, retention-pinned, and pending-deletion replacement blob referenced by the restored catalog. Sampling remains useful for routine drills, but a sample cannot authorize consumer reads, writes, or destructive maintenance after recovery.

### Integrate front-facing workflows through DataManager and FastAPI

DataManager exposes shared list/get/ensure/readiness/operation methods. Existing annual-report catalog methods become read-through compatibility surfaces during migration.

Additive API resources expose:

- effective annual-report metadata and integrity for an instrument;
- asset-management readiness and coverage;
- bounded ensure operation creation and status;
- caller-scoped consumer continuation/processing status;
- controlled file streaming by asset id.

Responses expose stable asset lineage, not unrestricted local paths. All metadata and existing business-profile GETs remain zero-network. Asset availability, ensure disposition, operation status, operation stage, batch outcome, result origin, and consumer-processing state are separate. Business-profile and broker responses retain existing schemas while adding optional shared lineage. No current API must synchronously crawl the market.

Ensure accepts exactly one normalized selector: either effective-period identity or exact source-filing identity. Selector members are all-or-none, fiscal year and report period must agree when both are present, exact filing identity must belong to the path instrument, and caller-supplied URLs or paths are rejected. `Idempotency-Key` is bound to principal plus normalized request fingerprint; reuse with different input is a conflict rather than a new or reused operation. HTTP outcome mapping is versioned and deterministic so normal metadata absence remains a structured `200`, unknown resources are `404`, validation is `422`, idempotency/current-state conflicts are `409`, deleted predecessors are `410`, rate limits are `429`, and temporary infrastructure blockers are `503`, with authentication and permission failures following the configured non-disclosure policy.

A generic asset ensure obtains only the shared source asset. A front-facing business command endpoint owned by business-profile or broker risk-control is the entry point for starting domain processing: it accepts that consumer's processing fingerprint and caller idempotency key, creates or reuses a `consumer_request_id` on a local hit, or creates an `asset_request_id` plus a consumer-specific continuation when the asset is missing. It enqueues exactly one consumer operation when the asset becomes valid. Asset completion never implies that business-profile or broker output is already current. Only the owner or operator can query either request projection; internal asset and consumer operation ids remain operator/service details. Generic ensure never starts unrelated consumers. Local-hit/local-miss asset ensures return no `asset_request_id`; only asynchronous acquisition returns one.

The repository currently has rate limiting but no complete authentication middleware. Acquire, content, cancellation, repair, and operator endpoints therefore remain disabled unless a trusted identity and scoped permission boundary is configured. Durable SQLite operations, not FastAPI `BackgroundTasks`, are the source of truth. The repository supplies DataManager/FastAPI/OpenAPI contracts and UI state definitions; the actual external UI repository and owner are an enablement dependency.

### Publish asset-change events or watermarks for consumers

When an effective asset is added, replaced, repaired, withdrawn, or deleted, append a monotonic change event or watermark keyed by instrument/fiscal year/asset id. Each consumer keeps its own checkpoint and can replay missed events idempotently without rediscovery or redownload. A correction marks old consumer runs stale and drives domain-specific reprocessing; the asset service does not delete or rewrite business facts itself.

### Make operations resumable and single-flight

Durable asset operations use scopes, checkpoints, leases, attempts, retry times, bounded errors, and separate status/stage/outcome fields. The same normalized scope and policy version has at most one active asset operation across scheduler, API, and consumers. That internal operation is not transferred to the first caller: API/consumer requests create principal-scoped subscriptions or opaque query handles that bind caller idempotency and expose only authorized projections of the shared work. A second principal may subscribe to the same acquisition without inheriting the first principal's idempotency key, consumer continuation, or diagnostics. Files are written to `.part` on the same verified NFS mount, validated, fsynced where supported, atomically renamed, reopened, and reverified. Lease expiry permits cleanup and retry after process failure.

Version 1 cancellation detaches only the caller's asset subscription and any not-yet-started consumer continuation. Once bounded shared acquisition work exists, it continues even after the last subscription is detached; this avoids ownership races and produces a reusable canonical asset. Cancelling an asset request does not stop already-started consumer processing, which remains governed by that consumer's own explicit stop contract.

The scheduler control plane uses these durable operations for authenticated manual start, status, cooperative stop, resume, and duplicate-start reuse. It retains bounded run history and exposes last successful cutoff, heartbeat age, consecutive failures, cursor lag, and oldest retry age. Front-facing readiness is a redacted summary; provider, filesystem, actor, and failure diagnostics require operator scope.

## Risks / Trade-offs

- [Deleting predecessor PDFs weakens historical reproducibility] -> Preserve complete metadata, hashes, replacement/deletion audit, and explicitly scope version 1 to latest-effective local assets; require a later policy change before point-in-time source replay is claimed.
- [A correction notice is mistaken for a complete replacement] -> Require a complete full-report classification and validated PDF attachment before winner promotion.
- [Provider categories omit a report] -> Combine category-filtered market scans with bounded targeted repair of missing expected instruments.
- [A dense filing date exceeds page bounds] -> Persist completed metadata, continue stable page ranges/subscopes under a fixed cutoff, and advance the parent cursor only after all children complete; otherwise expose an unsplittable-day blocker.
- [Attachment failure is mistaken for discovery failure] -> Keep discovery cursor and attachment retry state separate; only incomplete metadata work retains the cursor.
- [Successful empty windows never move beyond the last announcement timestamp] -> Persist a separate range `covered_until` and advance it to the fixed cutoff for every complete window, including empty windows.
- [Fallback success hides an incomplete required source] -> Keep per-source cursors and gaps and let only an explicit versioned route policy declare equivalent substitution.
- [Late or backdated corrections are missed] -> Add a rotating long-lookback reconciliation cohort over already-covered assets, separate from the three-day low-latency overlap.
- [Old managed periods starve in bounded reconciliation] -> Persist per-period reconciliation age and retry state and schedule oldest-first against a configured maximum cycle.
- [Incomplete newest-year evidence is mistaken for an older latest year] -> Fix `as_of` and fiscal-year/search bounds, classify incomplete/blocked separately from confirmed missing, and never downgrade silently.
- [Bootstrap and daily maintenance leave a discovery gap] -> Adopt only compatible complete bootstrap coverage watermarks and start daily with overlap; use bounded initial lookback for every other scope.
- [Cross-source or withdrawn corrections select the wrong winner] -> Require attachment-level classification, governed mirror/legal precedence, withdrawal observations, and fail-closed ambiguity/provisional states.
- [Concurrent callers duplicate downloads] -> Use attachment-scoped leases, unique constraints, content-addressed blobs, and atomic writes.
- [Migration deletes a valid existing file] -> Adopt read-only first, reconcile manifests/hash/consumers, produce a per-file allowlist, switch reads, verify independent-failure-domain backup, and only then delete explicitly managed duplicates; never clean mixed directories.
- [Storage fills during filing season] -> Estimate planned bytes, enforce warning/stop thresholds and absolute reserve, stop attachment prefetch while continuing metadata.
- [NAS is unavailable] -> Keep local service operational, fail backup explicitly, expose readiness degradation, and prevent unsafe local fallback. Same-server exports do not count as independent disaster recovery.
- [Backup target fills as missing-only blobs accumulate] -> Enforce target-side warning/reserve/preflight, leave local assets valid, block destructive readiness, and defer backup-blob GC to a separately approved policy.
- [SQLite and NFS deletion are not one transaction] -> Persist deletion intent before unlink, finalize `deleted|failed` afterward, and run an idempotent reconciler with crash-injection tests.
- [Business parser status corrupts shared asset status] -> Separate asset and consumer-processing stores.
- [History reads use a post-cutoff correction or deleted predecessor] -> Apply knowledge-cutoff filtering and return metadata-only unavailable state when eligible predecessor bytes are no longer retained.
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
11. Stop legacy annual-report writes, persist a versioned `legacy_path -> content_hash/shared_asset/consumer` rollback manifest, prove legacy paths can be reconstructed from verified blobs, then remove duplicate business-owned files and code only after verified cutover and enforce repository residue checks.
12. Enable daily scheduling only after bootstrap reaches configured coverage, integrity, storage, backup, and migration gates.

Rollback before legacy-write removal disables shared consumer routing and daily scheduling while leaving additive records and adopted files intact; rollback SHALL NOT delete canonical metadata, replacement lineage, or audit records. Rollback after physical cleanup restores the verified archive backup plus its paired database snapshot and application version, then reconstructs required legacy paths from the immutable, versioned, hash-verified path manifest before legacy consumers start; code rollback alone is insufficient after predecessor or duplicate file deletion.

## Requirement Traceability

The detailed requirements document is the business-level source of intent. The following matrix records where each implementation-relevant requirement is made normative or trackable.

| Requirements document sections | Normative OpenSpec coverage | Implementation tasks |
| --- | --- | --- |
| 3-7: ownership, scope, identities, responsibilities | `official-announcement-assets` business-neutral/provider/identity/source separation; `research-data-engine` consumer contract | 1, 2, 8 |
| 8: canonical metadata/blob/effective/operation/audit state | durable operations, effective selection, replayable changes, deletion audit | 1, 2, 3, 7 |
| 9: classifier and correction precedence | attachment-level classifier, mixed attachments, cross-source ambiguity, withdrawal, retention pins | 2, 3 |
| 10: latest-only historical backfill | latest-only scenarios, fixed bounds, terminal status; independent scheduler backfill | 4, 5, 11 |
| 11: daily windows, dense pages, late corrections, universe lifecycle | daily discovery, cursor boundaries, separate attachment retry, independent daily scheduler | 6 |
| 12: local-first ensure and exact filing | local-first contract, exact filing, local-only mode, durable single-flight | 7, 8, 9 |
| 13: content-addressed storage, adoption, deletion | storage layout, atomic files, migration allowlist, mount and space gates | 3, 4 |
| 14-15: capacity, reservations, independent backup, restore | storage capacity/backup gates and paired restore scenarios | 3, 10, 11 |
| 16: independent jobs and configuration | scheduler independence, default rollout gates, bounded reporting, integrity/backup jobs | 1, 6, 10, 11 |
| 17-18: DataManager, API and frontend states | additive safe API, structured missing, HTTP/idempotency semantics, state separation, protected business-command entrypoints, nullable asset request handles | 7, 9 |
| 19-20: business-profile and broker migration | shared consumer/no legacy fallback; broker modified capability and lineage | 8, 9, 11 |
| 21-23: failure, observability and testing | durable operation/audit/lineage, scheduler reports, storage/backup scenarios | 3, 6, 7, 10, 11 |
| 24-25: acceptance, rollout and rollback | bootstrap cron gate and paired restore; migration/cleanup tasks | 4, 10, 11 |
| 26-27: V1 trade-offs and implementation entry | proposal non-goals/impact, extensibility requirement, complete checklist | all groups |

## Confirmed Scope And Deferred Decisions

- Version 1 sets the scheduled/bootstrap universe through the versioned active RMB-denominated A-share eligibility policy for SSE, SZSE, and BSE; B shares and non-stock security types are excluded, while inactive and delisted instruments remain on-demand only.
- Version 1 intentionally deletes superseded physical annual-report PDFs after safe replacement. A later requirement is needed if point-in-time reconstruction of source documents becomes mandatory.
- Semiannual reports reuse the architecture but are not enabled in the first scheduled rollout.
