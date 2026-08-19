## Context

`research.announcement_assets` now owns annual-report discovery, classification, effective-version selection, download, blob storage, integrity, and recovery. Business-profile production already supports binding a shared asset, but it can still read or write the former business-profile archive manifest. Broker risk control and DataManager also retain compatibility paths. This leaves two owners for the same raw PDF and makes failures dependent on configuration flags such as `dual_read` and `legacy_fallback_enabled`.

The migration must preserve derived business-profile outputs and the financial fact write chain. It changes only how raw annual/semiannual report assets are obtained and identified.

## Goals / Non-Goals

**Goals:**

- Make `research.announcement_assets` the only production owner and read path for raw annual-report assets.
- Make business-profile and broker risk-control consumers fail with explicit asset readiness diagnostics when the shared asset is missing or invalid.
- Delete legacy acquisition, archive, manifest, compatibility facade, scheduler, configuration, and tests after all callers are migrated.
- Preserve shared asset integrity, effective correction selection, lineage, leases, asynchronous recovery, and consumer-specific derived artifacts.

**Non-Goals:**

- Rework PDF parsing, semantic extraction, unit normalization, financial numeric fact storage, or LLM routing.
- Migrate unrelated financial filing attachments that are not raw annual/semiannual report assets.
- Preserve a runtime switch or rollback path to the legacy archive implementation.
- Move consumer-derived page, section, semantic, or publish artifacts into the shared raw-asset module.

## Decisions

### One raw-asset owner with consumer projections

Production consumers will resolve a shared effective asset and use its immutable content handle, content hash, source identity, fiscal period, and availability time. Consumer queues may persist those fields as a projection for deterministic retries, but they will not copy ownership metadata into a second annual-report catalog.

Keeping a small projection in a work item is preferable to querying a mutable effective pointer in every stage: a correction discovered later can enqueue new work while an existing retry remains tied to the exact asset it started with.

### No legacy fallback or compatibility mode

The former `dual_read`, `legacy_fallback_enabled`, and legacy writer switches will be removed rather than set to false. Missing shared data becomes an explicit `asset_not_ready` or integrity result and is repaired by the shared ensure/backfill workflow.

This intentionally removes rollback to legacy code. Git history and database backups provide operational rollback; maintaining executable duplicate acquisition code would violate the ownership requirement.

### Shared module owns raw files; consumers own derived artifacts

Raw PDFs and their metadata live only in the shared announcement asset store. Business-profile page caches, selected sections, LLM results, and publish state remain consumer artifacts keyed by the shared asset identity/content hash. Broker facts remain in the existing financial fact tables and reference the shared source identity/hash.

### Historical reads reselect visible shared evidence

A knowledge-cutoff read will ask the shared announcement asset service to select from only announcements and attachment versions visible at that cutoff. It will not filter today's mutable effective pointer and will not mutate the current projection. Discovery and coverage views will page only effective assets whose shared blob is locally valid and integrity-valid, so historical candidates and metadata-only rows cannot starve or inflate production coverage.

### Retire entry points only after caller migration

Compatibility APIs and DataManager methods will either become thin calls to the new access facade when they are still public and useful, or be deleted when they have no production callers. Legacy archive sync commands and jobs are deleted because their behavior is fully replaced by shared asset ensure, daily sync, and latest-report backfill.

## Risks / Trade-offs

- [Existing legacy-only files have not been imported into the shared catalog] -> Do not read them implicitly; run the existing shared asset registration/backfill before expecting consumers to process them.
- [A queued business-profile item references a removed legacy manifest] -> Requeue from the effective shared asset; preserve already completed derived results only when their source hash matches a current shared blob.
- [Broker semiannual support is not yet represented by the shared catalog] -> Extend the existing announcement asset classifier/service in the same authoritative module rather than retaining the broker downloader.
- [Deleting old APIs breaks an unknown external caller] -> Scan repository commands, scheduler config, docs, and tests; keep only a documented thin new-facade adapter where a public contract is still actively used.

## Migration Plan

1. Inventory imports, configuration, commands, jobs, tests, and data contracts that reference legacy annual-report assets.
2. Make consumer input resolution shared-only and add representative missing/invalid/correction tests.
3. Migrate any still-used public lookup surface to `research.announcement_assets.access`.
4. Delete legacy writers, downloader/archive modules, sync jobs, flags, and tests; update current documentation.
5. Run targeted tests, import/reference scans, and temporary-database end-to-end checks.
6. Deploy shared-only code. Existing shared database rows/blobs are reused; missing coverage is filled by shared asset backfill.

Rollback is by reverting the deployment commit and restoring the pre-deployment database backup if schema/data changes require it. No runtime legacy switch remains after migration.

## Open Questions

None. The user explicitly selected immediate shared-only ownership and deletion of the legacy implementation.
