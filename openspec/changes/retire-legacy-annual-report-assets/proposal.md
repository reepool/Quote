## Why

The shared announcement asset module is now the authoritative owner for annual-report discovery, version selection, download, storage, integrity, and recovery, but production consumers still retain legacy archive and manifest fallbacks. Keeping both paths creates duplicate downloads, ambiguous ownership, and repeated fixes in obsolete code, so annual-report consumption must become shared-only and the legacy implementation must be removed.

## What Changes

- **BREAKING** Require all annual-report consumers to obtain effective report assets through `research.announcement_assets`; legacy manifest and archive fallback is removed.
- Migrate business-profile and broker risk-control annual-report inputs to shared asset handles and persisted shared asset content.
- Remove legacy annual-report download, archive, compatibility catalog, official archive sync, writer, configuration, commands, and dedicated tests after consumers are migrated.
- Preserve business-profile derived artifacts such as parsed pages, selected sections, semantic results, and publication state; only the upstream annual-report ownership changes.
- Treat a missing or invalid shared annual-report asset as an explicit readiness condition handled by the announcement asset workflow, never as permission to invoke a legacy source.
- Add regression checks proving that production code has no legacy annual-report imports, modes, fallback flags, or writers.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `broker-annual-report-risk-control-source`: Require broker risk-control evidence to consume only effective assets managed by the shared announcement asset service.
- `data-storage-layout`: Establish the shared announcement asset store as the only persisted raw annual-report asset location and remove legacy annual-report archive/manifest storage contracts.
- `research-data-engine`: Require business-profile annual-report processing to consume only shared announcement assets and expose explicit not-ready states when an asset is unavailable or invalid.
- `scheduler`: Remove legacy annual-report archive synchronization jobs and route discovery, download, repair, and backfill exclusively through shared announcement asset jobs.

## Impact

- Affected modules include `research/announcement_assets`, business-profile production and semantic input resolution, broker risk control, scheduler/data-manager adapters, API accessors, and research configuration.
- Legacy modules such as `research/annual_report_assets.py`, the old business-profile annual-report archive/downloader, and official archive sync are deleted once references are removed.
- Existing shared announcement asset database rows and blobs remain authoritative. Existing business-profile derived data remains usable when its source asset identity and content hash still match.
- Operators must run shared announcement asset ensure/backfill operations when a report is missing; there is no compatibility switch back to the retired implementation.
