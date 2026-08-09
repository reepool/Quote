## Why

Official annual-report PDFs are already required by business-profile extraction and broker risk-control ingestion, but archive ownership, manifest semantics, revision handling, and download avoidance still depend on those business modules. A shared announcement-asset capability is now required so annual reports are discovered, downloaded, validated, retained, replaced, and served independently of whether any one consumer is enabled or production-ready.

## What Changes

- Introduce a business-neutral official announcement asset service built on the existing `research.announcements` provider, routing, checkpoint, and attachment-retrieval contracts.
- Add an independent A-share annual-report daily task, enabled only after bootstrap/readiness gates pass, that discovers recent formal full annual reports and corrections, proactively downloads both original and corrected effective attachments, and maintains one effective local attachment per instrument and fiscal year.
- Add a resumable latest-only historical backfill for the current active SSE, SZSE, and BSE stock universe. It finds and stores only each instrument's latest available fiscal-year annual report, preferring the newest valid correction.
- Add local-first consumer APIs: read-only GETs remain zero-network; callers first reuse a verified local asset; when absent, an authorized explicit ensure may create or reuse a durable idempotent operation to discover, register, and download the required annual report without depending on business-profile scheduling.
- Centralize formal annual-report classification, correction precedence, source-qualified identity, content hashing, atomic archive writes, integrity checks, acquisition leases, retry state, and deletion audit.
- When a verified correction becomes effective, atomically switch the active record and remove any superseded physical attachment (original or earlier correction) only when it has no remaining retention pin and the independent backup/deletion gates pass. Preserve announcement metadata, hashes, replacement lineage, processing invalidation, and deletion audit even though the prior file is no longer retained.
- Reconcile and reuse valid annual-report files already stored under `data/filings/business_profile` and `data/filings/financial_statements/broker_risk_control`; do not redownload valid existing content during migration.
- Move business-profile and broker risk-control to the shared annual-report asset dependency while leaving their PDF parsing, derived artifacts, fact storage, review, and promotion semantics domain-owned.
- Expose stable DataManager and FastAPI contracts for annual-report availability, effective-version metadata, integrity status, and bounded ensure/download requests so current and future front-facing workflows can integrate without reading archive paths or business-profile tables directly.
- Add independent default-disabled configuration, scheduler reporting, disk-space gates with concurrent byte reservations, paired database/file restore requirements, independent-failure-domain file backup, observability, reconciliation, and rollout controls. The first release covers formal annual reports and corrections; semiannual and other announcement types remain extension points and are not proactively downloaded by annual-report maintenance.
- **BREAKING**: business consumers may no longer directly own or download formal annual-report attachments after migration. Legacy business-owned annual-report archive writes and duplicate active files are retired after reconciliation.

## Capabilities

### New Capabilities
- `official-announcement-assets`: Canonical announcement metadata, annual-report classification, local-first attachment acquisition, latest-effective revision management, storage, reuse, APIs, migration, and operational governance.

### Modified Capabilities
- `scheduler`: Adds independent annual-report latest-backfill and daily-update jobs with bounded discovery, resumable state, reporting, and business-independent enablement.
- `broker-annual-report-risk-control-source`: Replaces broker-owned annual-report discovery/download/archive behavior with the shared asset service while preserving broker parsing and fact contracts.
- `research-data-engine`: Adds stable business-facing and API-facing annual-report asset access and requires business-profile annual-report acquisition to use the shared dependency.
- `data-storage-layout`: Defines the shared archive root, existing-file adoption, one-effective-file retention, disk gates, and file-backup expectations under the remounted `data/filings` volume.

## Impact

- Affected code: `research/announcements`, a new shared asset package/repository/service, `research/annual_report_assets.py`, `research/business_profile_archive.py`, business-profile production orchestration, `research/broker_risk_control.py`, `data_manager.py`, FastAPI routes/models, scheduler tasks/configuration, research storage, validation scripts, and documentation.
- Affected data: new canonical announcement/attachment/blob/effective-annual-report/processing-link state; adopted references to existing annual-report files; eventual removal of duplicate and superseded physical annual-report files after verified cutover.
- Affected operations: one latest-only bootstrap, an independent daily metadata-and-attachment update, long-lookback late-revision reconciliation, on-demand acquisition, archive integrity audits, storage-watermark gates, and an incremental independent-failure-domain file-backup workflow separate from SQLite database backup.
- Public financial facts, DCF contracts, business-profile facts, and existing API responses remain compatible. New annual-report asset endpoints are additive.
- Version 1 intentionally optimizes current/latest annual-report availability rather than historical point-in-time reconstruction of superseded physical PDFs; metadata and lineage remain auditable after physical predecessor deletion.
