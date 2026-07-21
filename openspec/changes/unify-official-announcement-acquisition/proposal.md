## Why

CNInfo announcement discovery is already reused by corporate actions, shareholder events, financial disclosure maintenance, broker risk-control ingestion, and company business-profile evidence, but its models, checkpoints, audit tables, and attachment download paths remain CNInfo-specific or duplicated inside business modules. A source-neutral announcement acquisition capability is now justified so new disclosure-driven workflows can reuse one governed pipeline while CNInfo, SSE, SZSE, BSE, and future official sites remain replaceable source implementations.

## What Changes

- Introduce a source-neutral announcement acquisition contract for bounded discovery, normalized announcement and attachment metadata, source diagnostics, watermark handling, and attachment retrieval.
- Make CNInfo the first concrete provider behind that contract, preserving its current pagination limit, stock identity resolution, retry, pacing, TLS, and raw-payload lineage.
- Move the existing SSE, SZSE, and BSE announcement endpoint logic out of the company business-profile domain into reusable official-source providers, without claiming that every provider has identical search capabilities.
- Add a provider registry and configuration-driven source routes by purpose and exchange; capability declarations determine eligibility, while route order determines primary and fallback behavior.
- Replace CNInfo-named scan-state and audit persistence with source-neutral records keyed by purpose, source, and scan scope. The migration SHALL preserve evidence while it is copied and verified, then remove the legacy tables and runtime storage methods within this change.
- Centralize source-aware attachment URL resolution and download transport. Business-specific immutable archive layout, PDF/OCR extraction, title classification, semantic parsing, and fact promotion remain outside the common acquisition module.
- Migrate existing disclosure consumers incrementally, verify parity, and then delete direct imports of `CninfoAnnouncementScanner`, legacy CNInfo record/config/result facades, domain-local exchange transport, duplicated attachment download code, and obsolete configuration/tests.
- Require conservative failure semantics: partial or failed scans do not advance a committed watermark, and an empty successful result remains distinct from a transport or parser failure.
- **BREAKING**: remove the internal legacy CNInfo announcement scanner/storage interfaces and `cninfo_announcement_*` runtime tables after all callers and data have migrated; no permanent compatibility aliases, dual writes, or legacy fallback path remain when the change is complete.

## Capabilities

### New Capabilities

- `official-announcement-acquisition`: Source-neutral discovery, routing, normalization, checkpoint/audit persistence, and attachment retrieval for official company announcements.

### Modified Capabilities

None.

## Impact

- Affected code: `research/providers/cninfo_announcements.py`, `research/business_profile_discovery.py`, `research/business_profile_exchange_discovery.py`, `research/business_profile_archive.py`, `research/financial_disclosure_incremental_sync.py`, `research/shareholder_incremental_sync.py`, `research/broker_risk_control.py`, corporate-action discovery/document services, provider exports/registry, research configuration, and `research/storage.py`.
- Affected data: source-neutral scan-state and announcement-audit persistence plus a verified migration from existing `cninfo_announcement_*` tables. After count, key, and payload-lineage verification, the legacy tables are dropped; announcement evidence, archived documents, factors, profile facts, and financial facts remain preserved in their authoritative stores.
- Public APIs and business result schemas remain stable. Internal legacy provider, storage, configuration, and import interfaces are intentionally removed after callers switch to the source-neutral replacements.
- No new heavyweight dependency is required. Network behavior remains bounded, rate-limited, retry-aware, and disabled from unit tests through fixtures and injected transports.
