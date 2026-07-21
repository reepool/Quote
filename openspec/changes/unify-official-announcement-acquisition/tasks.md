## 1. Source-Neutral Contracts and Configuration

- [x] 1.1 Add source-neutral announcement query, scope, record, attachment, scan-result, route-attempt, and retrieval-result models with explicit raw lineage and timezone diagnostics.
- [x] 1.2 Define the synchronous announcement provider protocol and capability declaration for exchanges, scopes, filters, cursors, page limits, identity resolution, and attachment retrieval.
- [x] 1.3 Add deterministic source-qualified announcement keys and normalized scan-scope keys, including derived-identity diagnostics when a provider id is unavailable.
- [x] 1.4 Add provider registry and route resolver that separates capability eligibility from ordered source selection by purpose and exchange.
- [x] 1.5 Extend research configuration loading and validation for announcement providers, routes, fallback conditions, overlap windows, request bounds, and attachment host/size policies.
- [x] 1.6 Add source-neutral acquisition orchestration with caller-owned selectors, complete attempt diagnostics, and no business-semantic inference.

## 2. CNInfo Provider Migration

- [x] 2.1 Implement the CNInfo provider behind the common protocol by adapting current announcement query construction, response extraction, record normalization, and page-size limits.
- [x] 2.2 Move CNInfo stock/org identity resolution behind instrument-scoped provider discovery with typed not-found and failed outcomes.
- [x] 2.3 Preserve CNInfo TLS, headers, timeout, retry, backoff, pacing, bounded pagination, raw payloads, and stop diagnostics through provider configuration.
- [x] 2.4 Provide a strictly temporary migration adapter for `CninfoAnnouncementRecord`, `CninfoAnnouncementScanConfig`, `CninfoAnnouncementScanResult`, and `CninfoAnnouncementScanner` callers and mark every adapter entry point for mandatory removal in section 8.
- [x] 2.5 Add offline CNInfo fixtures and unit tests for all supported response containers, timestamps, duplicate values, identity lookup, effective page bounds, retry exhaustion, malformed payloads, and successful empty results.

## 3. Generic Checkpoint and Audit Persistence

- [x] 3.1 Add additive `announcement_scan_state` storage keyed by purpose, source, and normalized scope, with readable scope metadata, committed cursor, publication watermark, counts, status, attempts, and timestamps.
- [x] 3.2 Add additive `announcement_audit` storage for purpose-specific selected records with source-qualified identity, instrument linkage, normalized metadata, selection reasons, raw payload, and ingestion lineage.
- [x] 3.3 Implement atomic conservative cursor commits that retain the prior cursor for failed, degraded, indeterminate, or prematurely bounded scans.
- [x] 3.4 Add idempotent migration/backfill from existing `cninfo_announcement_scan_state` and `cninfo_announcement_audit` rows, preserving cursors, reasons, lineage, and raw payloads for later reconciliation.
- [x] 3.5 Implement temporary legacy CNInfo storage wrappers and parity tests for get, upsert, store, and list behavior during the migration window, with no final-state fallback contract.
- [x] 3.6 Add bounded repository reads and decide whether the operational audit remains API-gated or receives a dedicated maintenance read endpoint; document the chosen contract.

## 4. Official Exchange Providers and Routing

- [x] 4.1 Extract SSE announcement request, response, pagination, identity, and attachment normalization from the business-profile module into a reusable provider.
- [x] 4.2 Extract SZSE announcement request, response, pagination, identity, and attachment normalization into a reusable provider.
- [x] 4.3 Extract BSE announcement request, response, pagination, identity, and attachment normalization into a reusable provider.
- [x] 4.4 Register CNInfo, SSE, SZSE, and BSE capabilities and validate that routes cannot select providers lacking the requested exchange or query feature.
- [x] 4.5 Configure current purpose routes to preserve existing primary/fallback behavior and record every attempted source without introducing new unbounded fallback traffic.
- [x] 4.6 Add fixture tests for exchange JSON/JSONP variants, provider-specific query parameters, derived ids, symbol filtering, page completion, malformed payloads, and route fallback decisions.

## 5. Governed Attachment Retrieval

- [x] 5.1 Implement source-aware attachment URL resolution and approved-host policy so business modules no longer hard-code CNInfo or exchange attachment base URLs.
- [x] 5.2 Implement bounded attachment retrieval through project HTTP transport with TLS, source headers, timeout, retry, pacing, redirect validation, byte limits, and explicit failures.
- [x] 5.3 Return content hash, byte length, final URL, response media type, signature diagnostics, and retrieval timestamp without choosing an archive path or parsing the document.
- [x] 5.4 Add offline tests for relative URLs, redirects, host rejection, retryable failures, empty responses, oversize content, content-type mismatch, valid PDF signatures, and invalid PDF signatures.

## 6. Business-Profile Consumer Migration

- [x] 6.1 Refactor CNInfo business-profile discovery to consume normalized announcements and keep title/document classification in `research.business_profile_documents`.
- [x] 6.2 Refactor the business-profile source coordinator to use common configured routes and route-attempt diagnostics while preserving its public resolution schema.
- [x] 6.3 Replace business-profile attachment transport with the common retrieval service while preserving immutable archive layout, manifest lineage, correction handling, checkpoints, and PDF artifacts.
- [x] 6.4 Add old-versus-new fixture parity tests for candidate ids, classifications, timestamps, URLs, counts, fallback choices, and dry-run/write behavior.

## 7. Remaining Consumer Migration

- [x] 7.1 Migrate shareholder incremental announcement scans and audits to the common acquisition service while preserving shareholder filters, candidates, result schema, and overlap behavior.
- [x] 7.2 Migrate financial-disclosure incremental scans, lifecycle event inputs, audit reuse, and checkpoints while preserving report-period availability semantics.
- [x] 7.3 Migrate broker risk-control announcement discovery and attachment retrieval while preserving report classification, source manifests, parsing, and retryable-pending states.
- [x] 7.4 Migrate corporate-action announcement discovery and document retrieval while preserving structured CNInfo observations, special-action resolution, archives, page selection, LLM evidence, and factor governance boundaries.
- [x] 7.5 Migrate every remaining direct CNInfo announcement import in `data_manager.py`, scripts, scheduler paths, and tests so no consumer depends on compatibility imports.
- [x] 7.6 Add per-consumer parity tests for selected source identities, publication times, attachment URLs, scan counts, audit rows, cursor decisions, and unchanged public/business outputs.

## 8. Legacy Removal and Repository Cleanup

- [x] 8.1 Reconcile legacy and generic scan-state/audit row counts, source-qualified keys, cursor values, selection reasons, ingestion lineage, and raw-payload hashes, and fail cleanup on any unexplained difference.
- [x] 8.2 Create and verify a pre-cleanup database backup, then add the migration that drops `cninfo_announcement_scan_state` and `cninfo_announcement_audit` after successful reconciliation.
- [x] 8.3 Remove legacy CNInfo scanner facade classes, source-specific compatibility result/config/record types, storage methods, wrapper exports, dual-write branches, and fallback-to-legacy runtime paths.
- [x] 8.4 Remove domain-local SSE/SZSE/BSE announcement transport left in business-profile code and remove duplicated CNInfo/exchange attachment URL and download implementations replaced by the common retrieval service.
- [x] 8.5 Remove obsolete announcement configuration keys, aliases, scheduler/script parameters, fixtures, tests, and documentation; migrate still-valid coverage to the new names instead of leaving dead compatibility entries.
- [x] 8.6 Add a repository-wide residual check for legacy imports, methods, table names, hard-coded attachment hosts, duplicate transport, old configuration keys, scheduler/script parameters, obsolete fixtures/tests, and active documentation; allow legacy names only in versioned database migration history and clearly marked historical OpenSpec records, and make any unexplained match block rollout.
- [x] 8.7 Add clean-database and migrated-database tests proving only generic announcement tables and runtime interfaces remain while all previously valid evidence and consumer outputs are preserved.

## 9. Observability, Validation, and Documentation

- [x] 9.1 Add stage logs and diagnostics for route resolution, effective request bounds, page attempts, records seen, selected counts, cursor commit decisions, attachment retrieval, elapsed time, and failures without logging secrets.
- [x] 9.2 Add tests proving a later-page failure, malformed payload, exhausted bound, or identity failure cannot advance a committed cursor or become a confirmed empty result.
- [x] 9.3 Run the focused unit suites for announcement providers, storage migration and cleanup, business-profile discovery/archive, shareholder sync, financial disclosure, broker risk control, and corporate actions.
- [x] 9.4 Add a bounded read-only live validation script or extend an existing probe for CNInfo, SSE, SZSE, and BSE with explicit instruments, dates, pages, pacing, and no production writes.
- [x] 9.5 Run targeted live probes, record source capability/latency/response-shape findings, and resolve any discrepancy before enabling migrated routes by default.
- [x] 9.6 Update research architecture, configuration, execution, and affected business-domain documentation to describe the common announcement boundary, mandatory legacy replacement, point-in-time assumptions, migration status, and known source limitations.
- [x] 9.7 Confirm legacy announcement tables, runtime implementations, compatibility exports, fallback/dual-write paths, old configuration, duplicated transport, obsolete tests, and active legacy documentation were removed; confirm authoritative announcement evidence and archives remain intact, business facts/factor versions/public APIs were not unintentionally changed, and the backup-based rollback procedure is documented and tested. Treat this as a release gate, not post-release cleanup.
