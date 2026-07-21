## Context

The project has already converged on announcements as shared evidence, but not yet on a shared acquisition boundary:

- `research/providers/cninfo_announcements.py` provides reusable CNInfo metadata scanning, yet its public records, query configuration, and result types are source-named.
- shareholder, financial-disclosure, broker risk-control, company business-profile, and corporate-action flows call the CNInfo scanner directly and repeat checkpoint/audit orchestration.
- `research/business_profile_exchange_discovery.py` contains useful SSE, SZSE, and BSE announcement adapters, but they return business-profile candidates and classify titles inside the source adapter.
- attachment URL resolution and download transport are duplicated in business-profile archive, broker risk-control, and corporate-action document paths.
- persistence is named `cninfo_announcement_scan_state` and `cninfo_announcement_audit`, so another official source cannot use the same lifecycle without adding parallel tables or pretending to be CNInfo.

Announcements are point-in-time evidence. The acquisition layer must preserve publication timestamps, source identity, raw payloads, retrieval diagnostics, and conservative checkpoint behavior. It must not infer that an announcement proves a dividend term, company product, shareholder event, financial fact, or other business conclusion.

The implementation must coexist with active corporate-action and business-profile changes, preserve existing archives and normalized outputs, avoid live-network unit tests, and keep full-market work bounded and resumable.

## Goals / Non-Goals

**Goals:**

- Define one source-neutral contract for announcement discovery and attachment retrieval.
- Make CNInfo a concrete provider without losing current behavior or diagnostics.
- Reuse the existing official SSE, SZSE, and BSE endpoint work outside the business-profile domain.
- Separate provider capability from route priority and from business-specific selection.
- Persist source-neutral checkpoints and selected-announcement audit rows with raw lineage.
- Migrate existing consumers incrementally with parity tests, use compatibility adapters only inside the migration window, and remove every adapter and legacy path before this change is complete.
- Prevent partial scans from creating false completeness or skipping announcements.

**Non-Goals:**

- A universal announcement taxonomy or one classifier shared by all business domains.
- Automatic semantic extraction, LLM interpretation, fact approval, or factor promotion.
- Cross-source fuzzy deduplication or a claim that two similar titles are the same legal filing.
- Replacing existing business-specific immutable archive layouts, manifests, PDF parsing, OCR, or page-selection logic.
- Adding paid announcement providers, HKEX coverage, or a full-text search engine in the first implementation.
- Automatically running a full-market network crawl during deployment, startup, migration, or tests.
- Converting all existing synchronous research orchestration to asynchronous execution in this change.

## Decisions

### Use a layered acquisition boundary

Introduce a cohesive source-neutral announcement module with four responsibilities:

```text
business purpose + normalized query
  -> route resolver
  -> source provider discovery
  -> normalized announcements and attachments
  -> caller-owned selector/classifier
  -> generic checkpoint and audit persistence
  -> optional source-aware attachment fetch
  -> business-owned archive/parser/fact workflow
```

The common layer owns contracts, provider routing, scan orchestration, conservative state transitions, and transport. Source providers own endpoint parameters and response normalization. Business modules own classification and interpretation.

Alternative: expand `CninfoAnnouncementScanner` with more callbacks and exchange-specific branches. Rejected because the type system, configuration, storage, and caller vocabulary would remain coupled to one source.

Alternative: create one generic `DisclosureService` that also archives, parses, and promotes facts. Rejected because archive layout, correction lineage, parser rules, and approval semantics differ materially across corporate actions, business profiles, shareholders, and financial statements.

### Normalize source identity and attachments without hiding raw evidence

The source-neutral model will include at least:

- `source` and `source_announcement_id`;
- a stable source-qualified `announcement_key`;
- title and timezone-aware `published_at` plus the raw time value when available;
- market/exchange, symbols, security names, and provider organization identifiers;
- zero or more normalized attachment records containing source URL, resolved URL when known, media type/extension, attachment id/name, and raw metadata;
- raw source payload and normalization diagnostics.

The scan result will include source, status, records, page/request counts, provider cursor, maximum publication time, stop reason, retry diagnostics, and errors. A temporary migration adapter may expose the current single `adjunct_url` fields only until all callers switch; it is not part of the final architecture.

Source-qualified identity is authoritative. A deterministic fallback key may be generated from source, URL, title, symbol, and publication time only when the provider has no stable id, and the result must record that the identity is derived.

Alternative: use title plus date as a global id and merge sources. Rejected because corrections, duplicate titles, mirrored exchange filings, and source revisions make automatic legal-document identity unsafe.

### Model provider capabilities explicitly

Each provider declares supported exchanges, market-wide versus instrument-scoped discovery, date filtering, keyword/category filtering, cursor type, maximum page size, attachment retrieval, and any required provider identity resolution.

The common service validates a query against capabilities before network access. Provider-only parameters such as CNInfo `column`, `plate`, `orgId`, SSE `securityType`, or BSE form flags remain in validated source configuration or a provider-owned options mapping; business callers do not construct those fields.

CNInfo will internally resolve its stock/org identity when an instrument-scoped query requires it. Callers will no longer invoke `resolve_stock_identity()` as a prerequisite.

Alternative: require every provider to implement identical filters and pagination. Rejected because official sites expose materially different search contracts, and silent emulation would create misleading coverage.

### Separate capability registration from route selection

A provider registry resolves concrete providers by source name. A separate route configuration selects ordered sources by `purpose_key` and exchange, with an optional default route. Initial routes can preserve current behavior, for example CNInfo primary with the matching SSE/SZSE/BSE official endpoint as backup for business-profile discovery.

Fallback conditions are explicit: failed, degraded, identity-not-found, or successful-empty according to the purpose configuration. Every attempt is reported; selecting a fallback does not erase primary-source diagnostics.

Alternative: hard-code CNInfo-first inside each business service. Rejected because it repeats routing policy and prevents controlled source promotion or validation.

### Keep business selection outside providers

Providers return normalized announcements, not `BusinessProfileDocumentCandidate`, shareholder candidates, corporate-action facts, or broker reports. Callers apply source-neutral selectors to normalized records and may add purpose-specific selection reasons.

Existing classifiers such as business-profile document classification, shareholder filters, broker report matching, and corporate-action resolution remain in their current domains. The business-profile exchange adapters will be split so endpoint normalization becomes reusable and business classification becomes a downstream adapter.

This prevents source code from embedding unstable financial semantics and allows the same announcement to be audited independently for multiple purposes.

### Use generic scan scope and conservative cursor commits

Persist scan state by `purpose_key + source + scope_key`, where `scope_key` is a deterministic hash of the normalized market/instrument scope and relevant source query parameters. Store the readable normalized scope alongside the hash.

The state records the committed provider cursor, maximum normalized publication time, scan interval, counts, status, attempt diagnostics, and timestamps. A provider cursor may be a publication-time watermark or an opaque provider token; callers do not compare opaque cursors.

A cursor advances only when the provider reports a complete successful bounded scan for the requested interval. If any requested page fails, normalization becomes indeterminate, or the scan stops because a configured request/page bound is exhausted before reaching a known cursor, the prior committed cursor is retained. Overlap windows remain configurable to protect against late publication, timestamp precision, and ordering changes.

Alternative: always store the maximum timestamp observed before an error. Rejected because the next run could skip announcements from failed pages.

### Use a finite migration window and mandatory legacy retirement

Add generic `announcement_scan_state` and `announcement_audit` storage with `source`, source-qualified identity, normalized metadata, raw payload, selection reasons, purpose, instrument linkage, ingestion lineage, and timestamps.

Migration will backfill existing CNInfo rows idempotently. Legacy `get/upsert/store/list_cninfo_announcement_*` methods may exist only as temporary wrappers while callers are being converted. Before this change is complete, migrated row counts, source-qualified keys, cursor values, selection reasons, and raw-payload hashes must be verified; then the legacy runtime methods, old scanner facade types, old configuration keys, dual-write logic, and `cninfo_announcement_scan_state` / `cninfo_announcement_audit` tables are removed.

Versioned database migration files and historical OpenSpec records remain because they are required provenance, not active legacy functionality. Valid announcement evidence is migrated to the generic store before the old containers are dropped. No runtime compatibility fallback remains after rollout.

The generic audit table stores only announcements selected for a purpose, not every row seen on a market-wide scan. Scan counts and diagnostics record the unselected population without creating uncontrolled storage growth.

The audit store is an internal operational repository in this change, not a public research dataset. It exposes bounded repository reads for maintenance and reconciliation; the public API remains explicitly gated off until a separate consumer-facing query contract is justified. This satisfies the read-path requirement without exposing raw source payloads or unstable operational schema as a public API.

Alternative: keep the old tables and wrappers for one or more releases. Rejected because permanent dual paths create ambiguous ownership, allow state divergence, and leave historical garbage. A bounded migration window provides rollout safety without making compatibility code part of the steady state.

### Enforce replacement and zero-residue cleanup as rollout gates

The common announcement module is not considered online merely because its new providers and tables are available. Rollout completes only after every existing announcement consumer has switched to the source-neutral service and the superseded implementation has been removed from the active repository and runtime schema.

The rollout gate is ordered and fail-closed:

1. inventory all announcement acquisition entry points in business modules, `data_manager.py`, scheduler paths, scripts, tests, configuration, storage methods, and attachment transports;
2. replace each entry point with the common acquisition or retrieval contract and prove consumer-level parity for identities, publication times, URLs, scan counts, cursor decisions, audits, and public/business outputs;
3. stop legacy writes, reconcile legacy and generic rows including payload hashes and lineage, and verify a restorable pre-cleanup database backup;
4. delete legacy facade types, exports, storage wrappers, tables, configuration keys, scheduler/script parameters, duplicated source transports, duplicated attachment download code, obsolete fixtures/tests, and documentation that describes the old path as active;
5. run repository-wide residue checks and clean/migrated-database tests. Any unexplained legacy symbol, active old table, dual-write branch, fallback path, obsolete configuration entry, or duplicated transport blocks rollout.

Only versioned migration history and clearly marked historical OpenSpec records may retain legacy names. They are provenance, not callable compatibility surfaces. Rollback restores the verified backup together with the preceding application release; legacy code and tables are not retained in the new release as a rollback mechanism.

### Centralize attachment retrieval but keep archives domain-owned

Providers or a source-specific attachment resolver produce absolute URLs and required request metadata. A common attachment fetch service applies project HTTP transport, TLS profile, headers, timeout, retry, pacing, byte limits, content length, media-type/signature diagnostics, and SHA-256 hashing.

The result contains bytes and retrieval metadata but does not choose a business archive path or parse the content. Business-profile, corporate-action, broker, and financial services continue to create their own manifests, immutable paths, page artifacts, and parser versions.

Redirects to an unapproved host or downloads exceeding configured limits fail explicitly. A PDF content-type mismatch is diagnostic; an invalid PDF signature remains a hard failure for callers requiring a PDF.

Alternative: immediately replace all domain archives with one shared document store. Rejected because current schemas and correction/supersession rules are domain-specific and already used by active work.

### Preserve synchronous orchestration for the first migration

The initial provider protocol remains synchronous because current consumers, injected test sessions, scheduler tasks, and storage operations are synchronous. It will enforce bounded requests and must not start background work that survives a timeout. The contracts avoid embedding `requests` types so an async provider/service can be added later without changing normalized models.

Alternative: make this change also convert every consumer to async. Rejected as excessive scope with little correctness benefit for a primarily sequential, rate-limited official-site workflow.

### Migrate consumers in risk order with parity gates

Migration order:

1. introduce contracts, CNInfo provider, registry, service, generic storage, and strictly temporary migration wrappers;
2. migrate business-profile discovery and attachment download because it already exercises multiple official sources;
3. migrate shareholder and financial-disclosure incremental scans;
4. migrate broker risk-control and corporate-action announcement/document paths;
5. remove direct consumer imports only after fixture-based parity tests and targeted bounded live validation;
6. verify migrated data and remove all legacy wrappers, tables, configuration keys, duplicated transports, dead exports, and obsolete tests within this same change.

For each consumer, normalized candidate counts, selected ids, timestamps, URLs, watermark decisions, and existing output schemas must match the pre-migration baseline unless a documented correctness defect is intentionally fixed.

## Risks / Trade-offs

- [The generic model becomes a lowest-common-denominator abstraction] -> Keep provider capabilities and raw payloads explicit; do not pretend unsupported filters or cursors exist.
- [A migration changes evidence identity] -> Preserve provider ids, add source-qualified keys, backfill idempotently, and test old-to-new identity mapping.
- [Partial scans advance checkpoints and lose filings] -> Commit cursors only after complete successful scans and retain overlap windows.
- [New routing causes unexpected fallback traffic] -> Preserve current default routes, bound every attempt, log route decisions, and require explicit configuration for new sources or fallback-on-empty behavior.
- [Attachment centralization breaks domain archive behavior] -> Centralize only retrieval; keep archive naming, manifests, parsing, and supersession in existing services.
- [Active changes edit the same consumer files] -> Land the common layer and compatibility seams first, then migrate consumers in small commits after rebasing against active work.
- [Legacy deletion occurs before evidence is safely migrated] -> Require row-count, key, cursor, selection-reason, and payload-hash reconciliation plus a database backup before dropping legacy tables or removing the rollback build.
- [Temporary compatibility code becomes permanent] -> Track explicit deletion tasks and make repository-wide residual scans and clean-database schema checks release gates.
- [Official endpoint drift] -> Keep source fixtures, raw payload lineage, response-shape diagnostics, configurable endpoint parameters, and targeted live probes outside unit tests.
- [Generic audit storage grows quickly] -> Persist selected purpose-specific records, not every scanned record, and retain bounded list/read operations.

## Migration Plan

1. Add source-neutral models, provider protocol, capability declarations, registry, acquisition service, and attachment fetch result types.
2. Implement the CNInfo provider by adapting the current scanner and add fixture parity tests for pagination, identity resolution, response variants, page-size caps, timestamps, and failures.
3. Add generic storage tables and repository methods; idempotently backfill legacy CNInfo state/audit rows and expose temporary migration wrappers.
4. Extract SSE/SZSE/BSE endpoint normalization from the business-profile module into reusable providers and preserve business-profile routing through configuration.
5. Migrate consumers one at a time with old-versus-new fixture parity tests and unchanged public/business outputs.
6. Replace duplicated attachment retrieval with the common fetch service while retaining domain archive and parser code.
7. Run targeted read-only live probes for CNInfo and each enabled exchange source, followed by bounded dry-run consumer validation.
8. Stop legacy writes after generic read/write parity is proven and reconcile migrated counts, keys, cursors, reasons, and raw-payload hashes.
9. Create a pre-cleanup database backup, remove legacy CNInfo runtime methods and facade types, drop migrated legacy tables, remove old configuration keys and duplicated transport code, and run residual scans plus clean-database smoke tests.

Before destructive schema cleanup, rollout must create and verify a database backup. Rollback before cleanup disables the generic routes and uses the temporary migration build; rollback after cleanup restores the verified pre-cleanup database backup together with the prior application release. The final deployed state does not retain legacy tables or compatibility code solely for rollback convenience.

## Open Questions

- Whether a later change should consolidate immutable raw announcement documents across business domains after current archive schemas and correction lineage stabilize.
- Which HKEX disclosure source and identity model should implement the same contract when Hong Kong announcement coverage is prioritized.
