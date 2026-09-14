## Context

The current semantic runtime can select report sections, run deterministic and LLM extraction, verify exact evidence, create governed candidates, and promote records through manifests. Production state is nevertheless empty because discovery starts only from existing business-profile manifests or retry rows, all business-profile switches are disabled, scheduler parameters do not name field families or promotion identities, the production CLI constructs an empty counterparty resolver, and every product-to-commodity mapping remains candidate-only.

The operating constraint is unattended production with minimal human review. Correctness remains fail-closed: the system must not invent counterparties, roles, commodity mappings, or valuation inputs to improve coverage.

## Goals / Non-Goals

**Goals:**

- Discover new and corrected disclosures for the active A-share universe without downloading every issuer's full history.
- Process the minimum sufficient documents and pages for each missing or stale field family.
- Prefer deterministic extraction and local rules; invoke the LLM only for bounded semantic gaps.
- Automatically publish facts when all frozen gates pass and persist machine-rework reasons otherwise.
- Preserve immutable evidence and make archive cleanup evidence-aware.
- Fix relationship, concentration, role, and commodity mapping correctness gaps.

**Non-Goals:**

- Enabling production promotion before benchmark manifests and runtime identities are available.
- Automatically accepting fuzzy counterparty matches or model-proposed entity ids.
- Inferring pass-through, hedge effectiveness, lag, materiality, or DCF parameters from LLM text.
- Deleting official PDFs, manifests, approved history, or database rows merely because current production tables are empty.
- Downloading all historical annual reports for all listed companies.

## Decisions

### Use an index-first discovery frontier

A lightweight official announcement-index scan will enumerate active issuers and relevant annual, semiannual, correction, and specialist events. The frontier persists issuer, announcement identity, publication time, report period/type, index payload hash, scan watermark, and processing status. Semantic scope is the union of changed frontier items, manifest coverage gaps, stale field families, and due machine retries.

Scanning every issuer by calling per-company disclosure acquisition was rejected because it is slow, repeats negative lookups, and can starve later issuers under a fixed batch limit. Relying only on existing manifests was rejected because first-time issuers and newly published reports are invisible.

### Download only selected documents

The planner selects the latest active annual report as the default base, its correction when present, a newer semiannual report only for missing or time-sensitive facts, and specialist announcements only for governed event gaps. A PDF is downloaded only when a selected manifest lacks a verified local artifact. Hash-identical artifacts are reused; corrections and changed attachments create new immutable paths.

### Treat frequencies as layered maintenance, not repeated full rescans

- Daily during filing seasons: lightweight official index discovery.
- Weekly: bounded download, section selection, extraction, promotion, and machine retry.
- Monthly: manifest/archive/fact reconciliation and stalled-frontier detection.
- Semiannual: freshness evaluation after interim-report seasons; no forced full-company reprocessing.
- Annual: coverage reconciliation and rotating backfill for missing issuers/field families.

The same hash and freshness rules make every job incremental. Separate frequency names make capacity and alerts understandable without duplicating extraction implementations.

### Build counterparties from governed local identities

The production runtime will construct its resolver from active instrument/company identities and approved aliases valid at the knowledge cutoff. Only unique official identifiers, exact legal names, or approved exact aliases resolve automatically. Anonymous concentration assertions bypass entity resolution and produce concentration facts directly.

### Require transformation lineage for processor roles

Producer, logistics, storage, and trader roles may remain single-activity deterministic rules. A processor role requires explicit input and output linkage, or an equivalent governed transformation assertion, within the same issuer/segment/period. A standalone `processes` verb is insufficient for auto-promotion.

### Separate product identity, commodity identity, and price series identity

Catalog mappings will carry an explicit `commodity_id` distinct from `product_id` and `price_series_id`. Only one promoted current mapping may publish. Missing, ambiguous, stale, or unpromoted mappings become persisted machine-rework exceptions with stable identities and retry policy; they are not silently returned only in a run report.

### Audit archives before cleanup

An archive audit will classify files as manifest-active, manifest-superseded, hash-duplicate, unreferenced, hash-mismatched, or missing. Automatic cleanup is limited to reproducible caches and exact duplicates for which the canonical manifest reference and replacement path are proven. Unreferenced PDFs are quarantined or reported, not deleted automatically.

### Retain compatibility code until dependency proof exists

The deprecated `business_profile_llm.py` path and older OpenSpec changes will be marked for retirement only after import, documentation, scheduler, and replay dependency checks. Generated caches may be removed immediately; source tests and historical specifications are not redundant merely because production is disabled.

## Risks / Trade-offs

- [Risk] Daily index scans can still be rate-limited. -> Use source watermarks, bounded date windows, retry backoff, and no PDF download in the discovery job.
- [Risk] Enabling all issuers at once can exceed LLM capacity. -> Rotate bounded cohorts and prioritize changed annual reports, corrections, missing core fields, then lower-value enrichment.
- [Risk] Exact-only entity resolution leaves many named relationships unresolved. -> Persist machine rework, expand approved alias governance automatically from official identities, and reserve quick review for genuinely ambiguous high-value cases.
- [Risk] Explicit commodity ids require catalog migration. -> Add fields compatibly, reject missing production mappings, and keep candidate mappings non-executable until evidence promotion.
- [Risk] Archive duplicates may reflect different source payloads despite similar names. -> Compare full hashes and manifest lineage; do not delete based on names or announcement ids alone.

## Migration Plan

1. Fix anonymous relationship routing, processor-role gating, production resolver construction, and persistent publication-gap reporting with unit tests.
2. Add explicit commodity ids to catalog models and promote only a bounded evidence-backed starter mapping set after validation.
3. Add discovery-frontier storage and an index-only scheduler task; run in report mode and reconcile against active A-share coverage.
4. Enable weekly semantic processing for a bounded industry cohort with frozen identities/manifests, then expand by measured error and backlog gates.
5. Enable monthly and annual reconciliation after archive audit reports are stable.
6. Keep all switches reversible; rollback disables jobs and promotion while preserving manifests, artifacts, candidates, and audit history.

## Open Questions

- Which product-to-commodity mappings have sufficient existing official evidence to form the first promoted production cohort?
- Which local company-name and alias tables should be the canonical resolver source if instrument master and company profile disagree?
- What filing-season date windows and daily request budgets should be used for SSE, SZSE, and BSE after live rate-limit validation?
