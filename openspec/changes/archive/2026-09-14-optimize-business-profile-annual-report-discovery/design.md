## Context

Business-profile production already has source-neutral announcement providers, a metadata frontier, resumable date windows, durable stage queues, and immutable PDF manifests. The bootstrap caller currently omits the upstream category, uses a fixed multi-year market window, and routes a bounded incomplete CNInfo scan to providers declared ineligible for market scope. The frontier also links corrections only when an original was inserted first, although official indexes normally return newest announcements first.

Live probes on 2026-08-05 established stable official filters: CNInfo `category_ndbg_szsh`/`category_bndbg_szsh`, SSE `YEARLY`/`QUATER2` under `DQBG`, and SZSE `010301`/`010303` under `fixed_disc`. A March-April 2026 CNInfo annual filter reduced 227,584 unfiltered announcements to 11,170 categorized rows while preserving all three A-share markets.

## Goals / Non-Goals

**Goals:**

- Make initial and daily latest-annual discovery category-filtered, bounded, resumable, and observable.
- Preserve partial discoveries when a page bound is reached and repair coverage without rescanning full market history.
- Keep provider-specific category tokens inside providers while business callers use stable document families.
- Guarantee that a known corrected full report wins independently of discovery order and prevents unstarted original work.
- Retain strict local full-document classification, including BSE annual-report abbreviations.

**Non-Goals:**

- Automatically processing semiannual or specialist announcements in the default pipeline.
- Deleting an original PDF that was downloaded before a later correction existed.
- Replacing the common announcement service, frontier schema, work schema, archive layout, or semantic pipeline.
- Treating upstream category membership as sufficient evidence that an attachment is a full annual report.

## Decisions

### Use normalized periodic-report categories at the query boundary

Business-profile discovery sends `annual_report`. CNInfo, SSE, and SZSE providers translate that value to their official parameters and retain compatibility with existing raw CNInfo category tokens. This keeps source parameters out of production callers while allowing compatible exchange fallback and focused provider tests.

Passing `searchkey=年度报告` was rejected because it returns related notices and can miss official `年报` abbreviations. Relying only on local title filtering was rejected because it recreates the current full-index cost.

### Treat page-bound exhaustion as resumable, not fallback-worthy

When a provider returns records with `max_pages_exhausted` or its compatible legacy spelling, the acquisition service returns that partial result. Business-profile discovery persists its selected records and splits the date window. Fallback remains available for request failure, malformed payload, identity failure, or other configured statuses.

Falling back after a page bound was rejected because the service returns only one source result and can discard useful primary records. Raising the page cap alone was rejected because concentrated publication windows must remain bounded.

### Bootstrap the current filing season before targeted repair

An unscoped latest-annual backfill derives January 1 of the cutoff year as its initial market start. Daily discovery continues to use a short overlap window. After market discovery, a bounded rotating repair cohort queries only active issuers whose expected current annual period is absent from the frontier, using an instrument-scoped multi-year annual-category lookback. Rotation state uses the existing business-profile operation-state table, so no schema migration is needed.

Scanning every issuer first was rejected because it repeats thousands of negative identity and announcement requests. Retaining a fixed `2024-01-01` full-market bootstrap was rejected because it downloads index metadata for annual periods that latest-only selection will discard.

### Reconcile corrected full reports independently of arrival order

After every annual frontier upsert, the repository selects one active winner for the issuer, report period, and document family. Any correction wins over an original; among corrections the newest publication wins. All other full reports are marked superseded and the winner records lineage to the immediately preceding version. Queue selection uses the same precedence and supersedes any older unstarted latest-annual work before acquisition.

Deleting an already archived original was rejected because immutable source evidence may predate knowledge of the correction. Allowing both known versions into acquire was rejected because it wastes network, parsing, and LLM capacity.

### Retain local fail-closed document classification

The title parser accepts `YYYY年报` and `YYYY年年报` as annual families, then applies the existing suffix, summary, translation, and related-notice exclusions. Category-filtered rows still pass through this classifier before entering the frontier.

## Risks / Trade-offs

- [An official category can omit or misclassify a filing] -> Reconcile against the active universe and run bounded instrument-scoped repair for missing expected periods.
- [A single publication day can exceed the page cap] -> Persist selected rows, keep the unsplittable day pending, expose it in telemetry, and never report the window complete.
- [A correction appears while the original is already running] -> Coalesce before claims in the normal discovery-first run; preserve any artifact already acquired and prevent later original stages when the item is still unstarted or retryable.
- [Exchange category contracts drift] -> Keep mappings configurable/provider-owned, raw payload fixtures, response-shape diagnostics, and targeted live probes outside unit tests.
- [Newly listed issuers may have no expected prior-year annual report] -> Record successful empty targeted scans and rotate rather than blocking the market scan or inventing coverage.

## Migration Plan

1. Add normalized provider category mappings and tests without changing default routes.
2. Enable annual-category business-profile discovery and page-bound fallback suppression.
3. Make frontier and queue correction precedence order-independent.
4. Change bootstrap configuration to current-filing-season derivation and enable bounded missing-company repair.
5. Validate in temporary SQLite databases, run focused unit suites, then run a metadata-only live probe before resuming production backfill.
6. Roll back by restoring the prior bootstrap start/category configuration; persisted frontier, work, manifests, and PDFs remain compatible.

## Open Questions

- BSE official category parameters remain less stable than CNInfo's BSE plate. CNInfo remains the BSE primary and targeted source until a separate live parity probe proves the BSE official category contract.
