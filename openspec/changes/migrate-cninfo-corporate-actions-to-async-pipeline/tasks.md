## 1. Baseline And Configuration

- [x] 1.1 Record the current serial workflow's stage counts, latency, LLM calls, cached artifacts, unresolved outcomes, and database write behavior for a fixed regression sample.
- [x] 1.2 Add company-action pipeline configuration for stage queue sizes, title bundle size, download workers/pacing, PDF/OCR workers capped at eight, aggregate LLM target defaulting to 50, writer batch size, progress interval, and serial rollback mode.
- [x] 1.3 Enforce SSE/SZSE CNInfo routing and explicit BSE unsupported/skipped reporting without TDX supplementation into CNInfo tables.

## 2. Stage Contracts And Identity

- [x] 2.1 Define immutable typed payloads for inventory, discovery, title bundle, selected announcement, artifact, parsed pages, extraction case, verification case, validation outcome, and persistence command.
- [x] 2.2 Carry and validate instrument, `source_event_key`, source-qualified announcement ID, artifact/page hashes, run/stage identity, prompt/schema version, request ID, request/input hash, attempt, and idempotency key through every payload.
- [x] 2.3 Refactor existing company-action functions into stage callbacks that do not rely on mutable current-instrument/event loop state.
- [x] 2.4 Add a current-supersession and identity recheck before any analysis, review, resolved evidence, or checkpoint commit.

## 3. Inventory, Discovery, And Title Classification

- [x] 3.1 Build bounded inventory and search-window producers that reuse existing governance rules and emit classified outcomes for contradictory or insufficient anchors.
- [x] 3.2 Route announcement discovery and attachment retrieval exclusively through `research.announcements` with the company-action purpose, source-qualified audit, and existing failure semantics.
- [x] 3.3 Build per-instrument/search-window title bundles with only deterministic identity/date/duplicate/attachment/document-type filtering and no authoritative keyword allowlist.
- [x] 3.4 Update the LLM title schema and validator to require exactly one decision per supplied announcement ID with relevance, role, confidence, and reason.
- [x] 3.5 Submit additional instrument bundles while earlier title calls wait and immediately route completed selections to document preparation without a global phase barrier.

## 4. Document Preparation

- [x] 4.1 Add a separately bounded and source-paced official attachment retrieval stage that reuses immutable artifacts by announcement identity and content hash.
- [x] 4.2 Add native PDF text extraction workers under the common CPU resource pool with at most eight active parses and bounded size/page diagnostics.
- [x] 4.3 Add optional OCR routing only for enabled tasks and insufficient native text, sharing the same eight-worker parse budget.
- [x] 4.4 Pass artifact/page references between queues and release raw document bytes after immutable artifact persistence.
- [x] 4.5 Add idempotent page-text reuse keyed by artifact hash, extraction method/version, page number, and text hash.

## 5. Semantic Extraction And Verification

- [x] 5.1 Adapt title classification, company-action extraction, schema repair, and semantic verification calls to the common provider/account coordinator with one aggregate 50-call target rather than per-stage limits.
- [x] 5.2 Preserve the current versioned output schema for typed date roles, economic primitives, exact quotes, page evidence, conflicts, and uncertainty while removing any semantic dependency on expanding phrase lists.
- [x] 5.3 Implement stage fairness so title, extraction, and verification queues all progress under sustained load.
- [x] 5.4 Define and test the policy for mandatory versus conditional second-pass verification, retaining independent evidence checks for risk classes.
- [x] 5.5 Ensure no LLM lease is retained during document I/O, parsing, deterministic validation, or persistence.

## 6. Validation, Promotion, And Persistence

- [x] 6.1 Reuse and consolidate deterministic identity, exact-quote, date-role, event-stage, unit/formula, conflict, document-quality, and source-lineage gates without introducing case-specific stock/date exceptions.
- [x] 6.2 Route validated candidates to auto-promotion when enabled and route only conflicts, unsupported semantics, low-quality evidence, uncertainty, and configured audit samples to manual review.
- [x] 6.3 Implement one bounded serial SQLite writer queue with optional compatible batching and event-level atomic transaction/checkpoint boundaries.
- [x] 6.4 Keep raw CNInfo observations immutable and preserve existing resolved-evidence and factor-admission rules.
- [x] 6.5 Make write failures rollback cleanly and leave the item unacknowledged and resumable.
- [x] 6.6 Separate strict LLM response fields from explicitly allowlisted deterministic validation diagnostics during resolved review, preserve recomputed diagnostics in audit output, block unknown public fields from promotion, and keep negative dispositions recordable.
- [x] 6.7 Route residual analyses by provider retry, document-context repair, implementation rediscovery, source-event conflict, or evidence-bound review, and support explicit quick-review acknowledgement after complete archived-page reload.

## 7. Resume, Dry-Run, And Reporting

- [x] 7.1 Implement stage resume/cache decisions based on committed terminal outcomes plus event, artifact/page, prompt, schema, model-policy, and normalized input hashes.
- [x] 7.2 Ensure changed versions, changed artifacts, failed/incomplete outcomes, and operator override rerun the required stages while unchanged successful work is reused.
- [x] 7.3 Preserve dry-run as no business-fact/checkpoint/factor writes while allowing explicitly requested read-only source, document, and LLM validation.
- [x] 7.4 Add periodic aggregate logs for queue depth, active workers, provider wait/execution, download/parse throughput, writer backlog, retries, promotions, manual outcomes, and remaining work.
- [x] 7.5 Keep Telegram output aggregated for large jobs and split only bounded problem summaries; expose per-event windows, titles, candidates, rejections, and errors through queryable records/report paging.
- [x] 7.6 Return partial whenever terminal failures, conflicts, evidence-unavailable items, or manual-required items remain.

## 8. Verification And Rollout

- [x] 8.1 Add offline unit tests for bounded queues, out-of-order completion, cross-instrument identity, incomplete title bundles, duplicate artifacts, parsing cap, serial writes, cancellation, and shutdown.
- [x] 8.2 Add resume/idempotency tests covering committed success, transaction rollback, changed prompt/schema/artifact hashes, and duplicate task submission.
- [x] 8.3 Add business regression tests proving asynchronous results preserve existing evidence gates, auto-promotion policy, immutable observations, and factor eligibility.
- [x] 8.4 Run one-event and ten-event dry-runs, compare outcomes to the fixed serial baseline, and inspect detailed event diagnostics.
  - 2026-07-24 through 2026-07-28: one-event targeted governance/resolution reruns and repeated
    9/10/20-event batches confirmed resume identity, evidence routing, and writer behavior.
- [x] 8.5 Run live batches at aggregate LLM concurrency 10, 25, and 50, recording provider errors, latency, memory, file descriptors, parse workers, writer queue/locks, identity correctness, and output completeness at each gate.
  - 2026-07-22: the 50-way historical run was rejected as unstable after sustained 429/503/transport failures. Keep 50 as a stress-test ceiling, use 15 as the CNInfo body-analysis default, and require provider-wide adaptive cooldown/downshift before the next resumable batch.
  - Subsequent 20/30/50 configured batches exercised the shared adaptive controller; the accepted
    rollout criterion is observed downshift/recovery rather than forcing a fixed 25-way plateau.
- [x] 8.6 Process the historical unresolved SSE/SZSE inventory in bounded resumable batches, classify residual problems, and retry only remediable network/evidence failures.
  - 2026-07-31: historical CNInfo factor blockers reached zero after archive-unavailable,
    pre-listing, non-effective, asymmetric, and operator-attested classes were governed explicitly.
- [x] 8.7 Enable future incremental SSE/SZSE company-action events on the pipeline after acceptance and retain the serial rollback switch until stable operation is confirmed.
  - The daily sync now routes only structured anomalies and special-event keywords into the governed
    semantic pipeline; ordinary complete structured events continue through deterministic processing.
- [x] 8.8 Add governed-review regressions proving supported deterministic diagnostics do not block auto-promotion, unknown public fields still block resolution, and malformed analyses remain eligible for negative dispositions.
