## 1. Report Outline And Scoped Selection

- [x] 1.1 Add a reusable annual-report outline locator that parses management-discussion boundaries from the table of contents and falls back to verified major headings.
- [x] 1.2 Extend section selection to accept an outline page scope, rank table signatures and substantive headings, cluster adjacent hits, and fit selected context windows within the page budget.
- [x] 1.3 Record outline source/confidence and selected-page diagnostics in additive stage metrics without changing existing artifact identity contracts.

## 2. Structured And Semantic Extraction

- [x] 2.1 Apply chapter-scoped selection to the semantic runtime and preserve deterministic table parsing and unit/total validation for selected native-text sections.
- [x] 2.2 Add bounded semantic fallback inputs for selected narrative/table snippets and require page/quote evidence for accepted outputs.
- [x] 2.3 Distinguish expected non-disclosure from selector/parser machine rework and expose deterministic rows, semantic records, snippets, and unresolved reasons in stage results.

## 3. Durable Quality Gates And Recovery

- [x] 3.1 Add a stage-quality contract so partial, evidence-free, or blocking-machine-rework results remain retryable and cannot be acknowledged as completed work.
- [x] 3.2 Add idempotent recovery for completed latest-annual work with valid manifests but selector/parser or evidence-free stage history, reusing existing assets and preserving audit history.
- [x] 3.3 Update backfill/control telemetry to separate asset coverage, selected-section coverage, semantic output, effective publication, and machine rework.

## 4. Verification And Production Rollout

- [x] 4.1 Add focused tests for TOC and heading fallback, chapter scoping, ranking/page budgets, deterministic table parsing, bounded semantic evidence, and quality gates.
- [x] 4.2 Add recovery idempotency and reporting tests, then run the existing business-profile async, selector, deterministic extraction, semantic runtime, archive, asset, and backfill-control suites.
- [x] 4.3 Validate the OpenSpec change, compile modified modules, run equivalent review, execute the narrowly scoped production recovery, and verify that existing annual-report assets are reused.
