## 1. Correctness Baseline

- [x] 1.1 Add regression tests proving anonymous concentration assertions bypass named-entity resolution and persist operating facts
- [x] 1.2 Add transformation-link requirements to processor-role derivation and regression tests for standalone versus linked processing activities
- [x] 1.3 Build the production counterparty resolver from governed local identities and approved aliases, with fail-closed startup diagnostics
- [x] 1.4 Persist idempotent machine-rework exceptions for failed derived role and commodity publication inputs

## 2. Commodity Exposure Identity

- [x] 2.1 Add explicit commodity ids to product-to-market mapping models, configuration validation, and promotion evidence
- [x] 2.2 Update exposure publication to preserve separate product, commodity, and price-series identities
- [x] 2.3 Define and validate a bounded evidence-backed starter cohort of promoted commodity mappings
- [x] 2.4 Add as-of, supersession, ambiguity, stale-catalog, and candidate-leakage tests

## 3. Full-Market Disclosure Discovery

- [x] 3.1 Add persistent business-profile announcement frontier and scan-watermark storage
- [x] 3.2 Reuse the official announcement acquisition service to scan annual, semiannual, correction, and governed specialist events without PDF downloads
- [x] 3.3 Merge frontier changes, manifest gaps, field-family freshness, and due retries into semantic scope discovery
- [x] 3.4 Add rotating bounded cohort selection so issuers without manifests cannot starve behind a fixed batch limit

## 4. Minimum-Document Processing

- [x] 4.1 Enforce latest annual plus correction as the default disclosure base and add semiannual/specialist documents only for explicit gaps
- [x] 4.2 Verify hash-based local reuse and immutable naming across SSE, SZSE, and BSE report identities
- [x] 4.3 Add deterministic-first and selected-section LLM call metrics by field family
- [x] 4.4 Add tests proving unchanged complete issuers trigger no PDF download or LLM call

## 5. Automated Operations

- [x] 5.1 Add a filing-season daily index-only discovery scheduler task
- [x] 5.2 Harden weekly semantic maintenance configuration validation and production identities/manifests
- [x] 5.3 Add monthly manifest/archive/fact reconciliation and stalled-frontier reporting
- [x] 5.4 Add semiannual freshness and annual active-universe coverage reconciliation jobs
- [x] 5.5 Document enablement gates, budgets, retry policy, rollback, and Telegram progress reporting

## 6. Evidence-Safe Cleanup

- [x] 6.1 Add archive audit classification for active, superseded, duplicate, unreferenced, mismatched, and missing artifacts
- [x] 6.2 Prohibit automatic deletion when manifest schema is absent or references cannot be proven
- [x] 6.3 Remove reproducible Python/test caches and record the reclaimed space
- [x] 6.4 Audit legacy LLM compatibility code and overlapping OpenSpec changes, then retire only surfaces with no runtime, test, replay, or documentation dependency

## 7. Validation And Rollout

- [x] 7.1 Run focused unit tests, OpenSpec validation, and static checks
- [x] 7.2 Run a read-only production audit of active-universe coverage, manifest readiness, archive hashes, and empty-table blockers
- [x] 7.3 Run `codex review --uncommitted`, classify findings, and fix confirmed issues; use equivalent manual review if authentication remains invalid
- [x] 7.4 Commit and push only the changes created by this task, leaving pre-existing worktree content untouched
