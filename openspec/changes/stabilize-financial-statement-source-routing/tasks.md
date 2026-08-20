## 1. CNInfo Outcome And Canonical Semantics

- [x] 1.1 Extend official validation results with separate transport/structured-payload, parsed-fact, persisted-fact, strict-ready, missing-fact, and diagnostic counts while preserving failed instrument-period details.
- [x] 1.2 Pass profile-specific production canonical required facts from maintenance into CNInfo batch validation, replacing the validator's unrelated legacy default list.
- [x] 1.3 Preserve CNInfo `equity_total` as a distinct fact and add a same-context `equity_total - minority_equity` derivation for `equity_parent` only when both inputs are present and numeric; retain explicit derivation lineage.
- [x] 1.4 Update repair routing to use strict local readiness for fallback selection while treating successfully parsed official facts as partial success rather than source transport failure.
- [x] 1.5 Add focused parser, validator, and repair-router tests covering partial CNInfo facts, strict equity semantics, valid derivation, and unresolved parent-equity fallback.

## 2. Sina Fallback Reliability

- [x] 2.1 Implement a narrow Sina financial-report request adapter using the existing endpoint and project HTTP session with configured timeout, interval, bounded retries, backoff, status/content-type checks, and compact response-prefix diagnostics.
- [x] 2.2 Keep the existing statement fallback loop and source-native DataFrame semantics, including report-period normalization and next-interface fallback after final Sina failure.
- [x] 2.3 Add unit tests for valid JSON, empty/malformed response, retryable HTTP response, timeout, and diagnostic redaction without making network calls.

## 3. Reporting And Verification

- [x] 3.1 Update financial source-routing summaries and Telegram text to distinguish CNInfo requests, parsed/persisted official facts, strict-ready targets, missing/ambiguous canonical facts, fallback-required targets, fallback successes, and unresolved blockers.
- [x] 3.2 Update scheduler and incremental-sync regression tests for the new source-routing fields and ensure unresolved failures still produce degraded/partial status.
- [x] 3.3 Run focused unit tests, an isolated live CNInfo validation for bank/non-bank samples, and a bounded Sina smoke test; record representative results in the implementation review.
- [x] 3.4 Review the diff against the pre-existing dirty worktree, run the relevant static checks, and verify no scheduler identity, schema, public API, or canonical-key regression.
