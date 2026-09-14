## 1. Contracts and baseline

- [x] 1.1 Record the current extraction, verify, publish, retry, and queue schemas; define compatible identities for report-level batches.
- [x] 1.2 Add structured metrics for request count, tokens, admission wait, provider execution, retry/failover, and per-stage durations.
- [x] 1.3 Create the versioned offline evaluation fixture layout and seed representative section artifacts/labels for seven industries.
- [x] 1.4 Add mocked-gateway tests that establish the old-run regression baseline and the new request-count/publication metrics.

## 2. Shared gateway and durable scheduling

- [x] 2.1 Move business-profile LLM calls onto one long-lived gateway runtime and remove per-report event-loop ownership.
- [x] 2.2 Enforce one global in-flight limit with no nested report or record LLM pools.
- [x] 2.3 Add separate short admission and provider execution deadlines; convert admission exhaustion to durable `retry_due`.
- [x] 2.4 Preserve bounded Retry-After backoff, cooldown, circuit breaker, weighted failover, and non-retry classification for auth/contract failures.
- [x] 2.5 Ensure worker budget expiry releases claims without cancelling durable in-flight work, and add startup recovery for stale items.

## 3. Validation and verification redesign

- [x] 3.1 Keep one chapter-aware joint extraction request per report/family bundle and persist raw JSON before interpretation.
- [x] 3.2 Implement program-owned validation for schema, issuer/period, evidence, units, numeric reconciliation, duplicates, conflicts, and catalog versions.
- [x] 3.3 Replace record-level verify fan-out and activity follow-up calls with one bounded report/family ambiguity batch.
- [x] 3.4 Implement the batch response schema, target-id matching, partial-result persistence, and resumable retry without repeating completed targets.
- [x] 3.5 Map deterministic and batch outcomes to `validated`, `verified`, `held`, and `rejected`; retain model probabilities only for diagnostics.
- [x] 3.6 Scope exception backlog/readiness metrics and recovery gates to instrument/report/family/processing identity.

## 4. Compatibility and rollout

- [x] 4.1 Reclassify existing per-record verify work items into compatible report-level batch identities without deleting evidence or published records.
- [x] 4.2 Add targeted replay for the ten-company sample and confirm one extraction plus zero/one batch verification request per report.
- [x] 4.3 Run the offline evaluation corpus and compare semantic correctness, held rates, tokens, latency, retries, failovers, and 429 behavior.
- [x] 4.4 Enable the new scheduler/verification path by configuration; rollback remains a configuration switch that disables batch verification while retaining persisted extraction.
- [x] 4.5 Run the full focused runtime suite and final review after updating legacy verifier test doubles to the batch contract; unrelated pre-existing worktree changes remain untouched.
