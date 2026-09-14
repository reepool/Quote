## Context

The production path is an asynchronous annual-report pipeline: the shared announcement-asset module supplies the report, workers parse selected sections, a joint semantic bundle is extracted, records are checked, ambiguous records are verified, and accepted facts are published. The current implementation creates a runtime/event loop per report, exposes record-level verification fan-out, and uses a one-hour queue wait. In one observed run, five extraction calls were followed by 105 verification calls, 37 rate-limit attempts, 19 failovers, and verify tasks lasting about one hour.

The redesign must keep the user's automation-first goal, the existing reusable annual-report assets, SQLite's single-writer constraint, and resumability. It must not turn semantic work into a manual approval workflow.

## Goals / Non-Goals

**Goals:**

- Bound all provider requests with one global LLM admission counter.
- Keep report stages decoupled and durable so slow LLM work never blocks acquisition or parsing.
- Reduce a report to one joint extraction request plus zero or one bounded batch verification request.
- Apply deterministic checks to every record and use model output only as advisory semantic input for ambiguity resolution.
- Ensure retries, failover, and recovery are bounded, observable, and resumable.
- Provide a small offline evaluation corpus that measures semantic correctness and operational efficiency without network variability.

**Non-Goals:**

- No new manual production review queue.
- No requirement to call multiple LLMs for consensus on every record.
- No use of model probability as a publication gate.
- No replacement of the shared annual-report asset manager or public API contract.
- No live-network integration tests or speculative platform-wide validation framework.

## Decisions

### 1. One global LLM admission authority

The long-lived LLM gateway runtime owns the shared pool and provider coordinators. Report workers submit durable requests to it; they do not create event loops or nested semaphores. The configured global in-flight limit is the only concurrency limit counted against provider calls. Provider-specific limits remain inside the gateway and cannot multiply the report-level limit.

Alternatives considered: retaining per-report semaphores (causes hidden multiplicative concurrency); keeping one loop per report (risks cross-loop locks and inconsistent pool state).

### 2. Durable stage boundaries

Acquire and parse commit artifacts and enqueue extraction work. Extraction commits its raw structured response and enqueues deterministic validation. Validation commits a classification and enqueues at most one batch verification item for ambiguous records. Verification commits decisions and publication consumes them through the existing single writer. Admission timeout is short (60-90 seconds); provider execution deadline is separate (180-300 seconds). A timeout becomes `retry_due`, never a task that remains running for an hour.

Alternatives considered: waiting synchronously inside a report worker (causes queue starvation); cancelling all in-flight work at worker budget expiry (loses resumability).

### 3. Joint extraction and batched ambiguity verification

The report context builder selects relevant chapter sections and sends one Chinese-schema extraction request for the report's configured field families. Program checks run on all returned records. Only records with missing context, conflicting evidence, unresolved entities, unit problems, or extraction/check disagreement are placed in one bounded batch request. The batch response contains one result per target id with `supported`, `unsupported`, or `unclear`, failed aspects, and a Chinese reason. There is no second follow-up request for a single record.

Alternatives considered: one request per activity/relationship (too many requests and 429 exposure); treating all records as ambiguous (wastes tokens and hides deterministic defects).

### 4. Program-owned publication state

The program validates evidence references against persisted section/page/hash/quote data, validates issuer and report period, performs numeric/unit conversion and reconciliation, and detects duplicates/conflicts. It maps outcomes to `validated`, `verified`, `held`, or `rejected`. LLM confidence/probability is stored for diagnostics only and cannot promote a record that fails deterministic checks.

### 5. Scoped recovery and bounded retry

Exception backlog keys include instrument, report identity, field family, and processing identity. Only blocking exception tiers affect that work item's recovery gate. Retry classification distinguishes authentication and deterministic contract failures (no provider failover) from rate limits, transient transport errors, provider failures, and retryable parse/schema errors (bounded retry with `Retry-After`, cooldown, circuit breaker, and weighted failover). Exhausted work is durably marked for later replay.

### 6. Offline evaluation corpus

Store a small versioned set of already downloaded section artifacts with expected labels, evidence anchors, normalized values, and acceptable publication classifications. Tests use mocked gateway responses and measure extraction agreement, evidence support, held/rejected rates, request count, token budget, latency, and simulated 429 recovery. The corpus is regression data, not a production gate and not an ongoing manual review obligation.

## Risks / Trade-offs

- [Risk] A single batch prompt may exceed context limits for unusually large reports → cap section excerpts and batch size, persist overflow as another durable batch only when necessary.
- [Risk] Ambiguity classification may hold too many records → expose reason codes and evaluation metrics; tune deterministic rules and prompts from offline fixtures.
- [Risk] A shared gateway can become a single bottleneck → keep durable queues, bounded admission, provider failover, and health metrics; never bypass the authority with local calls.
- [Risk] Existing in-flight per-record verify items may use old identities → provide a compatibility reclassifier that consolidates them into the new report/family batch identity without deleting evidence.

## Migration Plan

1. Add contracts, metrics, and offline fixtures behind the existing stage identities.
2. Move gateway/event-loop ownership to the long-lived shared runtime and add short admission deadlines.
3. Enable deterministic classification and batched verification for a targeted set of reports; replay old verify items through the compatibility reclassifier.
4. Compare offline and targeted production metrics, then enable the default rollout.
5. Roll back by disabling the new batch verifier while retaining persisted extraction and deterministic results; no raw assets or published history are deleted.

## Open Questions

- Confirm the initial offline corpus report identifiers and expected label review ownership.
- Confirm whether provider execution deadlines should be 180 or 300 seconds after observing the first batch rollout.
