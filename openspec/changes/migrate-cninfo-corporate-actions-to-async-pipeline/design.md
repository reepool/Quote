## Context

The CNInfo company-action workflow already separates raw structured observations,
official announcement metadata, immutable document artifacts, page text, LLM analyses,
deterministic validation, review decisions, resolved evidence, and factor reconstruction.
Its evidence policy is intentionally conservative and remains valid. The runtime model,
however, is still predominantly event-serial: one event waits through title
classification, document retrieval/parsing, semantic extraction, semantic verification,
validation, and persistence before useful work begins for another event.

Provider calls can take two to five minutes. The useful local work between calls is much
shorter, so serial execution leaves CPU, network, and database capacity idle. Raising a
single title-classification semaphore does not solve full-body processing and can create
separate 50-request pools for title, extraction, and verification that exceed the
provider account limit.

Current unresolved inventory is on the order of hundreds of events and hundreds of
candidate announcement rows across more than 250 instruments. The pipeline must support
both the historical governance backlog and future incremental company-action updates.
CNInfo is used for SSE and SZSE company actions; BSE remains explicitly outside this
pipeline because CNInfo structured coverage is not considered supported for that market.

## Goals / Non-Goals

**Goals:**

- Continuously fill available work: while one instrument's title request waits for the
  model, prepare and submit other instruments; while extraction waits, continue title
  work, document retrieval, parsing, validation, and persistence.
- Share one default 50-call LLM budget across title classification, structured
  extraction, and semantic verification, under the common provider hard ceiling of 60.
- Keep PDF/OCR parsing at no more than eight concurrent workers and SQLite writes on one
  serial writer queue.
- Preserve official announcement, event, document, page, request, evidence, and factor
  lineage despite out-of-order completion.
- Automatically promote strongly evidenced, deterministically validated results; reserve
  manual review for genuine ambiguity, conflict, low-quality evidence, or unsupported
  semantics.
- Support resume, dry-run, aggregate reporting, and controlled retry without duplicate
  provider calls or duplicate database writes.

**Non-Goals:**

- Reimplementing CNInfo or exchange announcement transport. All discovery and attachment
  retrieval continue through `research.announcements`.
- Moving company-action prompts, output schemas, evidence gates, economic validation, or
  factor admission into `utils/llm`.
- Using title keywords as the authoritative semantic classifier or trying to enumerate
  every possible Chinese announcement wording.
- Backfilling BSE company actions from CNInfo or mixing TDX records into the independent
  CNInfo raw/official tables.
- Changing the existing production factor source before full-market validation and an
  explicit promotion decision.

## Decisions

### 1. Build a staged dataflow owned by the company-action domain

The company-action adapter composes common bounded stage runners into this logical flow:

```text
inventory
  -> search-window construction
  -> official announcement discovery
  -> per-instrument title bundle classification (LLM)
  -> selected attachment retrieval
  -> PDF text extraction / optional OCR
  -> event-context assembly
  -> structured semantic extraction (LLM)
  -> independent semantic verification (LLM when required)
  -> deterministic validation and auto-promotion decision
  -> serial persistence
  -> factor eligibility and aggregate report
```

Stages exchange immutable references and can progress independently. The business module
owns the graph and its payloads; `utils.llm` supplies only queues, leases, lifecycle, and
LLM admission.

Alternative considered: move the whole pipeline into `utils/llm`. Rejected because the
stage graph and evidence rules are specific to company actions and official disclosures.

### 2. Schedule by ready work rather than waiting by instrument

Inventory produces event work for many instruments. Title candidates for one instrument
and bounded search window are bundled up to the configured title limit. Once that LLM
request is submitted, the producer continues with the next instrument. When a title
result returns, selected documents enter retrieval immediately; no global title phase
barrier is required. Parsed evidence can similarly enter extraction while other
documents are downloading.

This realizes asynchronous overlap without allowing a response to mutate whichever
instrument happens to be current in a loop. Every callback receives and returns immutable
identity.

Alternative considered: complete all title classification before any document work.
Rejected because it creates long phase barriers and underuses local resources.

### 3. Use semantic title classification with deterministic identity filters only

Before the model, code may filter only by source identity, instrument, bounded date
window, duplicate announcement ID/hash, attachment availability, and explicit document
type constraints. It must not depend on an expanding keyword allowlist as the primary
relevance decision. The model receives all eligible titles for one bounded bundle and
returns one structured decision per source-qualified announcement ID, including
relevance, likely announcement role, confidence, and reason.

The title decision is recall-oriented and remains candidate selection, not evidence of a
company-action fact. The body must still be parsed and validated.

### 4. Share 50 LLM slots across every company-action semantic stage

Title classification, extraction, repair, and verification all use the common
provider/account coordinator. The business task requests a maximum aggregate bulk target
of 50, not 50 per stage or profile. Fairness weights may prevent title work from filling
the entire queue when body extraction or verification is ready. The common provider hard
ceiling remains 60 across this and other business jobs.

No stage holds an LLM lease while downloading, parsing, validating, or writing.

### 5. Bound document work separately and keep queue payloads small

Official metadata and attachments are retrieved only through `research.announcements`.
Download concurrency is separately configurable and paced by source/provider rules. PDF
text extraction and OCR share a CPU resource pool capped at eight. Queues carry
announcement IDs, artifact IDs, hashes, and page/section references; raw PDF bytes are
released after immutable artifact persistence and are not accumulated in the async
queues.

OCR remains opt-in. A low-quality native parse may route to OCR only when the task enables
it and the document meets bounded size/page rules.

### 6. Preserve the existing two-pass semantic and deterministic evidence model

The model owns semantic interpretation and returns the versioned company-action schema,
including date roles, economic primitives, exact quotes, page references, and conflicts.
Program code validates identity, quote existence, date evidence, unit normalization,
formula consistency, event-stage compatibility, conflict rules, and source artifact
lineage. It does not replace semantic extraction with a large wording parser.

Verification is conditional: already schema-valid extraction with complete, directly
quoted evidence may use the configured verification policy, while repairs or ambiguous
cases can require an independent second LLM pass. Policy is explicit and versioned.

### 7. Auto-promote validated results and isolate genuine manual work

With `auto_promote_validated=true`, an item is promoted only when the existing deterministic
gates classify it as a validated candidate with no unresolved conflict and sufficient
official evidence. Manual review is required for conflicting dates/economic terms,
low-quality OCR, incomplete evidence, unsupported event type, material mismatch, or
explicit model uncertainty. Merely being LLM-derived is not a reason for manual review.

Promotion writes resolved evidence; it never overwrites the original CNInfo observation.
Factor reconstruction continues to read only admitted resolved evidence.

Strict LLM response fields and deterministic validation diagnostics have separate
contracts. A governed review that would create resolved evidence validates the original
versioned response plus permitted corrections against the strict LLM schema. It may omit
only explicitly allowlisted deterministic diagnostic fields from that schema input;
validation then recomputes and retains those diagnostics in the reviewed audit result.
Any unknown non-internal public field still blocks resolution instead of being silently
discarded. Negative dispositions such as rejected, conflict, or manual-required remain
recordable for malformed analyses so operators can quarantine them without creating
resolved facts.

### 8. Serialize and batch idempotent SQLite persistence

All database mutations flow through one bounded writer queue and normally one consumer.
The writer may batch compatible rows but commits one event outcome atomically: artifact
lineage, analysis, validation, review/promotion, audit, and stage checkpoint must either
commit together as defined by existing transaction boundaries or remain resumable.

Workers never retain a database transaction while waiting for the model or document I/O.
The pipeline rechecks the source event key, announcement identity, artifact hash, input
hash, and latest supersession state immediately before commit.

### 9. Checkpoint terminal stage outcomes, not transient queue positions

Resume is based on durable stage outcomes and input hashes. It may reuse official
announcement metadata, immutable artifacts, page text, and successful LLM analyses when
their schema/prompt/input identities still match. It reruns work after schema/prompt
version changes, changed source artifacts, failed/incomplete outcomes, or explicit
operator override. Queue sequence and response arrival order are not persisted as truth.

Dry-run may perform explicitly requested read-only network/model/document work and report
would-write outcomes, but it must not create business facts, approvals, or factor rows.

### 10. Report aggregates and page problem details

Progress logs report totals and rates by stage, queue depth, active workers, LLM wait and
latency, parsing backlog, writer backlog, retries, and estimated remaining work. Telegram
receives summary messages. Detailed per-event windows, seen titles, candidates, rejected
items, and errors are stored/queryable or split into bounded problem messages only when
operator attention is needed.

### 11. Route residual outcomes by the repair that can change them

The inventory projection does not treat every non-promoted analysis as generic machine
rework. It inspects persisted semantic-verifier status, context diagnostics, event stage,
review reason codes, and deterministic gate results and emits one operational route:

- provider or incomplete verifier failures return to the failed-stage retry queue;
- omitted or truncated document context enters a document-context repair queue;
- proposal-only evidence returns to implementation-announcement discovery even when a
  proposal candidate is already stored;
- source-event conflicts enter the structured conflict queue;
- complete semantic results with missing date/economic support enter evidence-bound human
  review rather than repeated LLM extraction;
- fully validated candidates remain in quick review or auto-promotion.

Document-context repair is an executable, bounded stage rather than an inventory label.
It runs only the selected repair events, bypasses resume reuse for the prior incomplete
analysis, and builds a new prompt slice led by the prior omitted or truncated archived
sections. The repair input records its source analysis/input hash and archive coverage.
If one bounded repair still cannot cover the archive, the result moves to evidence-bound
review instead of looping through the same repair request indefinitely.

Automatic promotion continues to reject incomplete original model context. An authorized
quick reviewer may explicitly acknowledge archived context only after the review path
reloads the official pages for every cited announcement, confirms candidate metadata was
not omitted, reruns strict schema and deterministic evidence gates, and records the
acknowledgement in review audit lineage. This is an operator decision, not an implicit
relaxation of automatic promotion.

The review audit stores a deterministic, sorted lineage of every reloaded artifact and
page used by the review, including artifact/content/parser identity and page text hashes,
quality, and extraction metadata. Its SHA-256 is part of the review idempotency key, so a
changed archive cannot silently reuse a decision created against older evidence.

## Risks / Trade-offs

- [Fifty provider calls can amplify a prompt/schema bug] -> Require dry-run and staged
  live validation at 10, 25, then 50 with small event caps before full-market execution.
- [Out-of-order callbacks can cross-link evidence] -> Validate source-qualified event,
  announcement, artifact, page, request, and input hashes at every transition and again
  before commit.
- [Title bundles can exceed model context] -> Bound titles per request, use deterministic
  pagination with bundle identity, and require exactly one decision for every supplied ID.
- [Slow extraction can starve verification] -> Use workload/stage fairness inside the
  shared LLM budget and expose queue-age metrics.
- [PDF parsing can consume memory] -> Cap workers at eight, enforce artifact size/page
  limits, and pass references instead of bytes.
- [Serial writes can become the bottleneck] -> Use bounded batching and measure writer
  queue age; do not raise SQLite writer count without evidence and storage redesign.
- [Resume can reuse stale model output] -> Include prompt, schema, model policy, artifact,
  page-text, and normalized input hashes in cache eligibility.
- [Automatic promotion can accept a semantically wrong but self-consistent answer] ->
  retain exact official quotes, independent verification policy, deterministic conflict
  gates, audit sampling, and rollbackable resolved evidence.
- [Deterministic validation adds audit fields outside the strict LLM schema] -> Separate
  explicitly allowlisted derived diagnostics from schema input during governed review,
  recompute them after validation, and reject every unknown public field.
- [Residual routing can hide unresolved work behind terminal-looking labels] -> Every
  residual repair class remains factor-blocking until resolved or explicitly reviewed,
  and only existing reviewed, unsupported, or non-effective states are terminal.
- [Context acknowledgement can bypass a useful safety gate] -> Restrict it to explicit
  quick-review payloads, require complete candidate metadata and archived cited pages,
  rerun every deterministic gate, and retain the acknowledgement in the review payload.

## Migration Plan

1. Complete and integrate the common LLM orchestration capability behind existing direct
   call compatibility.
2. Introduce pipeline configuration with conservative defaults and keep the serial path
   available behind a temporary rollback switch.
3. Refactor current event processing into pure or narrowly stateful stage callbacks while
   preserving existing schemas, tables, and validation functions.
4. Add the serial writer/checkpoint path and prove idempotent resume with fake transports
   and out-of-order test results.
5. Run one-instrument and ten-event dry-runs, then live concurrency 10, 25, and 50 while
   checking identity, 429/error rate, memory, file descriptors, and database locks.
6. Run the unresolved historical inventory in bounded batches, review aggregate problem
   classes, and retry only remediable evidence/network failures.
7. Enable the same pipeline for future SSE/SZSE incremental company-action events after
   backlog acceptance; keep BSE excluded from CNInfo routing.

Rollback stops new admission, drains or cancels in-flight work, retains committed stage
outcomes for resume, and re-enables the serial business path. No raw observation or
resolved evidence is deleted by rollback.

## Open Questions

- The initial document download concurrency should be selected from live CNInfo/provider
  pacing evidence; it remains independently configurable rather than copied from the LLM
  limit.
- Whether every validated extraction needs a second LLM verification pass or only risk
  classes should be decided from measured false-positive/false-negative results during
  staged rollout.
- The long-term operator UI for manual review is outside this change, but the pipeline
  must expose enough structured problem data for a future high-throughput review screen.
