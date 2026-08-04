## Context

The project already has source-neutral announcement acquisition, a metadata-only business-profile frontier, immutable PDF manifests, deterministic page selection, an asynchronous common LLM client, and a checkpointed semantic pipeline. Production operations currently expose these as separate discovery, semantic, monthly, semiannual, and annual scheduler jobs. The semantic `plan` stage can also discover and download a missing filing inline, so slow network and model work are not operationally isolated from fresh discovery.

The operating policy is now narrower: unattended production normally needs only the latest annual report available for each issuer as of the knowledge cutoff, including an active correction or replacement. Historical periods and exceptional document classes are explicit backfill scope. The system must remain local, bounded, auditable, disabled by default, and require almost no routine human intervention.

## Goals / Non-Goals

**Goals:**

- Preserve daily discovery even while download, parsing, or LLM work has a backlog.
- Persist every slow unit of work with idempotent identity, lease, retry, and terminal state.
- Reuse the existing archive, parser, semantic, promotion, and evidence contracts.
- Automatically coalesce an issuer to its latest active annual report and correction.
- Drain annual-report-season load through configurable budgets and resumable date windows.
- Provide one automatic daily entry point and one manual backfill entry point.

**Non-Goals:**

- Replacing the common announcement provider, PDF archive layout, semantic schemas, or approval rules.
- Automatically analyzing semiannual reports, specialist announcements, or prospectuses.
- Deleting old PDFs, manifests, facts, or point-in-time history.
- Running unbounded background processes outside the existing scheduler lifecycle.

## Decisions

### Use one durable document work item with explicit stage transitions

Add a SQLite-backed work table keyed by a deterministic hash of source-qualified announcement identity, instrument, requested policy, and processing identity. A work item moves through `acquire`, `parse`, `semantic`, and `publish`; each stage has `pending`, leased `running`, retryable, completed, superseded, or terminal-failure state, attempt count, lease expiry, next-attempt time, checkpoint path, and structured diagnostics.

Workers atomically claim a bounded batch using a short transaction, perform network or CPU work outside the transaction, then acknowledge or retry. Expired leases are reclaimable. A process-local task or an in-memory queue was rejected because scheduler restarts and annual-report bursts would lose work.

### Commit discovery before consuming downstream work

The daily entry point runs metadata discovery and persists frontier/window state first. It then enqueues eligible annual reports and spends independent time/count budgets on downstream queues. Downstream failure changes only work-item state and the daily report; it cannot roll back discovered frontier records or prevent the next daily discovery run.

For a market/date range that hits the page bound before completion, persist smaller child date windows and resume them on later runs. Canonical provider watermarks remain committed only after a complete scan. Fixed-page success was rejected because concentrated filing dates can silently truncate the market index; simply raising the cap was rejected because it offers no bounded recovery.

### Default automatic selection is latest active annual only

For each issuer and knowledge cutoff, enqueue only the newest eligible full annual-report period. Within that period, prefer the newest active correction/replacement and mark queued older originals or older annual periods superseded before download when they have not begun. Keep an already archived prior annual active until a newer report is actually available, and retain all immutable old artifacts for historical reads.

Semiannual reports and specialist disclosures are omitted from automatic plans regardless of field family. This intentionally accepts slower recognition of rare midyear business changes in exchange for predictable, normalized, low-cost production. The manual backfill entry point can explicitly opt into those document types.

### Reuse the semantic checkpoint as the stage implementation boundary

The asynchronous production service maps queue stages to existing governed capabilities: archive acquisition, PDF/page/section parsing and deterministic extraction, unresolved semantic extraction through the common asynchronous LLM gateway, then verification/promotion. Stage runners receive explicit work identity and checkpoint paths, and they are invoked with bounded concurrency through `asyncio` while blocking PDF/parser code stays in worker threads.

Existing synchronous compatibility APIs remain, but the scheduler uses only the asynchronous production service. Rewriting the mature extraction and promotion code was rejected because it would duplicate evidence and transactional governance.

### Manual backfill is scope expansion, not a second pipeline

The manual-only scheduler task accepts explicit instruments, dates/report years, document types, and force/reconcile flags. It may discover the requested historical window, enqueue matching frontier records, and use the same workers and retry policy as daily production. An empty broad historical request is rejected to prevent accidental full-market downloads.

### Backpressure changes consumption, never discovery correctness

Configuration supplies per-stage concurrency, item/time budgets, lease duration, retry limit/backoff, queue high-water marks, and filing-season multipliers. High backlog reduces or pauses new slow-stage claims but does not disable metadata discovery. Queue reports include depth and oldest age by stage/status, leased/retry/terminal counts, discovery-window backlog, throughput, and reasons for skipped or superseded work.

## Risks / Trade-offs

- [A single publication date can still exceed a bounded provider scan] -> Report an unsplittable window explicitly, retry with a configurable peak cap, and never label the window complete merely because a page limit was reached.
- [Scheduler process exits with leased work] -> Use short durable leases and reclaim only after expiry; all stage actions remain idempotent.
- [Correction lineage can be incomplete at discovery time] -> Coalesce by issuer/report period/publication time and re-evaluate before every claim; immutable manifests provide final correction lineage.
- [Existing semantic plan currently performs acquisition] -> Keep acquisition isolated in the `acquire` worker and run later stages only from the same checkpoint after successful acknowledgement.
- [LLM backlog may span many days] -> Deterministic parsing completes independently, semantic work is token/time bounded, and the daily report exposes oldest age and high-water status.
- [Latest-annual-only reduces freshness for rare midyear restructurings] -> Accept this as explicit policy; urgent cases use bounded manual specialist backfill.

## Migration Plan

1. Add the work table and indexes additively; initialize without enqueueing or changing approved data.
2. Add latest-annual enqueue, leasing, retry, supersession, queue reports, and split discovery-window state.
3. Add the daily and manual DataManager/scheduler entry points while keeping both disabled by default.
4. Replace old automatic business-profile scheduler entries with the new two-task configuration; leave legacy callable methods temporarily available for compatibility but unscheduled.
5. Validate in temporary databases with fake discovery/stage handlers, then run read-only reconciliation and rollout-gate checks.
6. Roll back operationally by disabling the daily task. Queue, frontier, manifests, immutable PDFs, and audit history remain recoverable and are not deleted.

## Open Questions

- The exact production concurrency and filing-season multiplier remain deployment settings to tune from observed provider limits and LLM throughput; correctness does not depend on those values.
