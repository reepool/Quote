## Why

CNInfo company-action governance now has title classification, official announcement
retrieval, immutable PDF/page artifacts, two-stage semantic analysis, deterministic
validation, auto-promotion, and factor gates, but the full document workflow still runs
one event at a time. Model calls take minutes, so most wall-clock time is idle waiting;
simply raising a profile limit cannot help because document preparation, database access,
and persistence remain inside the same sequential loop.

## What Changes

- Replace the sequential event loop with a bounded asynchronous pipeline that overlaps
  title classification, official document preparation, LLM extraction, semantic
  verification, deterministic validation, and persistence.
- Use the common LLM resource coordinator with a default 50 active LLM calls and the
  provider-wide hard limit of 60 shared across title, extraction, and verification work.
- Keep official announcement discovery and attachment retrieval in
  `research.announcements`; company-action code remains a consumer and does not duplicate
  CNInfo transport or move announcement logic under `utils/llm`.
- Bound document downloads separately, cap PDF/OCR parsing at 8 workers, buffer prepared
  event references rather than unbounded document bytes, and de-duplicate work by
  source-qualified announcement identity and content hash.
- Serialize SQLite persistence through a dedicated writer queue, optionally batching
  idempotent writes, so the task connection pool and SQLite single-writer behavior do not
  consume LLM worker slots.
- Preserve exact `source_event_key`, announcement IDs, request IDs, request/input hashes,
  artifact hashes, stage sequence, and retry lineage across out-of-order completion.
- Keep strict LLM response schema validation separate from explicitly allowlisted
  deterministic validation diagnostics during governed review, while preserving those
  diagnostics in stored audit output and rejecting unknown public fields.
- Add resume-safe stage checkpoints, aggregate Telegram reporting, problem-detail paging,
  queue/concurrency metrics, and staged 10/25/50 live validation.
- Classify residual validation outcomes into provider retry, document-context repair,
  implementation rediscovery, source-event conflict, evidence-bound human review, and
  validated quick review instead of collapsing every non-promoted analysis into one
  machine-rework queue.
- Allow an explicitly authorized quick reviewer to acknowledge complete archived context
  after the review path reloads official pages omitted only by prompt page limits;
  automatic promotion remains fail-closed on the original incomplete model context.
- Supersede the sequential assumptions in the earlier unresolved-date and title-only
  concurrency changes without changing raw CNInfo observations, evidence gates, factor
  admission, BSE policy, or production factor reads.

## Capabilities

### New Capabilities
- `cninfo-corporate-action-async-resolution-pipeline`: End-to-end bounded asynchronous
  orchestration for CNInfo title selection, official document preparation, semantic
  extraction and verification, governed resolution, persistence, resume, and reporting.

### Modified Capabilities

## Impact

- Affected code: `data_manager.py`, `data_sources/cninfo_announcement_title_llm.py`,
  `data_sources/cninfo_corporate_action_documents.py`,
  `data_sources/cninfo_corporate_action_llm.py`, scheduler tasks/configuration, database
  operations used by company-action artifacts/analyses/reviews, and focused tests.
- Existing dependency: announcement discovery and attachment retrieval continue through
  `research.announcements`; no direct CNInfo transport is added.
- Existing company-action schemas, evidence validation, auto-promotion policy, raw tables,
  resolved evidence, factor reconstruction, and APIs remain authoritative.
- The earlier `parallelize-cninfo-title-classification` change becomes an implementation
  subset of this pipeline; its identity and coverage validation requirements are retained.
