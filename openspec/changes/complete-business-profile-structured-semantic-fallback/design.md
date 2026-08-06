## Context

Chapter-aware selection now narrows annual reports to the management-discussion chapter and successfully selects governed pages. The production batch nevertheless produced `ambiguous_table_layout` for 15 semantic attempts: structured field families only ran deterministic parsers, emitted zero evidence, and failed the stage-quality gate. Because the same checkpoint was retried without a semantic fallback, retries could not make progress.

The system already has selected-section artifacts, an evidence-constrained LLM gateway, deterministic record builders, immutable PDF/manifests, a durable queue, and a serialized SQLite writer. The fix must reuse those components, preserve deterministic authority, and keep LLM cost bounded.

## Goals / Non-Goals

**Goals:**

- Invoke the common LLM gateway only for selected structured sections that deterministic parsing classifies as ambiguous or unparseable.
- Produce locally validated segment and operating-fact candidates with immutable page/quote evidence.
- Let explicit expected non-disclosure finish without creating false zero facts.
- Make disabled semantic networking a blocked operational state instead of a retry-attempt consumer.
- Recover current affected retry work at the semantic stage without redownloading reports or repeating successful parse work.
- Expose fallback calls, accepted/rejected records, zero-output classifications, and blocked-network counts.

**Non-Goals:**

- Sending whole annual reports to the model.
- Replacing deterministic parsing for structurally usable tables.
- Automatically promoting model output without local evidence and numeric validation.
- Inferring undisclosed segment values, totals, units, directions, or commodity relationships.
- Broadening the active rollout to unrelated field families in this change.

## Decisions

### Use a dedicated structured semantic envelope

Add a narrow structured-extraction request/response contract for `structured_segments` and `tabular_operating_facts`. Inputs contain the field family, instrument/report identity, bounded selected snippets, allowed page numbers, and immutable quote hashes. Outputs contain typed rows plus exact evidence references; unknown values remain null.

Reusing the activity/relationship envelope was rejected because its action-object schema cannot express segment dimensions, revenue/cost/margin, operating metrics, units, totals, or reporting periods safely.

### Trigger only after deterministic ambiguity

The runtime first parses selected tables as today. If usable evidence-backed deterministic records exist, it skips the model. If record count is zero and the selected pages carry governed table signatures or parser diagnostics, it calls the structured semantic extractor once per selected document/field family. Pure narrative without a governed structured signal remains `expected_non_disclosure`.

This avoids paying LLM cost for easy tables and prevents keyword-only narrative from being hallucinated into numeric facts.

### Validate model rows locally before persistence

The extractor validates the model envelope atomically: every row must reference an allowed page and exact quote hash, typed numeric values must parse locally, percentages must be bounded, units/currencies must come from the selected text or remain unknown, and segment dimensions/metric names must use a closed local vocabulary. Unsupported or partially invalid batches fail closed as machine rework.

Accepted rows are converted through the existing record/evidence builders and written through the existing repository and single-writer coordinator. Deterministic and semantic derivation methods remain distinguishable in metadata.

### Separate blocked configuration from content retry

If `network_calls` is disabled or no LLM client exists when a structured fallback is required, the semantic stage returns a `blocked_configuration` quality classification. The async worker releases the item without incrementing its attempt count and reports the blocker. Gateway timeouts, schema failures, and unsupported output remain bounded retries because another attempt can change the result.

### Recover only affected semantic work

Add an idempotent recovery operation for semantic `retry_due` or terminal work whose last error is the extract quality gate and whose checkpoint proves selected pages plus `ambiguous_table_layout`/parser failure with zero evidence. Reset only to semantic, preserve the checkpoint, PDF, manifest, selected artifacts, attempt/recovery history, and never touch successful published work.

## Risks / Trade-offs

- [Model output misreads flattened tables] -> Require exact quote evidence, closed schemas, local numeric parsing, and fail the whole batch on unsupported rows.
- [LLM cost increases during filing season] -> One bounded call per ambiguous document/field family, deterministic-first routing, existing token/time budgets, and shared gateway concurrency.
- [Expected non-disclosure is mistaken for parser failure] -> Require governed table signatures or parser diagnostics before semantic fallback; otherwise record explicit non-disclosure without numeric zero facts.
- [Network kill switch causes a large blocked queue] -> Do not consume attempts; expose blocked counts and allow resume after configuration activation.
- [Existing retries already have attempt counts] -> Recover only positively identified affected work and preserve prior audit history.

## Migration Plan

1. Deploy schema, runtime, retry classification, telemetry, and focused tests with semantic networking still disabled.
2. Recover only the affected structured semantic retries without modifying PDFs or selected artifacts.
3. Enable bounded semantic networking and run one manual batch against the recovered items.
4. Verify nonzero LLM calls, accepted evidence-backed rows or explicit non-disclosure, no terminal failures, and no repeated unchanged quality-gate retries.
5. Enable continuous backfill only after the bounded batch passes; rollback by disabling semantic network calls while preserving resumable blocked work.

## Operator Runbook

- The `kill_switches.network_calls` flag is negative: `true` blocks the semantic gateway; keep it `false` for the bounded shadow rollout. `promotion` and `scope_widening` remain `false`.
- After deployment, run one normal `business_profile_backfill` batch. Recovery is automatic and only requeues semantic items whose checkpoint proves selected pages, zero evidence, and an affected structured-parser reason.
- Inspect `workers.semantic.quality` for `structured_fallback_calls`, accepted/rejected records, expected non-disclosures, and `blocked_configuration_reasons`. A blocked run is resumable and does not consume content attempts.
- Resume continuous processing only after a bounded batch has nonzero fallback calls or explicit non-disclosure completions, no repeated unchanged quality-gate retries, and no unexpected terminal failures. PDFs and selected-section artifacts are reused; no redownload is required.

## Open Questions

None. The existing common `semantic_extraction` gateway profile and SQLite write coordinator remain authoritative.
