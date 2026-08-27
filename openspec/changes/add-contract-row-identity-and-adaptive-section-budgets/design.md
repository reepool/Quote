## Context

The structured semantic path already preserves source-native values and immutable evidence, but operating facts created from multiple rows can still share an overly broad `fact_scope`. Contract-performance tables commonly repeat a product label while representing different contracts. The selector also receives a bounded annual-report outline but caps every family at `min(12, max_pages)`, so a legitimate section can exceed the cap even when the report provides a precise chapter boundary.

The change must use the existing semantic extractor, LLM gateway, selected-section store, and single publication owner. It must remain compatible with existing rows and allow old rows to be replayed without deleting approved history.

## Goals / Non-Goals

**Goals:**

- Make table-row and contract identity explicit in structured operating facts.
- Detect ambiguous same-label rows before publication and invoke a stronger reasoning profile only for those rows.
- Preserve all source-supported rows when ambiguity remains; never silently choose one amount.
- Derive selection budgets from the bounded report outline and table continuity while retaining character, token, and total-document guards.
- Split oversized sections into deterministic windows with diagnostics that explain the budget decision.
- Keep the existing LLM gateway, retry policy, evidence validation, and API contracts compatible.

**Non-Goals:**

- Rebuild the PDF parser or OCR subsystem.
- Ask the stronger model to calculate, normalize units, or replace exact evidence validation.
- Merge or delete already approved historical facts in place.
- Send an entire annual report to an LLM without chapter or window bounds.
- Introduce a new database, provider, or publication owner.

## Decisions

### Row identity is evidence-derived

Every structured operating row will carry a `source_row_key` derived from the selected table identity, physical page range, normalized row label, ordered raw cells, and row ordinal. When the source contains explicit contract identifiers, they are retained as `contract_reference_raw`; otherwise the row key remains the stable identity. The record id and fact scope include this identity, so equal product labels do not collide.

The deterministic table parser remains authoritative for rows it can read. Semantic responses may supply row context, but program code derives the durable key from immutable evidence rather than trusting a model-generated id.

### Ambiguity uses targeted stronger-model review

Rows are grouped only when their document, period, normalized subject, and fact type match. Multiple distinct row keys in one group are marked as `ambiguous_same_subject_rows` when a broad identity would otherwise conflict. A targeted review request contains the table rows and evidence spans and asks the configured stronger reasoning profile (default `gpt-5.6-terra`) to classify contract boundaries. It cannot alter numeric values or evidence ids.

If the review confirms separate contracts, rows remain separate. If it confirms a single row was duplicated, the program may deduplicate only when the evidence row keys and raw cells support that conclusion. If the review is unavailable, invalid, or inconclusive, all rows remain candidates and publication is blocked only for the ambiguous group, not unrelated facts.

### Adaptive context budget

The selector accepts an optional `page_budget` object. Its effective page allowance is derived in this order: the requested field-family budget, the chapter outline span, and the configured global maximum; the smallest non-null safety limit wins. The default no longer hard-codes 12 pages. The selector first keeps all high-value anchors and their context within the chapter, then expands across contiguous table continuation pages. If the chapter exceeds the effective allowance, it emits deterministic windows with `window_index`, `window_count`, and `budget_reason` rather than throwing a generic bound error.

Character and document token budgets remain authoritative. A window is never allowed to exceed those limits, and the runtime records the selected page count, chapter span, window count, and reason in stage diagnostics.

### Compatibility and replay

Rows written before this change receive a legacy-derived row key from their persisted table/evidence metadata when possible. If no reliable identity exists, they retain their existing record id and are not rewritten automatically. New extraction uses the expanded identity, and `result_policy=reuse` continues to prefer approved rows while allowing a new row identity for a genuinely distinct contract.

## Risks / Trade-offs

- [A report has no usable table row boundaries] -> Keep the existing evidence-backed record, mark row identity as `unresolved`, and route only conflicting groups to review.
- [The stronger model is slow or unavailable] -> Use the existing gateway retry/timeout contract; preserve rows as candidates and do not block unrelated field families.
- [Adaptive windows create multiple LLM requests] -> Split only oversized chapter scopes and reuse immutable selected-section artifacts; expose window metrics for tuning.
- [Legacy records lack row metadata] -> Do not guess identity or rewrite approved history; require a fresh extraction to create row-aware records.
- [A large chapter still exceeds global safety limits] -> Stop at the character/token/document budget with a typed diagnostic and recover from the next window/run.

## Migration Plan

1. Deploy code and configuration with the new schema fields optional for reads and required for newly generated structured rows.
2. Run focused unit tests and a canary containing the two polysilicon contract rows plus a normal segment table.
3. Replay affected candidate records with `force=true result_policy=reuse`; approved legacy rows remain intact.
4. Monitor ambiguity-review outcomes and adaptive-window metrics before increasing global budgets.

Rollback is code-only: disable targeted review and adaptive windows through the compatibility flags. Existing persisted rows remain readable because new fields are optional and no destructive migration is performed.

## Open Questions

- The stronger review profile should be configurable through the existing semantic policy rather than hard-coded; the initial default is `gpt-5.6-terra`.
- Exact per-family page and token ceilings should be tuned from the first canary metrics; the implementation will expose defaults and request overrides.
