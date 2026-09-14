## Why

The first chapter-aware production batch selected the correct management-discussion pages, but 15 of 20 semantic attempts were retried because ambiguous structured tables produced no deterministic records or evidence. The structured field-family branches never invoked the bounded LLM gateway, so retries could not change the result and continuous backfill would eventually convert recoverable documents into terminal failures.

## What Changes

- Add bounded LLM extraction for `structured_segments` and `tabular_operating_facts` only when selected chapter pages contain an ambiguous or unparseable governed table.
- Require structured LLM outputs to cite an allowed selected-page quote and pass local schema, unit, numeric, and evidence validation before persistence.
- Keep deterministic rows authoritative and avoid LLM calls when deterministic parsing already produced usable records.
- Treat explicit expected non-disclosure as an evidence-backed empty completion, while parser ambiguity, disabled networking, gateway failures, and unsupported model output remain separately classified machine rework.
- Prevent retries from consuming attempts when the semantic network kill switch is deliberately enabled, and expose actionable fallback and retry telemetry.
- Requeue only affected semantic retry work after deployment while preserving existing PDFs, manifests, selected-section artifacts, and prior audit history.

## Capabilities

### New Capabilities

- `business-profile-structured-semantic-fallback`: Bounded, evidence-backed semantic recovery for ambiguous annual-report segment and operating-fact tables.

### Modified Capabilities

None.

## Impact

- Affects the business-profile semantic extraction schema/runtime, async stage retry policy, production telemetry, recovery behavior, configuration, and focused tests.
- Reuses the existing common LLM gateway, selected-section artifacts, immutable annual-report archive, SQLite single-writer coordinator, and current candidate persistence contracts.
- Does not send full PDFs to the LLM, automatically widen the document scope, promote unsupported facts, redownload annual reports, or require destructive database migration.
