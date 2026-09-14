## Why

Production backfill proved that the business-profile pipeline still treats LLM-generated semantic summaries as if they were verbatim source transcription. Valid semantic rows are rejected when normalized names, units, or multi-section conclusions do not literally occur in one evidence quote, while downstream conversion failures lose the original exception and make long-running work impossible to diagnose confidently.

## What Changes

- Separate semantic synthesis from evidence provenance: the LLM owns business meaning, normalization, summarization, role classification, and field assignment, while local code owns source identity and deterministic span binding.
- Remove literal-substring gates that require semantic names, products, counterparties, normalized units, or summarized values to be copied verbatim from a single quote.
- Preserve hard local validation for closed JSON schemas, supported enums, finite numeric types, issuer/report-period scope, bounded evidence references, and deterministic source lineage.
- Treat valid LLM semantic results as candidate facts and retain exact referenced annual-report text as independent evidence, including multi-span and multi-section evidence from the same source document.
- Persist bounded semantic result summaries, row-level acceptance decisions, downstream conversion errors, model identity, token usage, request/response hashes, and evidence references for both successful and failed work.
- Add INFO logs for lifecycle and aggregate production progress, plus DEBUG logs for bounded/redacted prompt context, structured LLM output, per-row validation, normalization, persistence, and exception details.
- Preserve completed field-family checkpoints, budget each field-family request independently, and prevent the same retryable work item from being reclaimed repeatedly within one batch run.
- Audit all business-profile LLM paths for equivalent verbatim-text assumptions and cover them with regression tests.

## Capabilities

### New Capabilities

- `business-profile-semantic-synthesis`: Governed LLM semantic synthesis, independent evidence provenance, detailed production observability, and field-family-resumable processing.

### Modified Capabilities


## Impact

- Business-profile semantic request/response schemas, prompts, local validation, evidence resolution, unit normalization, and runtime identities.
- Semantic run and exception metadata, checkpoint behavior, queue retry eligibility, stage metrics, and logging.
- Focused extraction, runtime, async-production, persistence, and end-to-end tests.
- Existing annual-report PDFs and selected-section artifacts remain reusable; no destructive data migration or credential change is required.
