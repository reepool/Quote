## Why

Business-profile LLM extraction currently asks the model to reproduce exact quotes and calculate section-local character offsets. Production results show that semantically useful rows are systematically rejected for coordinate errors, even though coordinate calculation is deterministic work that should not consume model judgment.

## What Changes

- Generate bounded, stable evidence spans from selected annual-report sections before an LLM request.
- **BREAKING** Replace model-supplied evidence quotes, page numbers, and character offsets with one or more stable `evidence_span_id` references in business-profile extraction schemas.
- Keep semantic field extraction, normalization, summarization, and evidence selection in the LLM response.
- Resolve span references locally into exact source text, page, offsets, hashes, and governed evidence records.
- Reject unknown, ambiguous, truncated, or field-incompatible span references without weakening numeric, unit, issuer, period, or evidence gates.
- Version runtime identities so old offset-based checkpoints are superseded safely while immutable annual-report assets remain reusable.
- Report span-resolution outcomes and actual LLM request/row counts so long-running backfills expose actionable progress.

## Capabilities

### New Capabilities

- `business-profile-evidence-spans`: Stable, bounded evidence-span generation and deterministic evidence binding for LLM-assisted business-profile extraction.

### Modified Capabilities


## Impact

- `research/business_profile_semantic_extraction.py` request payloads, schemas, prompts, validation, and evidence normalization.
- Business-profile runtime identities, checkpoints, diagnostics, and async worker quality reporting.
- Focused unit and integration tests for structured rows, activities, relationships, invalid span references, request bounds, resume behavior, and metrics.
- No database schema migration, provider API change, credential change, or weakening of publication/promotion gates.
