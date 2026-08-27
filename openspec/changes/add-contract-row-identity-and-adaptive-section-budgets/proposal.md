## Why

Business-profile extraction currently collapses distinct rows from contract-performance tables when the rows share a product label. This can turn two valid disclosures, such as two separate polysilicon contracts with 4.18 and 0 billion yuan fulfilled amounts, into a false conflict. The section selector also uses a fixed 12-page context cap, which truncates genuinely long but relevant report sections and produces avoidable reselection warnings.

## What Changes

- Preserve contract/table-row identity in operating facts and prevent rows with the same product name from being merged without corroborating identity.
- Require the extraction response to carry row-level context and raw values; derive a stable programmatic row identity from immutable table evidence.
- Route only ambiguous duplicate/conflicting rows to a stronger reasoning model for targeted review, while retaining both rows when the ambiguity cannot be resolved.
- Replace the fixed 12-page selector limit with a chapter-aware adaptive page budget derived from section boundaries, table continuation pages, and field-family context.
- Enforce token, character, and document-level safety budgets while allowing a genuinely long section to be split into bounded windows.
- Add diagnostics and regression coverage for row identity, targeted ambiguity review, adaptive selection, and budget exhaustion.

## Capabilities

### New Capabilities

- `business-profile-contract-row-facts`: Contract/table-row identities, ambiguity detection, targeted stronger-model review, and safe persistence of distinct operating facts.
- `business-profile-adaptive-section-context`: Chapter-boundary-aware page selection and adaptive context windows with explicit safety budgets.

### Modified Capabilities

None.

## Impact

- Affects `research/business_profile_semantic_runtime.py`, `research/business_profile_semantic_extraction.py`, and `research/business_profile_section_selection.py`.
- Extends persisted operating-fact identity metadata and semantic diagnostics without deleting approved historical rows.
- Adds optional stronger-model routing through the existing LLM gateway; no new provider or persistence owner is introduced.
- Keeps `/run business_profile_backfill`, existing annual-report assets, API response compatibility, and single-writer publication semantics intact.
