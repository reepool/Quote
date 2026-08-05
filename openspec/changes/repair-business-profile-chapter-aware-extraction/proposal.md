## Why

The first bounded production batch proved that annual-report PDFs and manifests are being acquired, but the semantic pipeline still scans the whole PDF with broad aliases. On a representative 170-page annual report, repeated table headings and table-of-contents text expanded to 26 candidate pages, exceeded the 12-page selector budget, and produced `partial` results with zero facts while the durable queue marked the work complete.

The workflow must use the report's table of contents and heading hierarchy to narrow analysis to the management discussion and analysis chapter, then use deterministic table extraction and bounded semantic analysis on only the relevant sections. A partial or evidence-free pipeline must remain machine rework instead of being reported as published business-profile data.

## What Changes

- Add chapter-aware annual-report outline detection using the table of contents, section headings, and bounded fallback rules.
- Restrict business-profile section selection to relevant management-discussion subsections such as principal business, industry, business model, operating analysis, products, customers, and suppliers.
- Replace broad all-page keyword selection with ranked, clustered candidate pages that fit the page budget and preserve evidence lineage.
- Improve native-text table extraction for the selected pages and retain deterministic numeric validation; use LLM semantic extraction only on bounded narrative/table snippets when deterministic rules cannot normalize the content.
- Make stage acknowledgement fail closed for `partial`, empty-selection, unresolved machine-rework, or evidence-free results.
- Add idempotent recovery for the 18 already completed but evidence-free work items so their downloaded annual reports are reused without redownloading.
- Add truthful telemetry for chapter bounds, selected pages, deterministic records, LLM snippets, machine rework, and effective published coverage.

## Capabilities

### New Capabilities

- `business-profile-chapter-aware-extraction`: Chapter-scoped annual-report selection, structured extraction, bounded semantic fallback, and evidence-backed completion gates.

### Modified Capabilities

None.

## Impact

- Affects `research/business_profile_pdf_artifacts.py`, `research/business_profile_section_selection.py`, `research/business_profile_deterministic_extraction.py`, semantic runtime stage contracts, async work acknowledgement, recovery/control reporting, and focused unit tests.
- Reuses existing annual-report assets, source manifests, immutable archive paths, catalogs, and LLM gateway; no destructive database migration or PDF redownload is required.
- Existing valid manifests remain reusable. Only completed latest-annual work with `partial` or evidence-free stage results is eligible for recovery.
