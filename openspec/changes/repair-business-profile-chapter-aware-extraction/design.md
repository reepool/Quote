## Context

The current PDF artifact extractor produces reliable native page text, but the selector searches every page for aliases and table headers. Annual reports repeat those terms in the table of contents, financial notes, governance sections, and explanatory text. The selector therefore either selects unrelated pages or raises `selected page bound exhausted`; the semantic pipeline records a successful stage result even when its checkpoint remains `partial` and contains only machine rework.

The existing workflow already has durable annual-report manifests, immutable page artifacts, governed disclosure templates, deterministic table parsers, an LLM gateway, and durable work-item retries. This change must compose with those contracts and must not redownload valid reports or introduce a second persistence path.

## Goals / Non-Goals

**Goals:**

- Resolve the management-discussion chapter and its bounded page interval from the report table of contents, with a heading-based fallback.
- Select a small, ranked set of pages within that interval for each field family, preserving table-signature and heading evidence.
- Keep numeric/table extraction deterministic where the native text is structurally usable, and expose bounded narrative snippets to the existing LLM runtime only when semantic normalization is required.
- Prevent a durable work item from being acknowledged as complete when its stage result is partial, empty, or contains unresolved machine rework caused by a selector or parser failure.
- Recover the already completed evidence-free latest-annual work idempotently while reusing its verified annual-report asset.
- Report outline bounds, selected pages, deterministic rows, semantic snippets, machine rework, and effective published coverage.

**Non-Goals:**

- Replacing the existing official announcement discovery, frontier binding, annual-report archive, or source-manifest schema.
- Sending whole annual reports to an LLM or making LLM calls for straightforward headings, numbers, totals, or ratios.
- Treating an issuer's absence of a particular disclosure table as a factual zero; such cases remain explicitly unavailable or require a bounded semantic fallback.
- Adding a heavyweight PDF table-extraction dependency in this change.

## Decisions

### Use a reusable report-outline locator

Add a small outline module that scans only likely table-of-contents pages for major `第...节` entries, normalizes Chinese numerals and trailing page numbers, and returns the management-discussion start/end pages plus a confidence and fallback reason. If the TOC is absent or its page number cannot be mapped to the artifact, locate the actual `管理层讨论与分析` heading and the next major section heading. The result is passed to selection as an allowed page scope and is recorded in stage metrics.

This is preferred to hard-coding page numbers because listed-company templates vary while the major-section structure is comparatively stable. It is also preferred to a whole-document keyword search because repeated words outside the chapter are not evidence for business-profile extraction.

### Rank and cluster pages inside the chapter

Extend the selector with an optional page scope. Within the scope, score table-signature matches above generic aliases, count distinct matched rules, prefer non-TOC pages with substantive text, and cluster adjacent hits before adding context pages. Choose the highest-scoring clusters that fit `max_pages`; do not fail merely because the raw hit set is larger than the page budget. If no governed hit exists, return a structured selector-gap result so the runtime can invoke its bounded fallback rather than silently completing.

The selector remains deterministic and content-addressed. No page is selected outside the outline scope unless the outline locator reports a low-confidence fallback, in which case the existing full-document behavior is retained with a stricter candidate cap and telemetry.

### Keep deterministic parsing before semantic fallback

The selected sections continue through the existing deterministic table parser and unit/total validators. Narrative paragraphs and tables whose columns cannot be normalized are converted into bounded snippets containing page, section, and quote hashes, then passed to the existing LLM client under the current schema and evidence requirements. LLM output must cite one of those snippets; unsupported or uncited output remains machine rework.

This keeps the common path fast and reproducible, while allowing issuer-specific wording and product/application relationships to be normalized without reading an entire report.

### Add an explicit stage-quality contract

The semantic pipeline will expose a quality summary alongside its legacy `status`: `pipeline_status`, selected-document/page counts, deterministic record counts, semantic record counts, and unresolved machine-rework reasons. The async stage runner will only acknowledge a stage when its quality contract is satisfied. A stage result with `partial`, `stopped`, selector/parser machine rework, or zero usable output where the stage requires output is returned as retryable work; promotion-disabled shadow mode may remain `pipeline_status=partial` only when the current stage has valid evidence-backed output and no blocking rework.

This preserves the existing stage-by-stage checkpoint model while preventing a queue status from being confused with effective business-profile publication.

### Recover by evidence-free completion evidence

Extend the existing repository recovery operation to select only completed latest-annual work whose stage results show selector/parser machine rework or zero selected/evidence/semantic records and whose bound manifest is still valid. Recovery resets the item to the earliest affected stage, preserves its prior checkpoint and recovery history, and never deletes or redownloads the annual-report asset. A second run must return zero additional recoveries.

## Risks / Trade-offs

- [Some reports have no usable TOC or nonstandard section titles] → Use heading fallback, confidence telemetry, and a bounded full-document candidate fallback; do not claim high-confidence outline coverage.
- [Ranking may omit a rare but important table] → Preserve at least one cluster per matched field-family rule, use context-page expansion, and allow a targeted semantic retry when required evidence is absent.
- [Native PDF text loses table columns] → Keep deterministic parsing fail-closed, pass only the affected selected pages to semantic fallback, and retain page/quote evidence for validation.
- [Stricter acknowledgement increases retries] → Classify expected non-disclosure separately from selector/parser defects, use existing exponential/backoff limits, and expose queue reasons in the operator report.
- [Existing completed work has partial checkpoints] → Recovery is constrained to positive evidence, idempotent, and reuses verified manifests; no historical facts or PDFs are deleted.

## Migration Plan

1. Deploy the outline locator, scoped selector, quality contract, and focused tests.
2. Run the idempotent recovery against `data/research.db`; verify the 18 evidence-free items are requeued and valid assets remain reusable.
3. Run one bounded backfill batch and inspect outline bounds, selected pages, deterministic/semantic output counts, and machine-rework reasons.
4. Only after the bounded batch produces effective evidence-backed output should continuous mode be enabled.
5. Rollback is code-only; pending recovered work remains retryable and existing annual-report assets are untouched.

## Open Questions

None. The initial implementation will use the existing LLM gateway and current artifact schemas, adding only additive metrics and selector inputs.
