## Context

Business-profile semantic extraction currently sends selected annual-report sections to the common LLM gateway and requires each model row to reproduce an exact quote, page number, section identifier, and section-local character offsets. Production responses often contain useful semantic extraction but invalid coordinates. Computing coordinates is deterministic text-processing work, while selecting relevant evidence and normalizing a disclosed fact are semantic tasks.

Selected-section artifacts already preserve immutable document identity, exact section text, normalized document offsets, page numbers, and hashes. Existing publication gates require exact evidence plus numeric, unit, issuer, and report-period support. The redesign must reuse those assets, remain bounded for bulk backfills, preserve resumability, and avoid a database migration.

## Goals / Non-Goals

**Goals:**

- Let the LLM extract, normalize, summarize, and select supporting evidence without asking it to calculate text coordinates.
- Generate deterministic, bounded evidence spans from selected annual-report sections before each request.
- Resolve model-selected span identifiers locally into the existing governed evidence shape.
- Preserve fail-closed evidence, numeric, unit, issuer, period, and promotion gates.
- Rotate runtime identities so offset-contract retries do not collide with span-contract retries.
- Expose safe span-resolution and LLM request/row diagnostics for long-running backfills.

**Non-Goals:**

- Do not let the LLM create governed identifiers, source coordinates, page numbers, quotes, or hashes.
- Do not accept paraphrased evidence, fuzzy quote matches, inferred numbers, units, issuers, or periods.
- Do not change the annual-report asset schema, database schema, common LLM transport, provider credentials, or publication tables.
- Do not reprocess successfully published facts unless the versioned processing identity requires it.

## Decisions

1. **Build a request-local deterministic evidence catalog.** Each selected section is split into bounded paragraph or table-row groups after whitespace normalization. Every catalog entry includes a stable `evidence_span_id`, `section_id`, `page_number`, exact normalized text, normalized document offsets, and hashes. IDs are derived from immutable document/section/range content rather than list position. Whole-page spans are avoided unless the page already fits the configured span bound. This keeps evidence inspectable and limits model input.

   Alternative considered: send raw sections and let the model quote text. This preserves the current coordinate failure mode and wastes output tokens.

2. **The LLM returns semantic fields plus one or more span IDs.** Atomic activities, relationships, structured segment rows, and operating facts replace model-supplied evidence objects with `evidence_span_ids`. Multiple IDs are allowed because a name, value, unit, and relationship direction can be disclosed across adjacent rows or paragraphs. The schema bounds the number of IDs, requires uniqueness, and does not expose coordinate fields.

   Alternative considered: return a single span ID. This is simpler but rejects valid facts whose label and numeric value are separated by table layout.

3. **Local resolution is exact and fail-closed.** The validator resolves every ID only against the catalog sent in that request, deduplicates references while preserving order, and converts each span into the existing evidence record. Unknown, malformed, duplicated, truncated, or field-incompatible references are rejected. There is no fuzzy matching of model text and no fallback to section-wide evidence.

   Alternative considered: search the section for a model summary or quote. This can bind a plausible paraphrase to the wrong occurrence and weakens auditability.

4. **Compatibility checks run across the resolved evidence set.** Existing numeric, unit, entity-name, relationship-direction, issuer, and report-period checks inspect the union of resolved exact span text. A row survives only if the required disclosed values are supported by its referenced spans. Valid rows from a mixed response remain accepted; invalid rows retain bounded rework diagnostics.

5. **Span construction and request budgeting are coupled.** The catalog builder applies deterministic maximum span length, overlap/context, spans per section, sections per request, and total character limits. A span that would be cut through a paragraph or table row is omitted or split at a deterministic safe boundary and marked locally; truncated identifiers are never offered to the model. Request payload accounting includes span metadata and text rather than only raw section text.

6. **Processing identities are versioned, assets are not migrated.** Extraction schema and prompt versions advance to the span contract, and the existing runtime identity derivation incorporates those versions. Existing annual-report PDFs and selected-section artifacts remain reusable. Old retry checkpoints are naturally superseded by the new identity; published data and immutable assets are not deleted.

7. **Diagnostics distinguish transport, model, and local binding outcomes.** Safe audit metadata records catalog size, referenced/resolved span counts, accepted/rejected row counts, stable rejection codes, actual LLM request count, and provider usage. Raw prompts, raw model responses, full filing text, credentials, and unbounded quotes are not persisted in diagnostics.

## Risks / Trade-offs

- [A fact crosses a span boundary] -> Prefer paragraph/table-row grouping, include deterministic adjacent context, allow multiple span IDs, and route unsupported rows to machine rework rather than guessing.
- [More span metadata reduces available filing text] -> Use compact field names in the request catalog, enforce request-character accounting, and prioritize already selected high-value sections.
- [The same text appears multiple times] -> Bind IDs to immutable section and document ranges, so identical text at different locations remains unambiguous.
- [Providers return stale offset-shaped JSON] -> The closed response schema rejects coordinate fields and records a schema failure for resumable retry.
- [Version rotation increases queued work] -> Reuse downloaded annual reports and selected-section artifacts; rotate only semantic processing identities.
- [Evidence selected by the model is semantically irrelevant] -> Keep local field-specific compatibility checks and independent downstream verification/promotion gates.

## Migration Plan

1. Deploy span catalog construction, span-based response schemas, local binding, version bumps, diagnostics, and focused tests together.
2. Resume retryable semantic work. New processing identities use existing annual-report and selected-section assets but do not reuse offset-contract semantic checkpoints.
3. Observe rejection codes, accepted-row ratios, token usage, and queue progress during a single-batch run before continuous backfill.
4. Roll back by restoring the prior code versions. Existing published facts and annual-report assets remain compatible; span-contract checkpoints become inactive through identity versioning.

## Open Questions

None for implementation. Span-size tuning remains an operational optimization after production metrics are available and does not change the response contract.
