## Context

The current business-profile extractor correctly stopped asking the LLM to calculate source offsets, but its local validator still requires semantic labels, normalized units, values, products, and counterparties to appear verbatim in one merged quote. A production batch rejected most otherwise structured responses, exhausted a company-wide token budget while retrying field families, and persisted only coarse `context_incomplete` errors. Exact annual-report text remains essential for provenance, but it must not be confused with the semantic conclusion produced from that text.

## Goals / Non-Goals

**Goals:**

- Make LLM semantic synthesis the authoritative source for candidate business meaning and field assignment.
- Keep deterministic source identity, report scope, evidence-span resolution, schema/type constraints, and immutable evidence lineage.
- Accept supported multi-span and multi-section conclusions from one source document without fabricating a single contiguous quote.
- Persist and log enough bounded, redacted detail to inspect what the LLM concluded, what local code accepted, and where later conversion or persistence failed.
- Resume at field-family granularity, bound each LLM request independently, and avoid duplicate retries within one worker invocation.
- Preserve conservative production promotion: semantic results remain candidates until existing rollout manifests permit publication.

**Non-Goals:**

- Do not let the LLM create source coordinates, document identities, hashes, database identifiers, or production approval decisions.
- Do not log credentials, provider headers, full annual reports, or unbounded prompt content.
- Do not require a destructive database migration or redownload immutable annual-report assets.
- Do not weaken closed-schema, finite-number, issuer, report-period, catalog, temporal, or candidate-only promotion controls.

## Decisions

1. **Use a three-layer contract.** The response layer contains semantic facts; the provenance layer contains locally resolved source spans; the governance layer validates structure, scope, types, and promotion eligibility. Literal substring tests between semantic values and evidence text are removed. This preserves both LLM usefulness and source auditability.

   Alternative considered: retain `*_raw` transcription fields and add separate summaries. Production results show models naturally normalize and summarize, and forcing transcription recreates the same false-rejection path.

2. **Version semantic schemas and define fields as conclusions.** Structured segment labels, operating scopes, activity objects, relationship counterparties, and units are documented and prompted as semantic field values. Runtime adapters map them into existing candidate tables and record `semantic_synthesis` lineage metadata. Existing physical `*_raw` columns remain for compatibility but are not claimed to be verbatim source text for LLM-derived rows.

3. **Evidence resolution validates provenance, not wording.** Every referenced ID must exist in the request-local catalog and belong to the same immutable source document. Cross-section references are represented as a deterministic composite evidence bundle with an ordered list of exact span descriptors; semantic fields need not occur verbatim in any individual span.

4. **Keep structural and financial sanity checks.** Numeric fields must be finite and within schema ranges; percentage fractions remain bounded; unit conversion must resolve through the governed unit catalog before a normalized quantitative record is published. A conversion failure is classified explicitly as `unit_normalization_failed`, with the semantic row and source spans retained for machine rework, rather than mislabeled `context_incomplete`.

5. **Persist bounded semantic diagnostics for every outcome.** Audit metadata stores semantic output rows, accepted/rejected decisions, model/profile, hashes, usage, finish reason, evidence IDs, and a sanitized exception chain. Successful semantic runs and runtime exceptions use the same diagnostic shape. Limits cap rows, strings, stack text, and total serialized size.

6. **Use INFO for lifecycle and DEBUG for content.** INFO logs identify instrument, field family, model, outcome, counts, usage, latency, retry reason, and checkpoint reuse. DEBUG logs contain bounded structured request metadata, candidate evidence summaries, semantic output rows, row transformations, unit normalization, persistence identifiers, and tracebacks. Public filing excerpts are bounded and credentials are never logged.

7. **Make retries field-family resumable.** A completed semantic run is checked before making another model request. Token accounting gates the next request using per-field-family stage consumption rather than cumulative prior checkpoint tokens. Work IDs claimed once in a worker invocation are excluded from subsequent claims, so backoff expiry cannot cause duplicate attempts during the same batch.

8. **Rotate only semantic processing identities.** Prompt/schema/runtime identities advance so retryable work is evaluated under the corrected contract. PDFs, page artifacts, and selected sections remain reusable. Existing failed exceptions are recoverable under the new identity without deletion.

## Risks / Trade-offs

- [The LLM produces a plausible but unsupported semantic conclusion] -> Preserve exact referenced spans, candidate-only status, confidence/audit metadata, bounded schemas, and rollout promotion gates; monitor semantic samples before enabling production promotion.
- [Removing literal checks accepts incorrect numbers] -> Keep finite/range/unit conversion checks, record exact source spans, and expose numeric semantic output in DEBUG and persisted audits for sampling and later verifier improvements.
- [Composite evidence is less convenient than one quote] -> Store ordered exact span descriptors and a deterministic display excerpt while retaining every original page, section, range, and hash.
- [Detailed logs grow quickly] -> Put content only at DEBUG, bound row/excerpt sizes, and keep INFO aggregate-only.
- [Runtime identity rotation rebuilds semantic work] -> Reuse immutable report and section assets and supersede only incompatible semantic checkpoints.

## Migration Plan

1. Deploy schema/prompt/validator, composite evidence, diagnostics, retry, and checkpoint changes together.
2. Recover retryable semantic work under the new processing identity; do not delete prior runs, exceptions, or downloaded reports.
3. Run one small batch with DEBUG enabled, inspect accepted semantic summaries and persisted audit records, then return logging to INFO after production confidence is established.
4. Roll back by restoring the previous semantic identities and code. Existing immutable source assets and published records remain intact.

## Open Questions

None for implementation. Production sampling will determine later tuning of output-row and excerpt bounds without changing the semantic/provenance separation.
