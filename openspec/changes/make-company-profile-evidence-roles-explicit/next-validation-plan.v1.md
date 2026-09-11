# Next Validation Plan

This plan is prepared after provider-free implementation checks. It is not an external run and does not authorize a replay by itself.

## Frozen inputs

- Cohort: the archived twenty-report manufacturing/materials shadow cohort only.
- Historical baseline: `manufacturing-materials-shadow-segment-heading-replay-gemini-20260911-a` remains immutable and is not merged into the next output.
- Evidence: a new plan revision generated with explicit Evidence roles and a provider-free preparation audit.
- Route: `semantic_extraction__scorpio_gemini`.
- Model policy: the same frozen Gemini logical profile used by the prior cohort batch.
- Budgets: extract `20000`, verify `18000`, request timeout `300` seconds, provider-call ceiling `600`, with no outer hard cutoff.
- Output: one new batch identity and one report-local immutable output directory.

## Admission checks

Before the first semantic request, validate the cohort/PDF hashes, plan and preparation-audit hashes, field ownership for every required field, Evidence traceability, route/profile, budgets, and an unused output identity. Any mismatch fails before provider invocation.

## Execution boundary

Run the complete cohort once. Preserve typed provider, schema, verification, and semantic failures. Do not run targeted repairs, substitute a model, splice historical candidates, change Gold, or relax validators. The result may be `ready`, `hold`, or `failed` and remains `production_authorization=not_authorized`.

## Review and stop condition

After the single run, produce source-bound review and readiness artifacts. Compare only by immutable hashes. Stop after that comparison; any remaining semantic defect becomes a separately scoped change rather than another run in this change.
