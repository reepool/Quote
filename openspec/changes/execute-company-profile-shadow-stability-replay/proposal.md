## Why

The corrected Evidence plan and the bounded LLM execution path have now passed provider-free regression checks and a live repair probe, but scale readiness still rests on the earlier twenty-report replay that completed only 15 of 20 reports. One controlled new-ID replay is required to measure whether the stabilized path can execute the frozen cohort reliably without changing the semantic contract or hiding failures.

## What Changes

- Admit exactly one provider-bearing replay of the frozen twenty-report manufacturing/materials cohort after revalidating the existing manifest, PDF, Evidence-plan, preparation-audit, scope-correction, and stability artifacts by hash.
- Use one new immutable batch ID, the frozen primary Gemini logical profile, and the existing 20,000 extract, 18,000 verify, 300-second request, and 600-call budgets.
- Run all reports through the existing report-local shadow service; preserve typed failures, partition lineage, and report isolation without targeted reruns, model substitution, or historical-result splicing.
- Generate a source-bound review package, readiness audit, and immutable comparison against the prior refined replay, including execution completion, report usability, Evidence traceability, critical errors, review workload, token usage, latency, and failure classes.
- Close the validation honestly as `ready`, `hold`, or `failed`; retain `production_authorization=not_authorized` and keep Stage 6, approved writers, scheduler/backfill, commodity exposure, value-chain publication, and DCF closed.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: Add the single-use post-stability replay admission, immutable comparison, and research-only stop conditions for the frozen twenty-report cohort.

## Impact

- Affected execution path: the existing `scripts/run_company_profile_shadow_batch.py` thin operator, `ManufacturingMaterialsShadowBatchService`, and shadow audit helpers only where needed to admit and compare this single replay.
- Affected artifacts: one new immutable batch directory plus review, readiness, and before/after audit files stored outside historical batch directories.
- No semantic schema, Evidence selection, Gold expectation, model route, token/deadline budget, production database, scheduler, approved writer, or historical batch is changed.
