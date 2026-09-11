## Context

The authoritative September 11 shadow batch completed all twenty reports, but 15 segment scopes failed local partition reconciliation. The archived normalization change now accepts only same-year annual duration aliases and exposes the exact `报告分部的财务信息` heading. Its offline audit could not prove the eleven period conflicts were annual aliases because historical raw partition values were not retained.

The existing Stage 5 application service already accepts frozen prepared scopes and owns extraction, independent verification, disposition, projection, and typed provider traces. This validation therefore needs no new semantic service or batch replay mode.

## Goals / Non-Goals

**Goals:**

- Empirically execute one prior period-conflict scope and one prior incomplete-heading scope using their immutable prepared Evidence.
- Bind source batch/report and implementation identities before provider creation.
- Preserve complete per-scope output and typed traces under a new validation identity.
- Decide whether the two bounded fixes justify considering one later full-cohort replay.

**Non-Goals:**

- No twenty-report replay, historical-batch mutation, candidate splicing, model comparison, parameter tuning, prompt/schema expansion, or semantic repair loop.
- No claim that two successful scopes prove scale readiness or production quality.
- No Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Freeze two representative cases.** Use `manufacturing-materials-shadow-000055-2025:segment_financials-01` for the historical `reported_period` conflict and `manufacturing-materials-shadow-000408-2025:segment_financials-02` for the incomplete report-segment heading. Their source report files, Evidence, and prepared scopes remain unchanged.
2. **Delegate to the existing report execution owner.** A change-local one-time operator loads the frozen prepared scope and calls `ManufacturingMaterialsProfileSliceService.execute_prepared_report`. It reuses the existing provider factory and does not reproduce extraction, verification, or disposition logic.
3. **Use one bounded validation identity.** The validation uses `semantic_extraction__scorpio_gemini`, extract/verify limits `20000/18000`, a 300-second per-call timeout, and a shared ceiling of 12 physical calls, sufficient for the existing three-plus-two partitions, at most one extract repair per partition, and two verifies.
4. **Preflight may cross the sandbox boundary.** Run one read-only profile check first. If sandbox DNS or network permission fails, repeat only that check and the subsequently authorized validation outside the sandbox, without changing request content or budgets.
5. **Success is case-local.** A case is resolved only when the existing semantic owner completes the scope with accepted source-bound rows and without the targeted reconciliation failure. Any different semantic blocker or transport/schema failure is retained and reported; it does not authorize tuning or another run.

## Risks / Trade-offs

- **[A model response uses another annual label]** → Preserve the now value-bearing conflict diagnostic and classify the period case as retained.
- **[The exact heading is available but the model returns an invalid label]** → Preserve the schema/verification failure; do not fuzzy-map it after the fact.
- **[Scorpio transport fails]** → Record the typed failure under the sole validation identity and stop without substituting a model.
- **[Both cases pass but other cohort failures differ]** → Treat the result only as evidence for deciding whether a separate full replay has sufficient expected value.
