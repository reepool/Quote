## Context

The September 11 segment-heading replay is the current authoritative twenty-report
shadow batch. It completed all reports, reached 100% accepted-record Evidence
traceability and 100% sampled structural precision, and contained no critical reviewed
semantic errors, but all reports remained `hold`; unresolved-review median/p90 were
9/14 against gates of 2/5. Review showed repeated use of context-only Evidence for
legal-empty coverage, broader regime conclusions, and segment classification. The
provider adapter now exposes deterministic Evidence ownership roles with provider-free
tests and an immutable implementation audit.

The existing `ManufacturingMaterialsShadowBatchService`, Stage 5 semantic owner,
immutable report store, review builder, readiness builder, and comparison builder remain
authoritative. This change measures the repaired request contract and must not create a
parallel extraction or evaluation workflow.

## Goals / Non-Goals

**Goals:**

- Bind the exact cohort, corrected v5 plan, preparation audit, current baseline,
  Evidence-role implementation, provider settings, and absent output to one replay ID.
- Reject drift before provider construction and prove the rejection path without any
  external call.
- Execute the full cohort once through the authorized Scorpio Gemini route and preserve
  report-local typed results.
- Produce complete source review, readiness, recurrence, and before/after evidence and
  decide `ready`, `hold`, or `failed` under the existing gates.

**Non-Goals:**

- No Evidence-plan/PDF/cohort change, prompt-rule expansion, schema/validator change,
  timeout or output-budget tuning, model comparison, Gold change, targeted rerun,
  historical splice, or old-bundle mutation.
- No confidence platform, generalized admission framework, new semantic service,
  production writer, Stage 6, scheduler/backfill, commodity exposure, value-chain
  publication, or DCF.

## Decisions

1. **Reuse the corrected v5 plan and current preparation audit.** Evidence ownership is
   request metadata derived from prepared bindings, not a new Evidence plan. Replanning
   or selecting pages after seeing results would invalidate the comparison.
2. **Add one bounded replay proof validator.** The proof binds repository file hashes
   for the cohort, plan, preparation audit, baseline batch/readiness, provider adapter,
   and Evidence-role implementation audit. A local validator is used instead of a new
   registry or second orchestration layer.
3. **Use one immutable batch identity.** The only provider-bearing ID is
   `manufacturing-materials-shadow-evidence-role-gemini-20260911-a`. Once its first
   semantic request is made, success or typed failure consumes the validation attempt.
4. **Keep model and provider budgets unchanged.** The replay uses
   `semantic_extraction__scorpio_gemini`, extract/verify limits `20000/18000`, a
   300-second request timeout, a 600-physical-call ceiling, and no outer hard deadline.
5. **Treat network authorization as transport-only.** Frozen Evidence and request
   content may be transferred to `scorpio.reepool.com`. A sandbox DNS/permission failure
   is retried through the authorized external execution path; it does not permit a new
   batch, model, semantic rule, or parameter.
6. **A non-favorable result completes the change.** Review/readiness use only the new
   immutable batch. `ready`, `hold`, and `failed` are honest terminal validation results;
   no scope is rerun to improve metrics.

## Risks / Trade-offs

- **[A bound input or implementation hash drifts]** → stop before provider creation and
  record the exact drift; do not update the proof to fit unreviewed code.
- **[Scorpio fails after the first semantic request]** → preserve typed report-local
  outcomes and finish the single batch under the existing isolation policy.
- **[Evidence roles reduce known errors but readiness still misses its workload gate]**
  → quantify the remaining source-bound reason codes and close `hold`; any fix requires
  a later bounded change.
- **[Research readiness is mistaken for production approval]** → retain
  `production_authorization=not_authorized` in every artifact and keep every production
  consumer closed.
