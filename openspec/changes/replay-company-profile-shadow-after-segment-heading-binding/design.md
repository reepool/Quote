## Context

The authoritative September 11 segment-retry batch completed all twenty reports with
100% Evidence traceability, 100% sampled precision, and no critical semantic errors,
but every report remained `hold`. Segment review identified annual duration aliases and
an incomplete `报告分部` heading as the dominant remaining execution blockers. The annual
normalization was implemented and externally validated; the Evidence-context repair
then produced a full corrected twenty-report plan, and the unique full-heading binding
was separately validated through Scorpio with accepted source-bound rows.

The existing `ManufacturingMaterialsShadowBatchService`, Stage 5 semantic owner,
immutable report store, review builder, readiness builder, and comparison builder remain
authoritative. This change measures the combined current behavior and must not create a
parallel extraction or evaluation workflow.

## Goals / Non-Goals

**Goals:**

- Freeze the corrected full twenty-report plan and a newly generated provider-free
  preparation audit before any provider request.
- Bind the exact current segment Evidence/context and heading implementation plus both
  external repair results to one new replay identity.
- Execute the full cohort once on the existing Gemini route and preserve all typed
  report-local outcomes.
- Produce complete source review, readiness, segment-recurrence, and immutable
  before/after evidence against the September 11 segment-retry baseline.
- Determine whether the current workflow meets the existing scale-readiness gates while
  retaining research-only status.

**Non-Goals:**

- No cohort/PDF change, Evidence answer injection, prompt/schema/model comparison,
  output-token/deadline tuning, Gold change, targeted rerun, candidate splice, or old
  bundle mutation.
- No new semantic service, confidence platform, data warehouse, approved writer,
  scheduler/backfill, Stage 6, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Use the corrected full plan, not the old prepared scopes.** The replay binds
   `corrected-evidence-plan.v1.json`, which covers the same twenty reports but removes
   the confirmed unrelated segment neighbor. Reusing the prior batch's prepared scopes
   would bypass the Evidence-context repair. A new preparation audit is generated from
   this frozen plan with zero provider calls.
2. **Bind current behavior with a small provider-free proof.** Admission validates the
   current hashes of `stage5_provider.py`, `shadow_evidence.py`, and their directly
   relevant tests, together with the two immutable external validation results. The
   proof does not bind the operator file to itself and does not introduce a general
   implementation registry.
3. **Use one new immutable batch identity.** The batch ID is
   `manufacturing-materials-shadow-segment-heading-replay-gemini-20260911-a`. The first
   semantic provider request consumes it. Existing September 10/11 batches remain
   immutable and excluded from the result.
4. **Keep the existing model and budgets.** The replay uses
   `semantic_extraction__scorpio_gemini`, extract/verify output limits `20000/18000`, a
   300-second request timeout, and a 600-physical-call ceiling. The observed provider
   output-cap overrun remains diagnostic; it is not hidden by increasing the request.
5. **Use external permission only as a transport boundary.** A read-only preflight runs
   before semantic execution. DNS/network-permission failure inside the sandbox may be
   repeated outside it. The user has authorized transfer of the required annual-report
   Evidence and request content to `scorpio.reepool.com`; no other semantic or parameter
   change follows from that permission.
6. **A non-favorable result still completes the replay.** `ready`, `hold`, and `failed`
   are all honest terminal validation outcomes. Source review and readiness use only the
   new immutable batch; no scope or report is rerun to improve metrics.

## Risks / Trade-offs

- **[The corrected full plan reveals a new provider-free preparation defect]** → Stop
  before provider creation and record the exact report/scope failure; do not patch the
  plan in place.
- **[Scorpio transport or model output fails after execution begins]** → Preserve the
  typed report-local result and finish under the existing isolation policy without a
  replacement batch.
- **[Segment completion improves but another chapter remains the readiness blocker]** →
  Quantify the source-bound blocker distribution and close this change; any fix requires
  a later bounded change.
- **[A favorable research result is mistaken for production approval]** → Retain
  `production_authorization=not_authorized` in every artifact and keep all production
  consumers closed.
