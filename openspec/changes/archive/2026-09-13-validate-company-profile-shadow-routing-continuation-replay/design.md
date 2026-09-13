## Context

The owner-closure replay completed all twenty reports and reached 97.5% sampled
precision with zero critical errors, but all reports remained `hold` and unresolved
review median/p90 were 12.5/20. Source review isolated three remaining
Evidence-planning defects. The subsequent provider-free change produced immutable v5
plan identity `d091262f4350ff88779293900f0deb21d7c48da85a3102d7a947fa861ec4eafe`,
prepared 20 reports / 154 scopes / 369 Evidence items with 100% traceability, and
closed all six reviewed error/control cases without a provider call.

The existing `ManufacturingMaterialsShadowBatchService`, report-local persistence,
review package builder, readiness audit, and replay comparison remain authoritative.
This change measures v5; it must not create a parallel runner or tune policy after
seeing the result.

## Goals / Non-Goals

**Goals:**

- Bind one new replay identity to the committed v5 plan, both v5 audits, the frozen
  twenty-report cohort/PDFs, and the existing owner-closure empirical baseline.
- Prove all identities and output absence locally before provider creation.
- Execute exactly one complete Gemini replay through the authorized sandbox-external
  network path and retain all typed outcomes.
- Re-run the existing source review and readiness computation, including exact and
  defect-family recurrence for the three v5 corrections.
- Determine whether report usability and review workload materially improve while
  preserving factual precision and zero critical errors.

**Non-Goals:**

- No Evidence regeneration, sample change, prompt/schema/model comparison, token or
  deadline tuning, repair-policy change, Gold change, or readiness-threshold change.
- No targeted report/scope rerun, second batch, historical candidate splice, or bundle
  mutation.
- No production approval, Stage 6, approved writes, scheduler/backfill, commodity
  exposure, value-chain publication, or DCF.

## Decisions

1. **Replay the committed v5 plan without rebuilding it.** Admission binds the plan
   file and semantic identity, preparation audit
   `a6a6d1880c5ae58b5f602329ff3c4280e78c60c5a00abb7edf61384dc6c60511`, and
   correction audit
   `a16ebfc4bf836bbc002ead7a9d1fed5d5d1c1c12212993ad608a920916f76e22`.
   Rebuilding would create a different empirical input and invalidate the provider-free
   closure being measured.

2. **Add only one narrow replay admission/mode.** The existing CLI may select batch
   `manufacturing-materials-shadow-routing-continuation-gemini-20260910-a`, but the
   existing service continues to own execution and report persistence. Generic semantic
   modes remain unable to use v5.

3. **Use the frozen Gemini execution profile and budgets.** The replay keeps the same
   logical route, extract/verify output limits, 300-second attempt deadline, physical
   call ceiling, partition/merge behavior, and bounded failure handling as the
   owner-closure baseline. This isolates the Evidence-plan effect.

4. **Start provider execution only outside the restricted sandbox.** A read-only
   connectivity preflight may distinguish sandbox DNS from provider availability. Once
   the first provider request starts, the new batch identity is consumed; typed
   failures are evidence and do not authorize another attempt.

5. **Use the existing usefulness gates and source review.** The audit reports execution
   completion, usable-report rate, Evidence traceability, sampled precision, critical
   errors, unresolved median/p90, provider failures, tokens, latency, and corrected
   defect recurrence. A `hold` or `failed` result completes this validation honestly;
   only `ready` may justify a later restricted promotion proposal.

## Risks / Trade-offs

- **[The provider returns schema-invalid or oversized responses]** → Preserve the
  report-local typed traces and complete the sole batch without tuning or rerun.
- **[v5 removes the reviewed defects but report usability stays at zero]** → Use the
  source-bound blocker distribution to select one next bounded fix; do not broaden this
  replay change.
- **[A historical input or output identity drifts]** → Fail before provider creation and
  report the exact hash/path mismatch.
- **[A favorable research result is mistaken for production approval]** → Retain
  `production_authorization=not_authorized` in every artifact and keep all production
  consumers closed.
