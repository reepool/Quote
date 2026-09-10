## Context

The corrected external twenty-report replay remains the empirical baseline: execution completion is 95%, usable-report rate is 0%, sampled precision is 76/83 (91.566265%), and unresolved-review median/p90 are 11/17. The subsequent owner-regression change fixed seven proven Evidence-owner/substance defects provider-free, but it intentionally did not regenerate the cohort plan or run the model.

The existing `ShadowEvidencePlanner`, `ManufacturingMaterialsShadowBatchService`, immutable batch store, review/readiness builders, and replay comparison builder remain the authoritative path. This change must measure the fixes without creating a parallel workflow or changing semantic/model policy.

## Goals / Non-Goals

**Goals:**

- Generate a new immutable Evidence plan for the same twenty reports/PDFs with the current owner rules.
- Fail before provider creation unless all twenty reports prepare, the seven rejected owner shapes are absent, and the `920033.BJ` owner-bound legal-empty control remains present.
- Execute one new-ID Gemini replay from the already authorized sandbox-external network path.
- Complete source review, readiness, and immutable comparison against the corrected external baseline.
- Preserve an honest `ready`, `hold`, or `failed` outcome and identify the next business blocker.

**Non-Goals:**

- No new model comparison, prompt, field/schema, token/deadline, retry, Gold, or readiness threshold.
- No targeted report/scope rerun, candidate splice, second replay, or mutation of historical batches.
- No production approval, Stage 6, scheduler/backfill, approved-table write, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Regenerate rather than patch the old v3 plan.** The current planner creates a new plan version from the frozen manifest and PDF artifacts. The old v3 plan remains the reproducible baseline; individual pages or scopes are not edited in place.

2. **Use one provider-free lineage gate.** The existing operator gains a preparation mode that writes the new plan, its complete preparation audit, and a hash-bound correction audit. The audit checks all frozen owner-regression cases directly against the generated scope/page/field assignments and records zero provider calls. This reuses the existing correction-audit admission shape instead of adding a new validation platform.

3. **Keep execution on the existing report-local owner.** A single narrow replay contract/mode binds the new plan/audits, the archived owner-closure evidence, the existing Gemini profile, and unchanged budgets. The CLI only selects the contract; the service continues to own provider execution and persistence.

4. **Treat the first provider request as authoritative.** Provider-free preparation must be complete before network use. The one provider-bearing process starts through the user-authorized sandbox-external path. After it begins, typed failures are retained and no second run is permitted.

5. **Measure actual usefulness, not a favorable label.** The frozen source-review rule and readiness gates remain unchanged. `ready`, `hold`, and `failed` all complete this validation; only a genuinely `ready` result can justify a separate restricted production-promotion proposal.

## Risks / Trade-offs

- **[Current owner rules leave a report without a required owner]** → Stop provider-free with the exact report/chapter failure; do not weaken the rule or call the model.
- **[External provider execution is slow or fails]** → Preserve report-local typed results and close the single batch without tuning or rerun.
- **[The cohort remains hold despite better precision]** → Use the new source-bound comparison to choose the next bounded semantic fix; do not broaden this change.
- **[A research-ready result is mistaken for approval]** → Retain `production_authorization=not_authorized` in every plan, report, audit, and summary.
