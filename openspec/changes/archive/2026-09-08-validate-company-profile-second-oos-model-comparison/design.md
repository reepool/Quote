## Context

The repository already has a bounded out-of-sample manifest path and measured Stage 5 execution defaults. The next validation must test portability on a second unseen annual report while preserving the rule that an annual-report image is built from one coherent model run. The three enabled models are `grok-4.6`, `glm-5.3-flash`, and `gemini-3.8-flash`.

## Goals / Non-Goals

**Goals:**

- Establish one immutable second-sample manifest and six-chapter Evidence plan.
- Compare all three models on the same inputs and collect comparable operational and semantic evidence.
- Choose a deterministic primary model without claiming universal model superiority.
- Produce one isolated formal OOS bundle using only the selected model.

**Non-Goals:**

- Combining candidates, accepted facts, or verification decisions across models.
- Re-running BaoSteel or the four-report authority.
- Changing schemas, semantic rules, Gold expectations, acceptance policy, or production authorization.
- Building a new model registry, confidence platform, or production routing service.
- Starting Stage 6, approved tables, backfill, scheduler, API, commodity exposure, value-chain, or DCF publication.

## Decisions

1. **Freeze before calls.** Select one locally valid report not present in any prior authority, Gold, targeted run, or adjudication record. Freeze report identity, PDF hash, business/disclosure form, known limitations, and six-chapter Evidence before sending any model request.
2. **Use identical comparison inputs.** For one overview request and one table-heavy request, each model receives byte-equivalent request scope, prompt, schema, temperature, output budget, and deadline. Comparison calls write model-specific artifacts and never feed the formal bundle.
3. **Prefer semantic reliability over speed.** Rank models by successful structured parse and Evidence-bound source support first, then verifier agreement/coverage, then latency and token cost. A timeout or malformed response is an execution finding, not a reason to splice another model's output into its scope.
4. **Keep model identity explicit.** Every comparison and formal call records model, route, parameters, run ID, and request hash. The formal bundle uses one selected model for all extract/repair/verify calls, subject to the existing bounded failover policy.
5. **One formal run.** After offline checks and model comparison, execute one complete formal OOS run with a fresh run ID. If it fails, retain the typed failure and stop; no targeted loop or second complete run is allowed in this change.
6. **Research-only boundary.** All output remains `accepted_for_review`; report state may be `usable`, `usable_with_caveats`, `hold`, or `failed`, while `production_authorization` remains `not_authorized`.

## Risks / Trade-offs

- **[A model wins on one request but not the report]** → Treat the comparison as bounded evidence, then report formal-run caveats rather than claiming general superiority.
- **[One model has a transient provider failure]** → Preserve the typed failure and compare only completed observations; do not substitute another model's output into its request.
- **[The second report has unsupported disclosure forms]** → Record them as OOS findings and do not change semantic rules in this change.
- **[Three-model comparison increases calls]** → Limit comparison to two representative frozen scopes and one formal complete run.

## Migration Plan

No production migration is required. Add only isolated sample/comparison manifests and validation artifacts. Rollback is a code/document revert; historical bundles remain immutable.

## Open Questions

- Which locally valid report best provides a business/disclosure form not represented by the existing four reports and BaoSteel? Selection is resolved during task 1 before Evidence freeze.
