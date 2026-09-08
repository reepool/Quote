## Why

The BaoSteel out-of-sample run demonstrated that the bounded Stage 5 workflow can complete, but one unseen report is not enough to establish model portability. A second unseen report should be evaluated with the same frozen Evidence sent independently to `grok-4.6`, `glm-5.3-flash`, and `gemini-3.8-flash` so model suitability is measured on semantic extraction rather than assumed from one provider result.

## What Changes

- Freeze exactly one new official, locally valid annual report that is absent from the four-report authority, Gold set, targeted runs, and adjudication ledger.
- Freeze the six core chapter Evidence before any model call.
- Run a controlled three-model comparison on identical representative overview and table-heavy requests with identical temperature, output budgets, deadlines, prompts, and Evidence.
- Record per-model parse success, latency, token usage, candidate shape, Evidence binding, source support, and typed provider/execution failures in separate comparison artifacts.
- Select one primary model using structured reliability first, semantic support second, and latency third; retain the other models as comparison results only.
- Run exactly one complete formal OOS validation for the frozen sample using the selected model and the existing bounded workflow.
- Keep the output research-only with `accepted_for_review`, `production_authorization=not_authorized`, and no production publication or Stage 6 activation.

## Capabilities

### New Capabilities

- `company-profile-second-oos-model-comparison`: Defines the independent three-model comparison and bounded second-sample validation contract.

### Modified Capabilities

None.

## Impact

- Affects the Stage 5 OOS operator and model-selection evidence path, plus isolated manifests, comparison artifacts, and one formal validation bundle.
- Reuses the existing preparation, Evidence, semantic service, provider, projection, benchmark, and bundle owners.
- Does not modify the four-report authority, Gold expectations, historical bundles, production tables, scheduler, API, commodity/value-chain outputs, or Stage 6 state.
