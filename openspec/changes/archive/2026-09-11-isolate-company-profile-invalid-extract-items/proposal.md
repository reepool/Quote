## Why

The authoritative 20-report manufacturing/materials shadow replay completed transport successfully but retained 29 `candidate_schema_invalid` extract failures. In those calls, one locally invalid candidate or coverage item can currently invalidate the whole otherwise parseable response, discarding evidence-supported sibling facts and inflating report-level unresolved work.

## What Changes

- Isolate locally invalid extract items after a schema-parseable provider response instead of failing the whole extract call.
- Preserve every sibling candidate or coverage item that independently satisfies the existing Evidence, subject, metric, unit, period, and semantic contracts.
- Record bounded typed rejected-item diagnostics on the provider trace and keep the affected field unresolved when no valid result remains.
- Add provider-free mixed-response fixtures for the five observed failure families and a frozen 29-row historical-shape audit.
- Keep production authorization, Stage 6, approved storage, scheduler/backfill, commodity exposure, value-chain publication, and DCF closed.
- Defer adaptive output-token budgets to a separate change; this change does not alter model, prompt, timeout, retry, partition, or token parameters and does not rerun the frozen shadow batch.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: A schema-parseable extract response with one invalid item preserves valid sibling items, emits typed local rejection diagnostics, and remains unresolved rather than falsely complete when no valid result remains.

## Impact

- `research/company_profile/stage5_provider.py`: extract-response adaptation and local item validation.
- `research/company_profile/stage5_bundle.py`: backward-compatible typed rejected-item trace diagnostics.
- `tests/unit/test_research/test_company_profile_stage5_provider.py`: mixed valid/invalid response regressions.
- A provider-free OpenSpec audit reads the frozen external replay without modifying its bundle or invoking an LLM.
