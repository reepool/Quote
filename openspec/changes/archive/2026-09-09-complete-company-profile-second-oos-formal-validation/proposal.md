## Why

The second out-of-sample comparison selected `gemini-3.8-flash`, but its only formal run produced no bundle because the frozen Evidence plan used unsupported checklist aliases (`energy_input`, `relationship`, and `business_event`), preparation-only did not yet enforce the field closed set, and the sandboxed execution also encountered DNS failures. A corrected Evidence-plan revision and one externally network-enabled formal run are required to determine whether the existing Stage 5 contract can produce a complete research-only profile for `000717.SZ`.

## What Changes

- Freeze a new Evidence-plan revision for the existing `000717.SZ` sample, replacing `energy_input` with `material_input`, `relationship` with `counterparty_relationship`, and the redundant `business_event` checklist target with `business_regime` while preserving pages, anchors, headers, units, and all other scopes.
- Run the existing pre-provider field-contract check and preparation checks before any LLM request.
- Check Scorpio connectivity outside the restricted sandbox, then execute exactly one complete formal run with `gemini-3.8-flash`, a new run ID, and the already measured Stage 5 parameters.
- Record a complete bundle or a typed terminal failure without targeted retries, cross-run splicing, semantic-rule changes, or another model comparison.
- Keep all results research-only with `production_authorization=not_authorized` and Stage 6 closed.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-second-oos-model-comparison`: Permit one bounded completion run using a corrected Evidence-plan revision after the original formal run failed before producing a bundle.

## Impact

- Adds one immutable Evidence-plan revision, one connectivity audit, and one formal-run result for the existing second OOS sample.
- Reuses the existing Stage 5 manifest loader, preparer, semantic service, provider, projection, bundle store, and operator entry point.
- Does not change semantic fields, Gold expectations, historical four-report or BaoSteel bundles, production tables, scheduler, backfill, commodity/value-chain output, DCF, or Stage 6 state.
