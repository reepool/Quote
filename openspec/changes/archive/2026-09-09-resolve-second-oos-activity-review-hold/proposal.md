## Why

The completed `000717.SZ` second-OOS run is transport-complete and contains 71 accepted research records, but its report remains `hold` because three source-supported Activity candidates used the wrong actor-basis label. The user has approved all three items from the immutable Evidence, so the existing research-only result needs one auditable offline adjudication path rather than another LLM run.

## What Changes

- Record the three approved Activity decisions against their exact review IDs, runtime target IDs, Evidence ID, source wording, and immutable source-bundle hashes.
- Add a narrow offline application path that accepts only the reviewed Activity candidates, changes only `actor_basis` from `explicit_economic_relationship` to `direct_grammatical_actor`, and keeps `activity_actor=source_actor=公司` plus `subject_scope=unclear`.
- Recompute the affected coverage, research projection, benchmark, and report status into a new adjudication artifact outside the original bundle.
- Preserve the original formal bundle byte-for-byte, make no provider request, and retain `production_authorization=not_authorized` with Stage 6 and all production consumers closed.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-second-oos-model-comparison`: Permit a bounded, source-hash-bound offline human adjudication of held runtime candidates after the single formal run, without reopening extraction or changing production authorization.

## Impact

- Affects the existing Stage 5 company-profile service, bundle contracts, a thin offline operator entry point, focused tests, and one immutable adjudication artifact for `stage55-second-oos-completion-000717-20260909-c`.
- Does not change prompts, provider parameters, Evidence plans, the original OOS bundle, historical four-report/BaoSteel results, Gold, approved tables, scheduler/backfill, commodity exposure, value-chain publication, DCF, or Stage 6.
