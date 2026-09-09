## Context

The immutable source is `stage55-second-oos-completion-000717-20260909-c`. All 14 provider calls succeeded, but the `business_overview` task retained three Activity candidates as blocked because their runtime `actor_basis` was `explicit_economic_relationship`. The shared Evidence sentence makes “公司” the direct grammatical actor of the parallel verbs 制造、加工、销售. The user has approved all three for research review while requiring `subject_scope=unclear` and production isolation to remain unchanged.

The current Stage 5 bundle can store review decisions but has no safe offline operation that applies a decision to an immutable committed report. Re-running the semantic service would call the provider again and would violate the bounded OOS contract.

## Goals / Non-Goals

**Goals:**

- Bind the three decisions to the exact source run/report hashes, review IDs, runtime records, Evidence, actors, source sentence, and prior actor basis.
- Apply only the approved Activity actor-basis correction, then recompute coverage, research projection, benchmark, and report status using the existing Stage 5 functions.
- Write one independent, replayable adjudication result outside the source run and prove the source hashes remain unchanged.

**Non-Goals:**

- A generic record patch language, confidence platform, or new adjudication framework.
- Any LLM, PDF extraction, Evidence-plan, prompt, timeout, model, Gold, or semantic-contract change.
- Promotion to `consolidated_group`, `issuer`, production `approved`, Stage 6, scheduler/backfill, commodity exposure, value chain, or DCF.

## Decisions

1. **Use a typed Activity-only decision contract.** Each decision names one existing human-review ID and candidate record, requires `accept_for_research_review`, records the exact Evidence and source actors, and permits only `explicit_economic_relationship → direct_grammatical_actor`. A free-form field-update map is rejected because it could silently rewrite unrelated facts.
2. **Keep the committed bundle immutable.** The service loads and validates the committed manifest, verifies the manifest and report hashes, and writes a separate offline adjudication artifact. It never writes inside the input run directory.
3. **Recompute derived state from corrected task results.** The affected candidate disposition becomes `accepted_for_review`; its candidate review item is removed; the derived missing-coverage review closes only after accepted Activity records exist; `explicit_activity` coverage becomes `observed`; then the existing projection, contract benchmark, and report-state policy run again.
4. **Keep the operator thin.** A small CLI parses paths and delegates to the Stage 5 service. It contains no semantic loop or alternative status logic.
5. **Treat this as human adjudication, not model repair.** Provider traces and all unaffected records remain unchanged. The result is expected to be `usable_with_caveats`, not `usable`, because accepted `subject_scope=unclear` records remain.

## Risks / Trade-offs

- **[A decision points at a different or mutated bundle]** → Reject before applying any decision by checking both immutable hashes and run/sample identity.
- **[A decision tries to accept a non-Activity or rewrite subject scope]** → Reject through the closed Activity-only contract and precondition checks.
- **[The derived coverage closes while a real blocker remains]** → Close only the single `explicit_activity` coverage item after all referenced candidates are accepted, then derive status with the existing benchmark policy.
- **[A derived artifact is mistaken for production approval]** → Persist `provider_calls=0`, `production_authorization=not_authorized`, source identity, and explicit downstream prohibitions in the result and audit.
