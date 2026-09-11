## Context

The September 11 external validation for `000408.SZ:segment_financials-02` used one relevant physical page and completed one Gemini call within transport and output budgets. Controlled Evidence contained one complete table-owning heading, `报告分部的财务信息`, but the response shortened it to `报告分部`; the existing validator correctly rejected the candidate. The adapter already supports plan-owned per-row dimensions through `source_row_dimensions`, but automatically planned shadow scopes do not pre-map every physical row label.

The authoritative owner remains the existing Stage 5 provider adapter. This change must not add a second semantic path, broaden the vocabulary, mutate Evidence plans after execution, or weaken source-label validation.

## Goals / Non-Goals

**Goals:**

- Resolve the confirmed loss of otherwise supported segment facts when one complete table dimension is uniquely determined by controlled Evidence.
- Keep dimension ownership deterministic and local while leaving row labels, metric cells, periods, subjects, and Evidence IDs model-derived and independently validated.
- Preserve fail-closed behavior for ambiguous Evidence and all existing substantive conflicts.
- Prove the behavior provider-free before one case-local external validation.

**Non-Goals:**

- No fuzzy expansion of `报告分部` into a longer heading.
- No table parser, row-to-dimension registry, new response model, new semantic field, new provider route, or generalized confidence framework.
- No historical bundle or Gold mutation, cohort replay, Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Reuse the existing supported heading detector.** The adapter derives candidate headings from `_segment_dimension_options`, excludes the special `adjustment` value, and treats the result as locally bindable only when exactly one complete option remains. This preserves the already-tested accepted heading vocabulary and does not introduce fuzzy prefix matching.
2. **Schema ownership changes only for a unique scope heading.** When `source_row_dimensions` is empty and one unique complete heading exists, the compact row schema omits `dimension` and disallows provider-supplied dimension through `additionalProperties=false`. With zero or multiple headings, the current provider-owned dimension schema remains unchanged. Existing explicit `source_row_dimensions` continues to take precedence.
3. **Expansion and partition merge share the same local resolution.** A small helper resolves a row dimension in this order: explicit plan mapping, unique Evidence heading, or provider value. Provider dimension is rejected when either local binding applies. `consolidation_adjustment` rows still resolve to `adjustment`; the unique table heading applies only to normal rows.
4. **Validation remains source-exact.** The resolved local heading must still occur in controlled Evidence and come from the existing exact supported-heading detector; provider-owned values retain the existing full-heading checks. A short provider prefix is never rewritten. Multiple complete headings, no complete heading, unsupported labels, duplicate cells, Evidence conflicts, period conflicts, and subject conflicts remain blocking.
5. **Validation is deliberately narrow.** Provider-free tests replay the shape of the prior failure with dimension omitted, then one new validation identity may submit the same frozen scope to the authorized Gemini route. The external result closes only this failure class and does not authorize the pending twenty-report replay.

## Risks / Trade-offs

- **A scope contains two legitimate dimension tables** → no automatic binding; retain provider-owned dimension and existing validation rather than guessing.
- **A complete heading is split or damaged by PDF extraction** → no automatic binding; retain the typed failure and improve Evidence only in a separate source-backed change.
- **A normal row and adjustment row share the same label across partitions** → the existing row identity and row-class conflict checks remain authoritative; adjustment does not inherit the normal scope heading.
- **A model still emits `dimension` despite the bound schema** → schema validation rejects the response instead of silently ignoring provider content.

## Migration Plan

1. Add provider-free helper/schema/merge/expansion tests using immutable fixtures.
2. Implement the local binding in the existing Stage 5 adapter.
3. Run focused regression and strict contract checks.
4. If provider-free proof passes, run one new-identity external validation of the frozen scope.
5. Rollback is the single adapter/test commit; historical outputs and production state are untouched.

## Open Questions

None. A broader replay decision is intentionally deferred until this confirmed failure class is closed.
