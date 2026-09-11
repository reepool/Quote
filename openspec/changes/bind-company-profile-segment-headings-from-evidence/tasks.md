## 1. Provider-free implementation

- [x] 1.1 Add a local resolver for an explicit row mapping or exactly one supported complete Evidence heading, excluding the `adjustment` sentinel.
- [x] 1.2 Omit and forbid provider `dimension` in the compact segment-row schema when a unique Evidence heading is locally bound.
- [x] 1.3 Use the same resolved dimension in partition merge and semantic expansion while preserving `consolidation_adjustment=adjustment` and all existing guards.

## 2. Focused regression proof

- [x] 2.1 Add tests for unique full-heading binding, multiple/no-heading fallback, short-prefix rejection, adjustment behavior, and unchanged explicit `source_row_dimensions` precedence.
- [x] 2.2 Replay the prior frozen failure shape provider-free with a dimension-free compact row and prove source-exact Segment/Measurement expansion with zero provider calls.

## 3. Preflight and sole external validation

- [x] 3.1 Run focused pytest, Ruff, strict OpenSpec validation, `git diff --check`, and immutable input/hash checks.
- [x] 3.2 Freeze a new validation identity using the same `000408.SZ:segment_financials-02` scope, authorized Scorpio Gemini route, and bounded parameters.
- [x] 3.3 Submit the frozen scope once outside the sandbox if required; persist the request/result and classify success or typed failure without rerun or splicing.

## 4. Closeout

- [x] 4.1 Record whether the short-heading failure class is closed and whether broader replay remains unauthorized.
- [x] 4.2 Review, commit, and push only this change; keep production `not_authorized`, Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, and DCF closed.
