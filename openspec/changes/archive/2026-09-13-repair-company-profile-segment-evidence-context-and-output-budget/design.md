## Context

The external validation result `manufacturing-materials-shadow-segment-normalization-external-live-20260911-b` shows two different causes in one scope. The table page was valid, but the generic bounded-page window also included the next unrelated page. The first metric partition then produced 30,695 output tokens despite a requested 20,000-token ceiling, while the local merge rejected an ambiguous `报告分部` value instead of accepting it. The correct response is to improve request construction and evidence selection, not to relax the validator.

## Goals / Non-Goals

**Goals:**

- Keep only source-supported segment table pages in the request, adding neighbors only for explicit continuation markers or equivalent table-owned evidence.
- Tell the model exactly which source heading is allowed and require compact JSON containing only rows/coverage.
- Preserve all existing source-label, Evidence, period, subject, and output-budget protections.
- Validate the fix with provider-free tests and one external run of the same frozen scope.

**Non-Goals:**

- No fuzzy acceptance of `报告分部`, no after-the-fact heading rewrite, no response truncation, no Gold changes, no historical bundle mutation, no model substitution, and no cohort replay in this change.

## Decisions

1. **Segment context is not a generic ±1 page window.** For `extract_segment_financials`, direct selected pages form the initial range. `_bind_table_context_range` may add a neighboring page only when the selected text explicitly marks a continuation (`续表`, `续下表`, `接下页`) and the page is available. Existing material-input and other chapter behavior remains unchanged.
2. **Exact heading is a request constraint, not a post-hoc repair.** When the prepared Evidence contains `报告分部的财务信息`, the segment instruction states that every non-adjustment row must use that exact value. The schema continues to enumerate only exact source options and the local validator continues to reject ambiguous prefixes.
3. **Compactness is explicit and measurable.** Segment partition instructions require no prose, markdown, repeated source text, or duplicate rows; output-budget warnings remain visible in the trace. A valid but over-budget response is not truncated or silently treated as within budget.
4. **One external validation is sufficient for this change.** Reuse the same frozen `000408.SZ:segment_financials-02` Evidence identity with a new run ID and unchanged Gemini route/budgets. The result is case-local and research-only.

## Risks / Trade-offs

- **A table genuinely spans an adjacent page without a marker** → retain the existing table-context failure rather than guessing; the evidence planner can be improved in a later source-backed change.
- **The model still emits an ambiguous heading** → preserve the typed failure; do not normalize it after the fact.
- **The provider continues to exceed the output budget** → record the warning and investigate model/provider parameter support separately; do not raise the budget to hide it.
